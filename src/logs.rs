//! Bounded, append-only local call logs written as one JSON object per line per day.
use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Sender, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use base64::{engine::general_purpose::STANDARD, Engine};
use serde::Serialize;
use serde_json::{json, Value};

use crate::error::{ApiError, Result};
use crate::util::set_mode;

const QUEUE: usize = 16;

/// Captures up to `limit` bytes of a body while still counting the full size.
#[derive(Debug)]
pub struct Capture {
    limit: usize,
    pub data: Vec<u8>,
    pub total: usize,
}

impl Capture {
    pub fn new(limit: usize, enabled: bool) -> Self {
        Self {
            limit: if enabled { limit } else { 0 },
            data: Vec::new(),
            total: 0,
        }
    }

    pub fn add(&mut self, chunk: &[u8]) {
        self.total += chunk.len();
        let room = self.limit.saturating_sub(self.data.len()).min(chunk.len());
        self.data.extend_from_slice(&chunk[..room]);
    }

    pub fn export(&self) -> CaptureExport {
        self.export_replaced(&self.data)
    }

    /// Export a rewritten (redacted) body while keeping the true wire size.
    pub fn export_replaced(&self, data: &[u8]) -> CaptureExport {
        let truncated = self.total > data.len();
        match String::from_utf8(data.to_vec()) {
            Ok(body) => CaptureExport {
                body,
                encoding: "utf-8",
                bytes: self.total,
                truncated,
            },
            Err(_) => CaptureExport {
                body: STANDARD.encode(data),
                encoding: "base64",
                bytes: self.total,
                truncated,
            },
        }
    }
}

#[derive(Debug, Serialize)]
pub struct CaptureExport {
    pub body: String,
    pub encoding: &'static str,
    pub bytes: usize,
    pub truncated: bool,
}

/// One call log line. Field order matches the JSON written by the Python service.
#[derive(Debug, Serialize)]
pub struct Record {
    pub request_id: String,
    pub time: String,
    pub source: &'static str,
    pub status: u16,
    pub upstream_status: Option<u16>,
    pub outcome: &'static str,
    pub error: Option<String>,
    pub stream: bool,
    pub first_byte_latency_ms: Option<u64>,
    pub response_bytes: usize,
    pub request_bytes: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upstream_model: Option<String>,
    pub latency_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request: Option<CaptureExport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response: Option<CaptureExport>,
}

impl Record {
    pub fn new(request_id: &str, source: &'static str) -> Self {
        Self {
            request_id: request_id.to_string(),
            time: crate::util::now(),
            source,
            status: 500,
            upstream_status: None,
            outcome: "error",
            error: None,
            stream: false,
            first_byte_latency_ms: None,
            response_bytes: 0,
            request_bytes: 0,
            model: None,
            provider_id: None,
            upstream_model: None,
            latency_ms: 0,
            request: None,
            response: None,
        }
    }
}

enum Job {
    Write(Box<Record>),
    Flush(Sender<()>),
    Stop,
}

/// A bounded in-process queue; overload falls back to a synchronous append.
pub struct CallLogger {
    directory: PathBuf,
    sender: SyncSender<Job>,
    failures: Arc<AtomicU64>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl CallLogger {
    pub fn open(directory: &Path) -> Result<Self> {
        fs::create_dir_all(directory)?;
        set_mode(directory, 0o700)?;
        let (sender, receiver) = mpsc::sync_channel::<Job>(QUEUE);
        let failures = Arc::new(AtomicU64::new(0));
        let worker = {
            let directory = directory.to_path_buf();
            let failures = Arc::clone(&failures);
            thread::Builder::new()
                .name("call-logger".to_string())
                .spawn(move || run(directory, receiver, failures))?
        };
        Ok(Self {
            directory: directory.to_path_buf(),
            sender,
            failures,
            worker: Mutex::new(Some(worker)),
        })
    }

    pub fn failures(&self) -> u64 {
        self.failures.load(Ordering::Relaxed)
    }

    pub fn write(&self, record: Record) {
        match self.sender.try_send(Job::Write(Box::new(record))) {
            Ok(()) => {}
            Err(TrySendError::Full(Job::Write(record))) => self.append(&record),
            Err(TrySendError::Disconnected(Job::Write(record))) => self.append(&record),
            Err(_) => {}
        }
    }

    /// Make queued records visible before an administrator reads the file.
    pub fn flush(&self) {
        let (done, flushed) = mpsc::channel();
        if self.sender.send(Job::Flush(done)).is_ok() {
            let _ = flushed.recv();
        }
    }

    fn append(&self, record: &Record) {
        append_record(&self.directory, &self.failures, record);
    }

    pub fn list_days(&self) -> Vec<String> {
        let mut days: Vec<String> = fs::read_dir(&self.directory)
            .map(|entries| {
                entries
                    .filter_map(|entry| entry.ok())
                    .filter_map(|entry| entry.file_name().into_string().ok())
                    .filter(|name| name.ends_with(".jsonl"))
                    .filter_map(|name| {
                        let stem = name.trim_end_matches(".jsonl");
                        valid_day(stem).then(|| stem.to_string())
                    })
                    .collect()
            })
            .unwrap_or_default();
        days.sort_by(|a, b| b.cmp(a));
        days
    }

    pub fn read_day(&self, day: &str, limit: usize, query: &str) -> Result<Value> {
        if !valid_day(day) {
            return Err(ApiError::bad("date must use YYYY-MM-DD format"));
        }
        if !(1..=500).contains(&limit) {
            return Err(ApiError::bad("limit must be between 1 and 500"));
        }
        let needle: String = query.trim().to_lowercase().chars().take(200).collect();
        self.flush();
        let mut items: VecDeque<Value> = VecDeque::new();
        let path = self.directory.join(format!("{day}.jsonl"));
        if path.exists() {
            if let Ok(file) = File::open(&path) {
                for line in BufReader::new(file).lines() {
                    let Ok(line) = line else { break };
                    let Ok(item) = serde_json::from_str::<Value>(&line) else {
                        continue;
                    };
                    if !needle.is_empty()
                        && !serde_json::to_string(&item)
                            .is_ok_and(|text| text.to_lowercase().contains(&needle))
                    {
                        continue;
                    }
                    items.push_back(item);
                    if items.len() > limit {
                        items.pop_front();
                    }
                }
            }
        }
        let mut reversed: Vec<Value> = items.into_iter().collect();
        reversed.reverse();
        Ok(json!({ "date": day, "items": reversed, "days": self.list_days() }))
    }

    pub fn close(&self) {
        self.flush();
        let _ = self.sender.send(Job::Stop);
        let handle = self
            .worker
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(handle) = handle {
            let _ = handle.join();
        }
    }
}

impl Drop for CallLogger {
    fn drop(&mut self) {
        self.close();
    }
}

fn run(directory: PathBuf, receiver: mpsc::Receiver<Job>, failures: Arc<AtomicU64>) {
    while let Ok(job) = receiver.recv() {
        match job {
            Job::Write(record) => append_record(&directory, &failures, &record),
            Job::Flush(done) => {
                let _ = done.send(());
            }
            Job::Stop => break,
        }
    }
}

fn append_record(directory: &Path, failures: &AtomicU64, record: &Record) {
    let day = record.time.get(..10).unwrap_or("unknown");
    let Ok(line) = serde_json::to_string(record) else {
        return;
    };
    let written = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(directory.join(format!("{day}.jsonl")))
        .and_then(|mut file| file.write_all(format!("{line}\n").as_bytes()));
    if written.is_err() {
        failures.fetch_add(1, Ordering::Relaxed);
        eprintln!("ERROR: failed to write call log (check local disk)");
    }
}

fn valid_day(day: &str) -> bool {
    let bytes = day.as_bytes();
    bytes.len() == 10
        && bytes[..4].iter().all(u8::is_ascii_digit)
        && bytes[4] == b'-'
        && bytes[5..7].iter().all(u8::is_ascii_digit)
        && bytes[7] == b'-'
        && bytes[8..].iter().all(u8::is_ascii_digit)
}
