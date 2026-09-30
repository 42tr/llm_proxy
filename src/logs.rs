//! Bounded, append-only local call logs written as one JSON object per line per day.
use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Sender, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{Days, Utc};
use serde::Serialize;
use serde_json::{json, Value};

use crate::error::{ApiError, Result};
use crate::util::{set_mode, truncate_utf8};

const QUEUE: usize = 16;
/// Body fields left out of log listings; the detail endpoint returns them.
const BODY_FIELDS: [&str; 2] = ["request", "response"];

#[derive(Debug, Serialize)]
pub struct CaptureExport {
    pub body: String,
    pub encoding: &'static str,
    pub bytes: usize,
    pub truncated: bool,
}

impl CaptureExport {
    /// Export at most `limit` bytes of a (possibly redacted) body.
    ///
    /// `bytes` is the true wire size and `complete` says whether `body` holds all of it.
    /// The cut never splits a UTF-8 sequence, so a truncated text body stays text.
    pub fn new(body: &[u8], bytes: usize, complete: bool, limit: usize) -> Self {
        let kept = truncate_utf8(body, limit);
        let truncated = !complete || kept.len() < body.len();
        match std::str::from_utf8(kept) {
            Ok(text) => Self {
                body: text.to_string(),
                encoding: "utf-8",
                bytes,
                truncated,
            },
            Err(_) => Self {
                body: STANDARD.encode(kept),
                encoding: "base64",
                bytes,
                truncated,
            },
        }
    }
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
///
/// Day files older than `retention_days` are deleted when the logger starts and whenever
/// the day rolls over; `0` keeps every file.
pub struct CallLogger {
    directory: PathBuf,
    sender: SyncSender<Job>,
    failures: Arc<AtomicU64>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl CallLogger {
    pub fn open(directory: &Path, retention_days: u32) -> Result<Self> {
        fs::create_dir_all(directory)?;
        set_mode(directory, 0o700)?;
        let (sender, receiver) = mpsc::sync_channel::<Job>(QUEUE);
        let failures = Arc::new(AtomicU64::new(0));
        let worker = {
            let directory = directory.to_path_buf();
            let failures = Arc::clone(&failures);
            thread::Builder::new()
                .name("call-logger".to_string())
                .spawn(move || run(directory, receiver, failures, retention_days))?
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
        list_days(&self.directory)
    }

    /// The newest `limit` records of a day matching `query`, without their bodies.
    ///
    /// Only the byte ranges of matching lines are kept while scanning, so memory stays
    /// bounded by `limit` no matter how large the day file or its captured bodies are.
    pub fn read_day(&self, day: &str, limit: usize, query: &str) -> Result<Value> {
        if !valid_day(day) {
            return Err(ApiError::bad("date must use YYYY-MM-DD format"));
        }
        if !(1..=500).contains(&limit) {
            return Err(ApiError::bad("limit must be between 1 and 500"));
        }
        let needle: String = query.trim().to_lowercase().chars().take(200).collect();
        self.flush();
        let mut items = Vec::new();
        if let Ok(file) = File::open(self.day_path(day)) {
            let mut reader = BufReader::new(file);
            let mut ranges: VecDeque<(u64, usize)> = VecDeque::new();
            let mut line = Vec::new();
            let mut offset = 0u64;
            loop {
                line.clear();
                let Ok(read) = reader.read_until(b'\n', &mut line) else {
                    break;
                };
                if read == 0 {
                    break;
                }
                let matched = needle.is_empty()
                    || String::from_utf8_lossy(&line)
                        .to_lowercase()
                        .contains(&needle);
                if matched {
                    ranges.push_back((offset, read));
                    if ranges.len() > limit {
                        ranges.pop_front();
                    }
                }
                offset += read as u64;
            }
            let mut file = reader.into_inner();
            for (start, length) in ranges.into_iter().rev() {
                let Some(Value::Object(mut item)) = read_line(&mut file, start, length) else {
                    continue;
                };
                for field in BODY_FIELDS {
                    item.remove(field);
                }
                items.push(Value::Object(item));
            }
        }
        Ok(json!({ "date": day, "items": items, "days": self.list_days() }))
    }

    /// One complete record, including captured bodies.
    pub fn read_record(&self, day: &str, request_id: &str) -> Result<Value> {
        if !valid_day(day) {
            return Err(ApiError::bad("date must use YYYY-MM-DD format"));
        }
        let valid_id = !request_id.is_empty()
            && request_id.len() <= 64
            && request_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_');
        if !valid_id {
            return Err(ApiError::not_found("log record not found"));
        }
        self.flush();
        // Records are serialized with `request_id` first, so a prefix check finds the line.
        let prefix = format!("{{\"request_id\":\"{request_id}\"");
        if let Ok(file) = File::open(self.day_path(day)) {
            let mut reader = BufReader::new(file);
            let mut line = Vec::new();
            while matches!(reader.read_until(b'\n', &mut line), Ok(read) if read > 0) {
                if line.starts_with(prefix.as_bytes()) {
                    if let Ok(item) = serde_json::from_slice::<Value>(&line) {
                        return Ok(item);
                    }
                }
                line.clear();
            }
        }
        Err(ApiError::not_found("log record not found"))
    }

    fn day_path(&self, day: &str) -> PathBuf {
        self.directory.join(format!("{day}.jsonl"))
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

fn run(
    directory: PathBuf,
    receiver: mpsc::Receiver<Job>,
    failures: Arc<AtomicU64>,
    retention_days: u32,
) {
    let mut pruned_for = String::new();
    let mut prune = |day: &str| {
        if retention_days > 0 && day != pruned_for {
            prune_days(&directory, retention_days);
            pruned_for = day.to_string();
        }
    };
    prune(&crate::util::now()[..10]);
    while let Ok(job) = receiver.recv() {
        match job {
            Job::Write(record) => {
                prune(record.time.get(..10).unwrap_or(""));
                append_record(&directory, &failures, &record);
            }
            Job::Flush(done) => {
                let _ = done.send(());
            }
            Job::Stop => break,
        }
    }
}

/// Delete day files older than the newest `retention_days` calendar days (today included).
fn prune_days(directory: &Path, retention_days: u32) {
    let today = Utc::now().date_naive();
    let Some(oldest) = today.checked_sub_days(Days::new(u64::from(retention_days) - 1)) else {
        return;
    };
    let oldest = oldest.format("%Y-%m-%d").to_string();
    for day in list_days(directory) {
        if day < oldest && fs::remove_file(directory.join(format!("{day}.jsonl"))).is_err() {
            eprintln!("ERROR: failed to delete expired call log {day}");
        }
    }
}

fn list_days(directory: &Path) -> Vec<String> {
    let mut days: Vec<String> = fs::read_dir(directory)
        .map(|entries| {
            entries
                .filter_map(|entry| entry.ok())
                .filter_map(|entry| entry.file_name().into_string().ok())
                .filter_map(|name| {
                    let stem = name.strip_suffix(".jsonl")?;
                    valid_day(stem).then(|| stem.to_string())
                })
                .collect()
        })
        .unwrap_or_default();
    days.sort_by(|a, b| b.cmp(a));
    days
}

fn read_line(file: &mut File, start: u64, length: usize) -> Option<Value> {
    file.seek(SeekFrom::Start(start)).ok()?;
    let mut line = vec![0u8; length];
    file.read_exact(&mut line).ok()?;
    serde_json::from_slice(&line).ok()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exports_keep_truncated_text_as_utf8() {
        let body = "响应正文".as_bytes();
        let export = CaptureExport::new(body, body.len(), true, 7);
        assert_eq!(export.encoding, "utf-8");
        assert_eq!(export.body, "响应");
        assert_eq!(export.bytes, 12);
        assert!(export.truncated);

        let whole = CaptureExport::new(body, body.len(), true, 64);
        assert_eq!(whole.body, "响应正文");
        assert!(!whole.truncated);

        let partial = CaptureExport::new(b"abc", 10, false, 64);
        assert!(partial.truncated, "an incomplete capture is truncated");

        let binary = CaptureExport::new(b"\xff\x00", 2, true, 64);
        assert_eq!(binary.encoding, "base64");
    }

    #[test]
    fn expired_day_files_are_deleted() {
        let directory = std::env::temp_dir().join(format!(
            "llm-proxy-retention-{}-{}",
            std::process::id(),
            crate::util::uid("t")
        ));
        fs::create_dir_all(&directory).unwrap();
        let today = crate::util::now()[..10].to_string();
        for day in ["2001-01-01", today.as_str()] {
            fs::write(directory.join(format!("{day}.jsonl")), b"{}\n").unwrap();
        }
        fs::write(directory.join("notes.txt"), b"kept").unwrap();
        let logger = CallLogger::open(&directory, 7).unwrap();
        logger.flush();
        assert_eq!(logger.list_days(), vec![today]);
        assert!(directory.join("notes.txt").exists());
        logger.close();

        let keep_all = CallLogger::open(&directory, 0).unwrap();
        fs::write(directory.join("2001-01-01.jsonl"), b"{}\n").unwrap();
        keep_all.flush();
        assert_eq!(keep_all.list_days().len(), 2);
        keep_all.close();
        let _ = fs::remove_dir_all(&directory);
    }
}
