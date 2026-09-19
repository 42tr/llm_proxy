//! Single process LLM proxy with a browser-managed SQLite configuration.
//!
//! There are no external services: upstream providers and model mappings are configured through
//! the browser admin page, stored in local SQLite, and clients only send `model`.
pub mod admin;
pub mod error;
pub mod fernet;
pub mod http;
pub mod logs;
pub mod proxy;
pub mod store;
pub mod util;

use std::env;
use std::io::{self, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::os::unix::io::AsRawFd;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde_json::{json, Value};

use crate::error::{ApiError, Result};
use crate::http::{authorized, is_disconnect, is_timeout, Conn, Request};
use crate::logs::CallLogger;
use crate::store::ConfigStore;
use crate::util::{token_file, uid};

pub const MAX_BODY: usize = 10 * 1024 * 1024;
pub const MAX_RESPONSE: usize = 50 * 1024 * 1024;
pub const MAX_LOG: usize = 2 * 1024 * 1024;

const MAX_CONCURRENCY: usize = 32;
const MAX_WORKERS: usize = 64;
const HEADER_TIMEOUT: Duration = Duration::from_secs(15);
const POLL_MILLIS: i32 = 200;

const ADMIN_HTML: &str = include_str!("../static/admin.html");
const ADMIN_JS: &str = include_str!("../static/admin.js");
const ADMIN_CSS: &str = include_str!("../static/admin.css");
const ADMIN_FAVICON: &str = include_str!("../static/favicon.svg");
const CSP: &str =
    "default-src 'self'; script-src 'self'; style-src 'self'; frame-ancestors 'none'; base-uri 'none'";

static STOP: AtomicBool = AtomicBool::new(false);

pub struct App {
    pub store: ConfigStore,
    pub logger: CallLogger,
    pub admin_key: String,
    pub proxy_key: String,
    pub calls: Semaphore,
    pub max_body_bytes: usize,
    pub max_response_bytes: usize,
    pub max_log_bytes: usize,
}

impl App {
    /// Open the data directory, generating access tokens on first start.
    pub fn new(
        db_path: &Path,
        log_dir: &Path,
        admin_key: Option<String>,
        proxy_key: Option<String>,
    ) -> std::result::Result<Self, String> {
        let store = ConfigStore::open(db_path, env::var("LLM_PROXY_MASTER_KEY").ok().as_deref())
            .map_err(|error| error.message)?;
        let parent = db_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        let admin_key = key(
            admin_key,
            "LLM_PROXY_ADMIN_KEY",
            &parent.join("admin.token"),
        )
        .map_err(|error| error.to_string())?;
        let proxy_key = key(proxy_key, "LLM_PROXY_API_KEY", &parent.join("client.token"))
            .map_err(|error| error.to_string())?;
        if admin_key == proxy_key {
            return Err("admin and client API keys must be different".to_string());
        }
        Ok(Self {
            store,
            logger: CallLogger::open(log_dir).map_err(|error| error.message)?,
            admin_key,
            proxy_key,
            calls: Semaphore::new(MAX_CONCURRENCY),
            max_body_bytes: MAX_BODY,
            max_response_bytes: MAX_RESPONSE,
            max_log_bytes: MAX_LOG,
        })
    }
}

fn key(supplied: Option<String>, variable: &str, path: &Path) -> io::Result<String> {
    if let Some(value) = supplied.filter(|value| !value.is_empty()) {
        return Ok(value);
    }
    if let Ok(value) = env::var(variable) {
        if !value.trim().is_empty() {
            return Ok(value);
        }
    }
    token_file(path)
}

/// Counting permits with non-blocking acquisition, matching the Python bounded semaphores.
pub struct Semaphore {
    used: Mutex<usize>,
    max: usize,
}

impl Semaphore {
    pub fn new(max: usize) -> Self {
        Self {
            used: Mutex::new(0),
            max,
        }
    }

    pub fn try_acquire(&self) -> bool {
        let mut used = self.guard();
        if *used < self.max {
            *used += 1;
            true
        } else {
            false
        }
    }

    pub fn release(&self) {
        let mut used = self.guard();
        *used = used.saturating_sub(1);
    }

    pub fn in_use(&self) -> usize {
        *self.guard()
    }

    fn guard(&self) -> MutexGuard<'_, usize> {
        self.used
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

pub fn bind(host: &str, port: u16) -> io::Result<TcpListener> {
    let address = (host, port).to_socket_addrs()?.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("cannot resolve {host}"),
        )
    })?;
    TcpListener::bind(address)
}

/// Accept connections until SIGINT or SIGTERM, then drain workers and the call log queue.
pub fn serve(listener: TcpListener, app: Arc<App>) {
    install_signal_handlers();
    let workers = Arc::new(Semaphore::new(MAX_WORKERS));
    let handles: Mutex<Vec<JoinHandle<()>>> = Mutex::new(Vec::new());
    let descriptor = listener.as_raw_fd();
    if listener.set_nonblocking(true).is_err() {
        eprintln!("ERROR: the listener must support polling");
        return;
    }
    while !STOP.load(Ordering::SeqCst) {
        if !readable(descriptor, POLL_MILLIS) {
            continue;
        }
        match listener.accept() {
            Ok((stream, _peer)) => {
                if !workers.try_acquire() {
                    reject(stream);
                    continue;
                }
                let app = Arc::clone(&app);
                let permits = Arc::clone(&workers);
                let spawned = thread::Builder::new()
                    .name("worker".to_string())
                    .spawn(move || {
                        handle(app, stream);
                        permits.release();
                    });
                match spawned {
                    Ok(worker) => {
                        let mut guard = handles
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        guard.retain(|finished| !finished.is_finished());
                        guard.push(worker);
                    }
                    Err(_) => workers.release(),
                }
            }
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                ) => {}
            Err(err) => {
                eprintln!("ERROR: accept failed: {err}");
                thread::sleep(Duration::from_millis(20));
            }
        }
    }
    for worker in handles
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .drain(..)
    {
        let _ = worker.join();
    }
    app.logger.close();
}

fn reject(stream: TcpStream) {
    let _ = stream.set_write_timeout(Some(Duration::from_secs(1)));
    let _ = (&stream).write_all(
        b"HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
    );
    let _ = stream.shutdown(std::net::Shutdown::Both);
}

fn handle(app: Arc<App>, stream: TcpStream) {
    let _ = stream.set_nodelay(true);
    let request_id = uid("req");
    let Ok(mut conn) = Conn::new(stream, request_id.clone()) else {
        return;
    };
    let _ = conn.set_read_timeout(Some(HEADER_TIMEOUT));
    let _ = conn.set_write_timeout(Some(HEADER_TIMEOUT));
    let request = match conn.read_request() {
        Ok(Some(request)) => request,
        Ok(None) => return,
        Err(err) => {
            if !is_disconnect(&err) && !is_timeout(&err) {
                conn.fail(&ApiError::bad("malformed request"));
            }
            return;
        }
    };
    match std::panic::catch_unwind(AssertUnwindSafe(|| dispatch(&app, &request, &mut conn))) {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            // Never include exception details: URLs and credentials can be embedded in them.
            if error.status >= 500 {
                eprintln!("ERROR: internal request failure {request_id}");
            }
            conn.fail(&error);
        }
        Err(_) => {
            eprintln!("ERROR: internal request failure {request_id}");
            conn.fail(&ApiError::internal("internal server error"));
        }
    }
    conn.close();
}

fn dispatch(app: &App, request: &Request, conn: &mut Conn) -> Result<()> {
    let (path, method) = (request.path.as_str(), request.method.as_str());
    if path == "/v1/chat/completions" && method == "POST" {
        proxy::chat(app, request, conn, None);
        return Ok(());
    }
    if path == "/v1/models" && method == "GET" {
        if !authorized(request, &app.proxy_key) {
            return Err(ApiError::unauthorized("client authorization required"));
        }
        let models: Vec<Value> = app
            .store
            .routes()?
            .iter()
            .filter(|route| route.enabled && route.provider_enabled)
            .map(|route| {
                json!({ "id": route.public_model, "object": "model", "created": 0, "owned_by": "llm-proxy" })
            })
            .collect();
        let _ = conn.send_json(200, &json!({ "object": "list", "data": models }));
        return Ok(());
    }
    if path == "/healthz" && method == "GET" {
        let _ = conn.send_json(200, &json!({ "status": "ok" }));
        return Ok(());
    }
    if method == "GET"
        && matches!(
            path,
            "/" | "/admin"
                | "/admin/"
                | "/static/admin.js"
                | "/static/admin.css"
                | "/static/favicon.svg"
        )
    {
        let (body, mime) = match path {
            "/static/admin.js" => (ADMIN_JS, "text/javascript"),
            "/static/admin.css" => (ADMIN_CSS, "text/css"),
            "/static/favicon.svg" => (ADMIN_FAVICON, "image/svg+xml"),
            _ => (ADMIN_HTML, "text/html; charset=utf-8"),
        };
        let extra = [
            ("Content-Security-Policy".to_string(), CSP.to_string()),
            ("Referrer-Policy".to_string(), "no-referrer".to_string()),
        ];
        let _ = conn.send(200, body.as_bytes(), mime, "no-store", &extra);
        return Ok(());
    }
    let parts: Vec<&str> = path.trim_matches('/').split('/').collect();
    if parts.len() < 2 || parts[0] != "api" || parts[1] != "admin" {
        return Err(ApiError::not_found("not found"));
    }
    if !authorized(request, &app.admin_key) {
        return Err(ApiError::unauthorized("admin authorization required"));
    }
    admin::dispatch(app, method, &parts[2..], request, conn)
}

fn readable(descriptor: i32, millis: i32) -> bool {
    let mut polled = libc::pollfd {
        fd: descriptor,
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: `polled` is a valid pollfd for an open descriptor owned by this process.
    unsafe { libc::poll(&mut polled, 1, millis) > 0 }
}

fn install_signal_handlers() {
    // SAFETY: registering process-wide signal handlers with an async-signal-safe callback.
    unsafe {
        libc::signal(
            libc::SIGINT,
            request_stop as *const () as libc::sighandler_t,
        );
        libc::signal(
            libc::SIGTERM,
            request_stop as *const () as libc::sighandler_t,
        );
        libc::signal(libc::SIGPIPE, libc::SIG_IGN);
    }
}

extern "C" fn request_stop(_: libc::c_int) {
    STOP.store(true, Ordering::SeqCst);
}

/// Command line arguments with the documented environment variable fallbacks.
pub struct Args {
    pub host: String,
    pub port: u16,
    pub data_dir: String,
}

const USAGE: &str = "usage: llm-proxy [--host HOST] [--port PORT] [--data-dir DIR]

Options default to LLM_PROXY_HOST, LLM_PROXY_PORT and LLM_PROXY_DATA_DIR, then to
127.0.0.1, 8080 and ./data. Secrets come from LLM_PROXY_ADMIN_KEY, LLM_PROXY_API_KEY
and LLM_PROXY_MASTER_KEY, otherwise they are generated inside the data directory.
";

impl Args {
    pub fn parse() -> Self {
        let mut host = env::var("LLM_PROXY_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
        let mut port = env::var("LLM_PROXY_PORT").unwrap_or_else(|_| "8080".to_string());
        let mut data_dir = env::var("LLM_PROXY_DATA_DIR").unwrap_or_else(|_| "./data".to_string());
        let mut argv = env::args().skip(1);
        while let Some(argument) = argv.next() {
            let (name, inline) = match argument.split_once('=') {
                Some((name, value)) => (name.to_string(), Some(value.to_string())),
                None => (argument.clone(), None),
            };
            let inline_value = inline.clone();
            let value = match inline_value {
                Some(value) => value,
                None => argv
                    .next()
                    .unwrap_or_else(|| fail(&format!("{name} requires a value"))),
            };
            match name.as_str() {
                "--host" => host = value,
                "--port" => port = value,
                "--data-dir" => data_dir = value,
                "--help" | "-h" => {
                    print!("{USAGE}");
                    std::process::exit(0);
                }
                other => fail(&format!("unrecognized argument {other}")),
            }
        }
        let number: i64 = port.trim().parse().unwrap_or(-1);
        if !(0..=65535).contains(&number) {
            fail("port must be between 0 and 65535");
        }
        Self {
            host,
            port: number as u16,
            data_dir,
        }
    }
}

fn fail(message: &str) -> ! {
    eprintln!("llm-proxy: error: {message}");
    eprint!("{USAGE}");
    std::process::exit(2);
}

/// Process entry point: open the data directory, bind and serve until interrupted.
pub fn main() {
    let args = Args::parse();
    let data = PathBuf::from(&args.data_dir);
    let app = match App::new(&data.join("proxy.sqlite3"), &data.join("logs"), None, None) {
        Ok(app) => Arc::new(app),
        Err(message) => {
            eprintln!("ERROR: {message}");
            std::process::exit(1);
        }
    };
    let listener = match bind(&args.host, args.port) {
        Ok(listener) => listener,
        Err(err) => {
            eprintln!("ERROR: cannot listen on {}:{}: {err}", args.host, args.port);
            std::process::exit(1);
        }
    };
    let port = listener
        .local_addr()
        .map(|address| address.port())
        .unwrap_or(args.port);
    println!("LLM Proxy: http://{}:{port}/admin", args.host);
    if env::var_os("LLM_PROXY_ADMIN_KEY").is_none() {
        println!("Admin login token: {}", data.join("admin.token").display());
    }
    if env::var_os("LLM_PROXY_API_KEY").is_none() {
        println!("Client API token: {}", data.join("client.token").display());
    }
    serve(listener, app);
}
