//! OpenAI compatible `chat/completions` forwarding with unbuffered SSE passthrough.
use std::collections::HashMap;
use std::io::{self, Read};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde_json::{Map, Value};
use ureq::Agent;

use crate::error::ApiError;
use crate::http::{authorized, is_disconnect, is_timeout, Conn, Request};
use crate::logs::{Capture, Record};
use crate::store::{text_field, ResolvedRoute};
use crate::App;

const MAX_CHUNK: usize = 32 * 1024;
/// Header names whose values are replaced with `[REDACTED]` in logged bodies.
const SECRET_NAMES: [&str; 8] = [
    "authorization",
    "proxy-authorization",
    "api-key",
    "api_key",
    "apikey",
    "access_token",
    "password",
    "secret",
];
/// Upstream response headers worth forwarding to the client.
const FORWARDED: [&str; 4] = [
    "content-type",
    "content-encoding",
    "retry-after",
    "www-authenticate",
];

/// Why a call failed, before it is mapped onto a status code and a log record.
enum Fault {
    Api(ApiError),
    Timeout,
    Upstream,
    ClientGone,
}

/// Payload and route supplied by the admin connectivity test.
pub struct Supplied {
    pub payload: Value,
    pub route: ResolvedRoute,
}

pub fn chat(app: &App, request: &Request, conn: &mut Conn, supplied: Option<Supplied>) {
    let source = if supplied.is_some() {
        "admin_test"
    } else {
        "client"
    };
    let request_id = conn.request_id.clone();
    let mut call = Call {
        app,
        conn,
        record: Record::new(&request_id, source),
        route: None,
        payload: None,
        capture: None,
        acquired: false,
        started: Instant::now(),
    };
    let fault = call.run(request, supplied).err();
    call.finish(fault);
}

struct Call<'a> {
    app: &'a App,
    conn: &'a mut Conn,
    record: Record,
    route: Option<ResolvedRoute>,
    payload: Option<Value>,
    capture: Option<Capture>,
    acquired: bool,
    started: Instant,
}

impl Call<'_> {
    fn run(&mut self, request: &Request, supplied: Option<Supplied>) -> Result<(), Fault> {
        let payload = match supplied {
            Some(supplied) => {
                self.route = Some(supplied.route);
                supplied.payload
            }
            None => {
                if !authorized(request, &self.app.proxy_key) {
                    return Err(Fault::Api(ApiError::unauthorized(
                        "client authorization required",
                    )));
                }
                self.conn
                    .read_json(request, self.app.max_body_bytes)
                    .map_err(Fault::Api)?
            }
        };
        let Value::Object(map) = &payload else {
            return Err(Fault::Api(ApiError::bad(
                "request body must be a JSON object",
            )));
        };
        let model = text_field(map, "model").map_err(Fault::Api)?;
        let stream = match map.get("stream") {
            None => false,
            Some(Value::Bool(flag)) => *flag,
            Some(_) => return Err(Fault::Api(ApiError::bad("stream must be a boolean"))),
        };
        if !matches!(map.get("messages"), Some(Value::Array(messages)) if !messages.is_empty()) {
            return Err(Fault::Api(ApiError::bad(
                "messages must be a non-empty array",
            )));
        }
        // Header and body parsing has completed; long-running streams should not be
        // terminated by the request-header timeout.
        let _ = self.conn.set_read_timeout(None);
        self.record.model = Some(model.clone());
        self.record.stream = stream;
        self.record.request_bytes = compact(&payload).len();
        if self.route.is_none() {
            self.route = Some(self.app.store.resolve(&model).map_err(Fault::Api)?);
        }
        let route = self.route.clone().expect("route resolved above");
        self.record.provider_id = Some(route.provider_id().to_string());
        self.record.upstream_model = Some(route.upstream_model.clone());
        if !self.app.calls.try_acquire() {
            return Err(Fault::Api(ApiError::new(
                429,
                "proxy concurrency limit reached",
                "rate_limit_error",
            )));
        }
        self.acquired = true;
        let _ = self
            .conn
            .set_write_timeout(Some(route.timeout().max(Duration::from_secs(5))));
        self.capture = Some(Capture::new(
            self.app.max_log_bytes,
            route.provider.log_response_body,
        ));
        let body = upstream_body(map, &route);
        self.payload = Some(payload);
        self.forward(&route, stream, body)
    }

    fn forward(&mut self, route: &ResolvedRoute, stream: bool, body: Vec<u8>) -> Result<(), Fault> {
        let timeout = route.provider.timeout_ms.max(0) as u64;
        let agent = agent_for(timeout);
        let mut request = agent
            .post(&route.provider.endpoint_url)
            .header("Content-Type", "application/json")
            .header("Accept-Encoding", "identity")
            .header(
                "Accept",
                if stream {
                    "text/event-stream"
                } else {
                    "application/json"
                },
            );
        for (name, value) in route.extra_headers() {
            request = request.header(name, value);
        }
        request = match route.provider.auth_type.as_str() {
            "bearer" => request.header("Authorization", format!("Bearer {}", route.api_key())),
            "api_key" => request.header("api-key", route.api_key()),
            "custom_header" => {
                request.header(route.provider.auth_header_name.as_str(), route.api_key())
            }
            _ => request,
        };
        let deadline = self.started + route.timeout();
        let response = request
            .send(body.as_slice())
            .map_err(|err| classify(err, deadline))?;
        let status = response.status().as_u16();
        self.record.upstream_status = Some(status);
        let (content_type, extra) = forwarded_headers(response.headers());
        let is_sse = content_type
            .split(';')
            .next()
            .unwrap_or("")
            .trim()
            .eq_ignore_ascii_case("text/event-stream");
        let mut reader = response.into_body().into_reader();
        if is_sse {
            self.stream_body(&mut reader, status, &content_type, &extra, deadline)?;
        } else {
            self.buffer_body(&mut reader, status, &content_type, &extra, deadline)?;
        }
        self.record.outcome = if status < 400 {
            "completed"
        } else {
            "upstream_error"
        };
        Ok(())
    }

    /// Forward SSE chunks as they arrive; close-delimited framing keeps latency flat.
    fn stream_body(
        &mut self,
        reader: &mut impl Read,
        status: u16,
        content_type: &str,
        extra: &[(String, String)],
        deadline: Instant,
    ) -> Result<(), Fault> {
        self.conn
            .start_response(status, content_type, "no-cache", None, extra)
            .map_err(|err| {
                if is_disconnect(&err) {
                    Fault::ClientGone
                } else {
                    Fault::Upstream
                }
            })?;
        self.record.status = status;
        let mut buffer = vec![0u8; MAX_CHUNK];
        loop {
            if Instant::now() >= deadline {
                return Err(Fault::Timeout);
            }
            match reader.read(&mut buffer) {
                Ok(0) => return Ok(()),
                Ok(len) => {
                    let chunk = &buffer[..len];
                    self.note_first_byte();
                    if self.over_size_limit(chunk) {
                        return Err(Fault::Api(ApiError::new(
                            502,
                            "upstream response exceeds size limit",
                            "response_too_large",
                        )));
                    }
                    if let Err(err) = self.conn.write_chunk(chunk) {
                        return Err(if is_disconnect(&err) {
                            Fault::ClientGone
                        } else {
                            Fault::Upstream
                        });
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                Err(err) => return Err(classify_io(&err, deadline)),
            }
        }
    }

    /// Non-stream responses are buffered so the upstream status and body can be forwarded verbatim.
    fn buffer_body(
        &mut self,
        reader: &mut impl Read,
        status: u16,
        content_type: &str,
        extra: &[(String, String)],
        deadline: Instant,
    ) -> Result<(), Fault> {
        let mut body = Vec::new();
        let mut buffer = vec![0u8; MAX_CHUNK];
        loop {
            if Instant::now() >= deadline {
                return Err(Fault::Timeout);
            }
            match reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(len) => {
                    let chunk = &buffer[..len];
                    self.note_first_byte();
                    if self.over_size_limit(chunk) {
                        return Err(Fault::Api(ApiError::new(
                            502,
                            "upstream response exceeds size limit",
                            "response_too_large",
                        )));
                    }
                    body.extend_from_slice(chunk);
                }
                Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                Err(err) => return Err(classify_io(&err, deadline)),
            }
        }
        self.conn
            .start_response(status, content_type, "no-cache", Some(body.len()), extra)
            .map_err(|err| {
                if is_disconnect(&err) {
                    Fault::ClientGone
                } else {
                    Fault::Upstream
                }
            })?;
        self.record.status = status;
        self.conn.write_chunk(&body).map_err(|err| {
            if is_disconnect(&err) {
                Fault::ClientGone
            } else {
                Fault::Upstream
            }
        })
    }

    fn note_first_byte(&mut self) {
        if self.record.first_byte_latency_ms.is_none() {
            self.record.first_byte_latency_ms = Some(self.started.elapsed().as_millis() as u64);
        }
    }

    /// Add a chunk to the capture and report whether the response grew past the hard limit.
    fn over_size_limit(&mut self, chunk: &[u8]) -> bool {
        let Some(capture) = self.capture.as_mut() else {
            return false;
        };
        capture.add(chunk);
        capture.total > self.app.max_response_bytes
    }

    fn finish(&mut self, fault: Option<Fault>) {
        match fault {
            None => {}
            Some(Fault::Api(error)) => {
                self.record.error = Some(error.kind.to_string());
                if !self.conn.sent() {
                    self.record.status = error.status;
                }
                self.conn.fail(&error);
            }
            Some(Fault::ClientGone) => {
                self.record.error = Some("client_disconnected".to_string());
                if !self.conn.sent() {
                    self.record.status = 499;
                }
            }
            Some(Fault::Timeout) => {
                self.record.error = Some("upstream_timeout".to_string());
                if !self.conn.sent() {
                    self.record.status = 504;
                }
                self.conn.fail(&ApiError::new(
                    504,
                    "upstream request timed out",
                    "upstream_timeout",
                ));
            }
            Some(Fault::Upstream) => {
                self.record.error = Some("upstream_error".to_string());
                if !self.conn.sent() {
                    self.record.status = 502;
                }
                self.conn.fail(&ApiError::new(
                    502,
                    "upstream request failed",
                    "upstream_error",
                ));
            }
        }
        if self.acquired {
            self.app.calls.release();
        }
        self.record.latency_ms = self.started.elapsed().as_millis() as u64;
        if let (Some(route), Some(payload)) = (self.route.take(), self.payload.take()) {
            if route.provider.log_request_body {
                let mut capture = Capture::new(self.app.max_log_bytes, true);
                let redacted = compact(&redact(&payload));
                capture.add(&mask_secrets(&redacted, &route));
                self.record.request = Some(capture.export());
            }
            if let Some(capture) = self.capture.take() {
                self.record.response_bytes = capture.total;
                if route.provider.log_response_body {
                    // Redact structured secret fields where a complete JSON response is available.
                    let raw = serde_json::from_slice::<Value>(&capture.data)
                        .map(|value| compact(&redact(&value)))
                        .unwrap_or_else(|_| capture.data.clone());
                    let masked = mask_secrets(&raw, &route);
                    self.record.response = Some(
                        capture
                            .export_replaced(&masked[..masked.len().min(self.app.max_log_bytes)]),
                    );
                }
            }
            self.route = Some(route);
        }
        let record = std::mem::replace(
            &mut self.record,
            Record::new(&self.conn.request_id, "client"),
        );
        self.app.logger.write(record);
    }
}

fn classify(err: ureq::Error, deadline: Instant) -> Fault {
    match &err {
        ureq::Error::Timeout(_) => Fault::Timeout,
        ureq::Error::Io(io) if is_timeout(io) => Fault::Timeout,
        _ if Instant::now() >= deadline => Fault::Timeout,
        _ => Fault::Upstream,
    }
}

fn classify_io(err: &io::Error, deadline: Instant) -> Fault {
    if is_timeout(err) || Instant::now() >= deadline {
        Fault::Timeout
    } else if is_disconnect(err) {
        Fault::ClientGone
    } else {
        Fault::Upstream
    }
}

/// Rewrite the client model to the upstream model, leaving everything else untouched.
fn upstream_body(payload: &Map<String, Value>, route: &ResolvedRoute) -> Vec<u8> {
    let mut body = payload.clone();
    body.insert(
        "model".to_string(),
        Value::String(route.upstream_model.clone()),
    );
    compact(&Value::Object(body))
}

fn compact(value: &Value) -> Vec<u8> {
    serde_json::to_vec(value).unwrap_or_default()
}

fn forwarded_headers(headers: &ureq::http::HeaderMap) -> (String, Vec<(String, String)>) {
    let hop: Vec<String> = headers
        .get_all("connection")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(|value| value.trim().to_ascii_lowercase())
        .collect();
    let mut content_type = String::new();
    let mut extra = Vec::new();
    for (name, value) in headers.iter() {
        let lower = name.as_str().to_ascii_lowercase();
        if !FORWARDED.contains(&lower.as_str()) || hop.contains(&lower) {
            continue;
        }
        let Ok(text) = value.to_str() else { continue };
        if lower == "content-type" {
            content_type = text.to_string();
        } else {
            extra.push((name.as_str().to_string(), text.to_string()));
        }
    }
    (content_type, extra)
}

fn redact(value: &Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(key, item)| {
                    let hidden = SECRET_NAMES.contains(&key.to_lowercase().as_str());
                    (
                        key.clone(),
                        if hidden {
                            Value::String("[REDACTED]".to_string())
                        } else {
                            redact(item)
                        },
                    )
                })
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.iter().map(redact).collect()),
        other => other.clone(),
    }
}

/// Scrub the upstream credential and configured header values out of a logged body.
fn mask_secrets(data: &[u8], route: &ResolvedRoute) -> Vec<u8> {
    let replacement = b"[REDACTED]";
    let mut secrets: Vec<&str> = Vec::new();
    if !route.api_key().is_empty() {
        secrets.push(route.api_key());
    }
    secrets.extend(
        route
            .extra_headers()
            .map(|(_, value)| value)
            .filter(|value| !value.is_empty()),
    );
    let mut masked = data.to_vec();
    for secret in secrets {
        masked = replace_all(&masked, secret.as_bytes(), replacement);
    }
    masked
}

fn replace_all(data: &[u8], needle: &[u8], replacement: &[u8]) -> Vec<u8> {
    if needle.is_empty() {
        return data.to_vec();
    }
    let mut out = Vec::with_capacity(data.len());
    let mut index = 0;
    while let Some(offset) = data[index..]
        .windows(needle.len())
        .position(|window| window == needle)
    {
        out.extend_from_slice(&data[index..index + offset]);
        out.extend_from_slice(replacement);
        index += offset + needle.len();
    }
    out.extend_from_slice(&data[index..]);
    out
}

/// Agents are cheap to clone but expensive to build, so cache one per upstream timeout.
fn agent_for(timeout_ms: u64) -> Agent {
    static AGENTS: OnceLock<Mutex<HashMap<u64, Agent>>> = OnceLock::new();
    let pool = AGENTS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut agents = pool.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(agent) = agents.get(&timeout_ms) {
        return agent.clone();
    }
    let timeout = Duration::from_millis(timeout_ms.max(100));
    let agent = Agent::config_builder()
        .http_status_as_error(false)
        .max_redirects(0)
        .timeout_connect(Some(timeout.min(Duration::from_secs(10))))
        .timeout_global(Some(timeout))
        .user_agent(ureq::config::AutoHeaderValue::None)
        .accept(ureq::config::AutoHeaderValue::None)
        .accept_encoding(ureq::config::AutoHeaderValue::None)
        .build()
        .new_agent();
    agents.insert(timeout_ms, agent.clone());
    agent
}
