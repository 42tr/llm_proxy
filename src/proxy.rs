//! OpenAI compatible `chat/completions` forwarding with unbuffered SSE passthrough.
use std::collections::HashMap;
use std::io::{self, Read};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use memchr::memmem;
use serde_json::{Map, Value};
use ureq::Agent;

use crate::error::ApiError;
use crate::http::{authorized, is_disconnect, is_timeout, parse_object, Conn, Request};
use crate::logs::{CaptureExport, Record};
use crate::store::{is_secret_header, text_field, AuthType, ResolvedRoute};
use crate::{App, Permit};

const MAX_CHUNK: usize = 32 * 1024;
/// Upper bound for the body of a streamed response. The provider timeout only bounds the
/// wait for the response headers, so long generations are not cut off mid-stream.
pub const STREAM_TIMEOUT: Duration = Duration::from_secs(60 * 60);
/// Credentials shorter than this are not scrubbed from logged bodies: masking them would
/// mangle ordinary text far more often than it would hide a real secret.
const MIN_MASKED_SECRET: usize = 8;
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
    let record = Record::new(&conn.request_id, source);
    let mut call = Call {
        app,
        conn,
        record,
        route: None,
        payload: None,
        response: Vec::new(),
        log_response: false,
        started: Instant::now(),
        permit: None,
    };
    let fault = call.run(request, supplied).err();
    call.finish(fault);
}

struct Call<'a> {
    app: &'a App,
    conn: &'a mut Conn,
    record: Record,
    route: Option<Arc<ResolvedRoute>>,
    payload: Option<Value>,
    /// The buffered response body, or for streams the first `max_log_bytes` of it when logged.
    response: Vec<u8>,
    log_response: bool,
    started: Instant,
    /// Held for the whole call and released on drop, even if the call panics.
    permit: Option<Permit<'a>>,
}

impl Call<'_> {
    fn run(&mut self, request: &Request, supplied: Option<Supplied>) -> Result<(), Fault> {
        let (mut payload, request_bytes) = match supplied {
            Some(supplied) => {
                self.route = Some(Arc::new(supplied.route));
                let bytes = compact(&supplied.payload).len();
                (supplied.payload, bytes)
            }
            None => {
                if !authorized(request, &self.app.proxy_key) {
                    return Err(Fault::Api(ApiError::unauthorized(
                        "client authorization required",
                    )));
                }
                let raw = self
                    .conn
                    .read_body(request, self.app.max_body_bytes, self.app.body_timeout)
                    .map_err(Fault::Api)?;
                (parse_object(&raw).map_err(Fault::Api)?, raw.len())
            }
        };
        self.record.request_bytes = request_bytes;
        let Value::Object(map) = &mut payload else {
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
        self.record.model = Some(model.clone());
        self.record.stream = stream;
        let route = match &self.route {
            Some(route) => Arc::clone(route),
            None => self.app.store.resolve(&model).map_err(Fault::Api)?,
        };
        self.route = Some(Arc::clone(&route));
        self.record.provider_id = Some(route.provider_id().to_string());
        self.record.upstream_model = Some(route.upstream_model.clone());
        let Some(permit) = self.app.calls.try_acquire() else {
            return Err(Fault::Api(ApiError::new(
                429,
                "proxy concurrency limit reached",
                "rate_limit_error",
            )));
        };
        self.permit = Some(permit);
        let _ = self
            .conn
            .set_write_timeout(Some(route.timeout().max(Duration::from_secs(5))));
        self.log_response = route.provider.log_response_body;
        let body = upstream_body(map, &route.upstream_model);
        self.payload = Some(payload);
        self.forward(&route, stream, body)
    }

    fn forward(&mut self, route: &ResolvedRoute, stream: bool, body: Vec<u8>) -> Result<(), Fault> {
        let timeout = route.timeout();
        let mut request = agent_for(timeout, stream)
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
        request = match route.provider.auth_type {
            AuthType::Bearer => {
                request.header("Authorization", format!("Bearer {}", route.api_key()))
            }
            AuthType::ApiKey => request.header("api-key", route.api_key()),
            AuthType::CustomHeader => {
                request.header(route.provider.auth_header_name.as_str(), route.api_key())
            }
            AuthType::None => request,
        };
        let headers_by = self.started + timeout;
        let response = request
            .send(body.as_slice())
            .map_err(|err| classify(err, headers_by))?;
        drop(body);
        let status = response.status().as_u16();
        self.record.upstream_status = Some(status);
        let (content_type, extra) = forwarded_headers(response.headers());
        let is_sse = content_type
            .split(';')
            .next()
            .unwrap_or("")
            .trim()
            .eq_ignore_ascii_case("text/event-stream");
        let deadline = if stream {
            Instant::now() + STREAM_TIMEOUT
        } else {
            headers_by
        };
        let mut reader = response.into_body().into_reader();
        if is_sse {
            self.conn
                .start_response(status, &content_type, "no-cache", None, &extra)
                .map_err(write_fault)?;
            self.record.status = status;
            self.pump(&mut reader, deadline, true)?;
        } else {
            // Buffered so the upstream status and body can be forwarded verbatim.
            self.pump(&mut reader, deadline, false)?;
            self.conn
                .start_response(
                    status,
                    &content_type,
                    "no-cache",
                    Some(self.response.len()),
                    &extra,
                )
                .map_err(write_fault)?;
            self.record.status = status;
            self.conn.write_chunk(&self.response).map_err(write_fault)?;
        }
        self.record.outcome = if status < 400 {
            "completed"
        } else {
            "upstream_error"
        };
        Ok(())
    }

    /// Read the upstream body until EOF. Streamed chunks are forwarded as they arrive
    /// (close-delimited framing keeps latency flat); otherwise the body is buffered.
    fn pump(
        &mut self,
        reader: &mut impl Read,
        deadline: Instant,
        stream: bool,
    ) -> Result<(), Fault> {
        let mut buffer = vec![0u8; MAX_CHUNK];
        loop {
            if Instant::now() >= deadline {
                return Err(Fault::Timeout);
            }
            let len = match reader.read(&mut buffer) {
                Ok(0) => return Ok(()),
                Ok(len) => len,
                Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                Err(err) => return Err(classify_io(&err, deadline)),
            };
            let chunk = &buffer[..len];
            if self.record.first_byte_latency_ms.is_none() {
                self.record.first_byte_latency_ms = Some(self.started.elapsed().as_millis() as u64);
            }
            self.record.response_bytes += len;
            if self.record.response_bytes > self.app.max_response_bytes {
                return Err(Fault::Api(ApiError::new(
                    502,
                    "upstream response exceeds size limit",
                    "response_too_large",
                )));
            }
            if !stream {
                self.response.extend_from_slice(chunk);
                continue;
            }
            if self.log_response {
                let room = self
                    .app
                    .max_log_bytes
                    .saturating_sub(self.response.len())
                    .min(len);
                self.response.extend_from_slice(&chunk[..room]);
            }
            self.conn.write_chunk(chunk).map_err(write_fault)?;
        }
    }

    fn finish(mut self, fault: Option<Fault>) {
        if let Some(fault) = fault {
            self.report(fault);
        }
        // Return the permit before the comparatively slow log serialization.
        self.permit = None;
        self.record.latency_ms = self.started.elapsed().as_millis() as u64;
        if let Some(route) = self.route.take() {
            self.capture_bodies(&route);
        }
        self.app.logger.write(self.record);
    }

    /// Record the failure and tell the client, unless a response has already started.
    fn report(&mut self, fault: Fault) {
        let (kind, status, error) = match fault {
            Fault::Api(error) => (error.kind, error.status, Some(error)),
            Fault::ClientGone => ("client_disconnected", 499, None),
            Fault::Timeout => (
                "upstream_timeout",
                504,
                Some(ApiError::new(
                    504,
                    "upstream request timed out",
                    "upstream_timeout",
                )),
            ),
            Fault::Upstream => (
                "upstream_error",
                502,
                Some(ApiError::new(
                    502,
                    "upstream request failed",
                    "upstream_error",
                )),
            ),
        };
        self.record.error = Some(kind.to_string());
        if !self.conn.sent() {
            self.record.status = status;
        }
        if let Some(error) = error {
            self.conn.fail(&error);
        }
    }

    /// Attach redacted request and response bodies when the provider asks for them.
    fn capture_bodies(&mut self, route: &ResolvedRoute) {
        let limit = self.app.max_log_bytes;
        if route.provider.log_request_body {
            if let Some(mut payload) = self.payload.take() {
                redact(&mut payload);
                let masked = mask_secrets(compact(&payload), route);
                self.record.request = Some(CaptureExport::new(
                    &masked,
                    self.record.request_bytes,
                    true,
                    limit,
                ));
            }
        }
        if route.provider.log_response_body {
            let body = std::mem::take(&mut self.response);
            let complete = body.len() == self.record.response_bytes;
            // Redact structured secret fields where a complete, reasonably sized JSON body is
            // available; anything else only has the known credentials scrubbed.
            let parsed = (complete && body.len() <= limit)
                .then(|| serde_json::from_slice::<Value>(&body).ok())
                .flatten();
            let raw = match parsed {
                Some(mut value) => {
                    redact(&mut value);
                    compact(&value)
                }
                None => body,
            };
            let masked = mask_secrets(raw, route);
            self.record.response = Some(CaptureExport::new(
                &masked,
                self.record.response_bytes,
                complete,
                limit,
            ));
        }
    }
}

fn write_fault(err: io::Error) -> Fault {
    if is_disconnect(&err) {
        Fault::ClientGone
    } else {
        Fault::Upstream
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

/// Serialize the payload with the upstream model, leaving everything else untouched.
///
/// The model is swapped in place and restored afterwards instead of cloning a body that may
/// be megabytes large; key order is preserved, so the field keeps its position.
fn upstream_body(payload: &mut Map<String, Value>, upstream_model: &str) -> Vec<u8> {
    let original = payload.insert(
        "model".to_string(),
        Value::String(upstream_model.to_string()),
    );
    let body = serde_json::to_vec(payload).unwrap_or_default();
    if let Some(original) = original {
        payload.insert("model".to_string(), original);
    }
    body
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

/// Replace the values of secret-looking keys, in place, anywhere in the document.
fn redact(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (key, item) in map.iter_mut() {
                if SECRET_NAMES.contains(&key.to_lowercase().as_str()) {
                    *item = Value::String("[REDACTED]".to_string());
                } else {
                    redact(item);
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(redact),
        _ => {}
    }
}

/// Scrub the upstream credential and secret-looking header values out of a logged body.
fn mask_secrets(data: Vec<u8>, route: &ResolvedRoute) -> Vec<u8> {
    let secrets = std::iter::once(route.api_key()).chain(
        route
            .extra_headers()
            .filter(|(name, _)| is_secret_header(name))
            .map(|(_, value)| value),
    );
    secrets
        .filter(|secret| secret.len() >= MIN_MASKED_SECRET)
        .fold(data, |data, secret| {
            replace_all(data, secret.as_bytes(), b"[REDACTED]")
        })
}

/// Replace every occurrence of `needle`; the input is returned untouched when absent.
fn replace_all(data: Vec<u8>, needle: &[u8], replacement: &[u8]) -> Vec<u8> {
    let finder = memmem::Finder::new(needle);
    let mut matches = finder.find_iter(&data).peekable();
    if matches.peek().is_none() {
        return data;
    }
    let mut out = Vec::with_capacity(data.len());
    let mut index = 0;
    for offset in matches {
        out.extend_from_slice(&data[index..offset]);
        out.extend_from_slice(replacement);
        index = offset + needle.len();
    }
    out.extend_from_slice(&data[index..]);
    out
}

/// Agents are cheap to clone but expensive to build, so cache one per timeout and mode.
///
/// Non-stream calls are bounded end to end by the provider timeout. Streams only have to
/// deliver their response headers within it; the body may then run for `STREAM_TIMEOUT`.
fn agent_for(timeout: Duration, stream: bool) -> Agent {
    static AGENTS: OnceLock<Mutex<HashMap<(Duration, bool), Agent>>> = OnceLock::new();
    let pool = AGENTS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut agents = pool.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(agent) = agents.get(&(timeout, stream)) {
        return agent.clone();
    }
    let timeout = timeout.max(Duration::from_millis(100));
    let builder = Agent::config_builder()
        .http_status_as_error(false)
        .max_redirects(0)
        .timeout_connect(Some(timeout.min(Duration::from_secs(10))))
        .user_agent(ureq::config::AutoHeaderValue::None)
        .accept(ureq::config::AutoHeaderValue::None)
        .accept_encoding(ureq::config::AutoHeaderValue::None);
    let builder = if stream {
        builder
            .timeout_send_request(Some(timeout))
            .timeout_send_body(Some(timeout))
            .timeout_recv_response(Some(timeout))
            .timeout_recv_body(Some(STREAM_TIMEOUT))
    } else {
        builder.timeout_global(Some(timeout))
    };
    let agent = builder.build().new_agent();
    agents.insert((timeout, stream), agent.clone());
    agent
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replace_all_handles_repeats_and_absence() {
        assert_eq!(
            replace_all(b"a-xx-b-xx".to_vec(), b"xx", b"[R]"),
            b"a-[R]-b-[R]"
        );
        assert_eq!(replace_all(b"nothing".to_vec(), b"xx", b"[R]"), b"nothing");
    }

    #[test]
    fn redact_hides_nested_secret_fields() {
        let mut value =
            serde_json::json!({ "a": { "API_KEY": "k", "keep": [ { "password": 1 } ] } });
        redact(&mut value);
        assert_eq!(
            value,
            serde_json::json!({ "a": { "API_KEY": "[REDACTED]", "keep": [ { "password": "[REDACTED]" } ] } })
        );
    }

    #[test]
    fn upstream_body_swaps_the_model_without_reordering() {
        let mut payload = serde_json::json!({ "model": "public", "messages": [] });
        let Value::Object(map) = &mut payload else {
            unreachable!()
        };
        let body = upstream_body(map, "upstream");
        assert_eq!(body, br#"{"model":"upstream","messages":[]}"#);
        assert_eq!(
            payload["model"], "public",
            "the logged payload keeps the client model"
        );
    }
}
