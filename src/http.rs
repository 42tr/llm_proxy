//! Minimal HTTP/1.1 server plumbing: request parsing, bounded JSON bodies, response framing.
//!
//! Every response is close-delimited (`Connection: close`), which keeps streamed SSE correct
//! without chunked encoding, exactly like the Python service did.
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use serde_json::{json, Value};
use subtle::ConstantTimeEq;

use crate::error::{ApiError, Result};
use crate::util::http_date;

const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_REQUEST_LINE: usize = 16 * 1024;

pub struct Request {
    pub method: String,
    pub path: String,
    pub query: String,
    headers: Vec<(String, String)>,
}

impl Request {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }

    pub fn header_values<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a str> {
        self.headers
            .iter()
            .filter(move |(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }

    /// The media type without parameters, matching Python `get_content_type()`.
    pub fn content_type(&self) -> String {
        self.header("content-type")
            .unwrap_or("text/plain")
            .split(';')
            .next()
            .unwrap_or("")
            .trim()
            .to_lowercase()
    }

    pub fn query_param(&self, name: &str) -> Option<String> {
        self.query.split('&').find_map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            // Python `parse_qs` drops blank values, so an empty parameter means "not supplied".
            let decoded = percent_decode(value);
            (key == name && !decoded.is_empty()).then_some(decoded)
        })
    }
}

/// One accepted connection: a buffered reader plus a cloned writer handle for the same socket.
pub struct Conn {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
    pub request_id: String,
    sent: bool,
}

impl Conn {
    pub fn new(stream: TcpStream, request_id: String) -> io::Result<Self> {
        let writer = stream.try_clone()?;
        Ok(Self {
            reader: BufReader::new(stream),
            writer,
            request_id,
            sent: false,
        })
    }

    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.reader.get_ref().set_read_timeout(timeout)?;
        self.writer.set_read_timeout(timeout)
    }

    pub fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.writer.set_write_timeout(timeout)
    }

    pub fn sent(&self) -> bool {
        self.sent
    }

    /// Parse the request line and headers. `Ok(None)` means the peer closed an idle connection.
    pub fn read_request(&mut self) -> io::Result<Option<Request>> {
        let mut line = Vec::new();
        if self.reader.read_until(b'\n', &mut line)? == 0 {
            return Ok(None);
        }
        if line.len() > MAX_REQUEST_LINE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "request line too long",
            ));
        }
        let request_line = String::from_utf8_lossy(&line)
            .trim_end_matches(['\r', '\n'])
            .to_string();
        let mut fields = request_line.split_whitespace();
        let (Some(method), Some(target)) = (fields.next(), fields.next()) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "malformed request line",
            ));
        };
        let (path, query) = split_target(target);
        let mut headers = Vec::new();
        let mut total = line.len();
        loop {
            let mut raw = Vec::new();
            let read = self.reader.read_until(b'\n', &mut raw)?;
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "truncated request headers",
                ));
            }
            total += read;
            if total > MAX_HEADER_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "request headers too large",
                ));
            }
            let text = String::from_utf8_lossy(&raw);
            let header = text.trim_end_matches(['\r', '\n']);
            if header.is_empty() {
                break;
            }
            let Some((name, value)) = header.split_once(':') else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "malformed request header",
                ));
            };
            headers.push((name.trim().to_ascii_lowercase(), value.trim().to_string()));
        }
        Ok(Some(Request {
            method: method.to_ascii_uppercase(),
            path,
            query,
            headers,
        }))
    }

    /// Read and parse a JSON object body, applying the same limits as the Python service.
    pub fn read_json(&mut self, request: &Request, max_body: usize) -> Result<Value> {
        if request.header("transfer-encoding").is_some() {
            return Err(ApiError::bad(
                "chunked request bodies are not supported; send Content-Length",
            ));
        }
        let lengths: Vec<&str> = request.header_values("content-length").collect();
        if lengths.len() != 1 || !lengths[0].bytes().all(|b| b.is_ascii_digit()) {
            return Err(ApiError::new(
                411,
                "a single valid Content-Length is required",
                "invalid_request",
            ));
        }
        let length: usize = lengths[0].parse().map_err(|_| {
            ApiError::new(
                411,
                "a single valid Content-Length is required",
                "invalid_request",
            )
        })?;
        if length > max_body {
            return Err(ApiError::new(
                413,
                "request body is too large",
                "invalid_request",
            ));
        }
        if request.content_type() != "application/json" {
            return Err(ApiError::new(
                415,
                "Content-Type must be application/json",
                "invalid_request",
            ));
        }
        let mut body = Vec::with_capacity(length.min(64 * 1024));
        let mut taken = self.reader.by_ref().take(length as u64);
        match taken.read_to_end(&mut body) {
            Ok(read) if read == length => {}
            Ok(_) => return Err(ApiError::bad("incomplete request body")),
            Err(err) if is_timeout(&err) => {
                return Err(ApiError::new(408, "request body timeout", "timeout"));
            }
            Err(_) => return Err(ApiError::bad("incomplete request body")),
        }
        let value: Value =
            serde_json::from_slice(&body).map_err(|_| ApiError::bad("invalid JSON"))?;
        match value {
            Value::Object(_) => Ok(value),
            _ => Err(ApiError::bad("request body must be a JSON object")),
        }
    }

    /// Write a complete response with a known body length.
    pub fn send(
        &mut self,
        status: u16,
        body: &[u8],
        content_type: &str,
        cache: &str,
        extra: &[(String, String)],
    ) -> io::Result<()> {
        if self.sent {
            return Ok(());
        }
        let mut buffer = Vec::with_capacity(320 + body.len());
        head(
            &mut buffer,
            &self.request_id,
            status,
            content_type,
            cache,
            Some(body.len()),
            extra,
        );
        buffer.extend_from_slice(body);
        self.writer.write_all(&buffer)?;
        self.writer.flush()?;
        self.sent = true;
        Ok(())
    }

    pub fn send_json(&mut self, status: u16, value: &Value) -> io::Result<()> {
        let body = serde_json::to_vec(value).unwrap_or_else(|_| {
            br#"{"error":{"type":"internal_error","message":"internal server error"}}"#.to_vec()
        });
        self.send(
            status,
            &body,
            "application/json; charset=utf-8",
            "no-store",
            &[],
        )
    }

    /// Status without a body, e.g. `204 No Content`.
    pub fn send_empty(&mut self, status: u16) -> io::Result<()> {
        if self.sent {
            return Ok(());
        }
        let mut buffer = Vec::with_capacity(320);
        head(
            &mut buffer,
            &self.request_id,
            status,
            "",
            "no-store",
            None,
            &[],
        );
        self.writer.write_all(&buffer)?;
        self.writer.flush()?;
        self.sent = true;
        Ok(())
    }

    pub fn send_error(&mut self, error: &ApiError) -> io::Result<()> {
        let body = json!({ "error": { "type": error.kind, "message": error.message, "request_id": self.request_id } });
        self.send_json(error.status, &body)
    }

    /// Report an error unless a response has already started.
    pub fn fail(&mut self, error: &ApiError) {
        if !self.sent {
            let _ = self.send_error(error);
        }
    }

    /// Write headers only; the body is streamed with `write_chunk`.
    pub fn start_response(
        &mut self,
        status: u16,
        content_type: &str,
        cache: &str,
        content_length: Option<usize>,
        extra: &[(String, String)],
    ) -> io::Result<()> {
        if self.sent {
            return Ok(());
        }
        let mut buffer = Vec::with_capacity(320);
        head(
            &mut buffer,
            &self.request_id,
            status,
            content_type,
            cache,
            content_length,
            extra,
        );
        self.writer.write_all(&buffer)?;
        self.writer.flush()?;
        self.sent = true;
        Ok(())
    }

    pub fn write_chunk(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.writer.write_all(bytes)?;
        self.writer.flush()
    }

    pub fn close(&mut self) {
        let _ = self.writer.flush();
        let _ = self.writer.shutdown(std::net::Shutdown::Both);
    }
}

fn head(
    buffer: &mut Vec<u8>,
    request_id: &str,
    status: u16,
    content_type: &str,
    cache: &str,
    content_length: Option<usize>,
    extra: &[(String, String)],
) {
    push_line(buffer, &format!("HTTP/1.1 {status} {}", reason(status)));
    push_line(buffer, "Server: LLMProxy/1.0");
    push_line(buffer, &format!("Date: {}", http_date()));
    push_line(buffer, &format!("X-Request-ID: {request_id}"));
    push_line(buffer, "X-Content-Type-Options: nosniff");
    push_line(buffer, &format!("Cache-Control: {cache}"));
    for (name, value) in extra {
        push_line(buffer, &format!("{name}: {value}"));
    }
    if !content_type.is_empty() {
        push_line(buffer, &format!("Content-Type: {content_type}"));
    }
    if let Some(length) = content_length {
        push_line(buffer, &format!("Content-Length: {length}"));
    }
    push_line(buffer, "Connection: close");
    buffer.extend_from_slice(b"\r\n");
}

fn push_line(buffer: &mut Vec<u8>, line: &str) {
    buffer.extend_from_slice(line.as_bytes());
    buffer.extend_from_slice(b"\r\n");
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        409 => "Conflict",
        411 => "Length Required",
        413 => "Content Too Large",
        415 => "Unsupported Media Type",
        429 => "Too Many Requests",
        499 => "Client Closed Request",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "Unknown",
    }
}

pub fn is_timeout(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
    )
}

pub fn is_disconnect(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::NotConnected
    )
}

/// Constant time comparison of the supplied bearer token.
pub fn authorized(request: &Request, expected: &str) -> bool {
    let supplied = request.header("authorization").unwrap_or("");
    let expected = format!("Bearer {expected}");
    supplied.as_bytes().ct_eq(expected.as_bytes()).into()
}

fn split_target(target: &str) -> (String, String) {
    let without_fragment = target.split('#').next().unwrap_or(target);
    match without_fragment.split_once('?') {
        Some((path, query)) => (path.to_string(), query.to_string()),
        None => (without_fragment.to_string(), String::new()),
    }
}

/// Decode `application/x-www-form-urlencoded` style escapes used in query strings.
pub fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'%' if index + 2 < bytes.len() => {
                let hex = &value[index + 1..index + 3];
                match u8::from_str_radix(hex, 16) {
                    Ok(byte) => {
                        out.push(byte);
                        index += 3;
                    }
                    Err(_) => {
                        out.push(b'%');
                        index += 1;
                    }
                }
            }
            b'+' => {
                out.push(b' ');
                index += 1;
            }
            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).to_string()
}
