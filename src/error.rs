//! Error type shared by every handler; it maps onto an OpenAI style JSON error body.
use std::fmt;

/// A failure that can be reported to the caller as `{"error":{"type","message","request_id"}}`.
#[derive(Debug)]
pub struct ApiError {
    pub status: u16,
    pub message: String,
    pub kind: &'static str,
}

impl ApiError {
    pub fn new<S: Into<String>>(status: u16, message: S, kind: &'static str) -> Self {
        Self {
            status,
            message: message.into(),
            kind,
        }
    }

    pub fn bad<S: Into<String>>(message: S) -> Self {
        Self::new(400, message, "invalid_request")
    }

    pub fn unauthorized<S: Into<String>>(message: S) -> Self {
        Self::new(401, message, "unauthorized")
    }

    pub fn not_found<S: Into<String>>(message: S) -> Self {
        Self::new(404, message, "not_found")
    }

    pub fn conflict<S: Into<String>>(message: S) -> Self {
        Self::new(409, message, "conflict")
    }

    pub fn internal<S: Into<String>>(message: S) -> Self {
        Self::new(500, message, "internal_error")
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ApiError {}

/// Internal failures are reported without detail: URLs and credentials can be embedded in them.
impl From<rusqlite::Error> for ApiError {
    fn from(_: rusqlite::Error) -> Self {
        ApiError::internal("internal server error")
    }
}

impl From<std::io::Error> for ApiError {
    fn from(_: std::io::Error) -> Self {
        ApiError::internal("internal server error")
    }
}

impl From<serde_json::Error> for ApiError {
    fn from(_: serde_json::Error) -> Self {
        ApiError::internal("internal server error")
    }
}

pub type Result<T> = std::result::Result<T, ApiError>;
