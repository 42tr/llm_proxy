//! Small helpers shared by the store, the call logger and the HTTP layer.
use std::fs::{self, File, OpenOptions, Permissions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use chrono::Utc;

/// RFC 3339 UTC timestamp with microseconds, byte compatible with the Python service.
pub fn now() -> String {
    Utc::now().format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string()
}

/// HTTP `Date` header value.
pub fn http_date() -> String {
    Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string()
}

pub fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

pub fn random_bytes(length: usize) -> Vec<u8> {
    let mut buffer = vec![0u8; length];
    getrandom::getrandom(&mut buffer).expect("the platform CSPRNG must be available");
    buffer
}

/// `provider_9f2c…` style identifier, 32 lowercase hex characters after the prefix.
pub fn uid(prefix: &str) -> String {
    let mut id = String::with_capacity(prefix.len() + 33);
    id.push_str(prefix);
    id.push('_');
    for byte in random_bytes(16) {
        id.push_str(&format!("{byte:02x}"));
    }
    id
}

/// Equivalent of Python `secrets.token_urlsafe(32)`.
pub fn token_urlsafe() -> String {
    URL_SAFE_NO_PAD.encode(random_bytes(32))
}

pub fn set_mode(path: &Path, mode: u32) -> io::Result<()> {
    fs::set_permissions(path, Permissions::from_mode(mode))
}

/// Create `path` with mode 0600 holding `initial`, or return the trimmed existing content.
pub fn private_file(path: &Path, initial: &[u8]) -> io::Result<Vec<u8>> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
            set_mode(parent, 0o700)?;
        }
    }
    match OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
    {
        Ok(mut file) => {
            file.write_all(initial)?;
            Ok(initial.to_vec())
        }
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
            let mut buffer = Vec::new();
            File::open(path)?.read_to_end(&mut buffer)?;
            Ok(trim_ascii(&buffer))
        }
        Err(err) => Err(err),
    }
}

/// Read a secret token file, creating it with a fresh random token when missing.
pub fn token_file(path: &Path) -> io::Result<String> {
    let generated = token_urlsafe();
    let bytes = private_file(path, generated.as_bytes())?;
    Ok(String::from_utf8_lossy(&bytes).trim().to_string())
}

pub fn trim_ascii(bytes: &[u8]) -> Vec<u8> {
    let start = bytes
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    let end = bytes
        .iter()
        .rposition(|b| !b.is_ascii_whitespace())
        .map_or(start, |i| i + 1);
    bytes[start..end].to_vec()
}
