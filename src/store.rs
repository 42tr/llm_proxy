//! SQLite backed configuration; upstream credentials are stored Fernet encrypted.
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use rusqlite::{params, Connection, ErrorCode, OptionalExtension, Row, ToSql};
use serde::Serialize;
use serde_json::{Map, Value};
use url::Url;

use crate::error::{ApiError, Result};
use crate::fernet::Fernet;
use crate::util::{now, private_file, set_mode, uid};

pub type Headers = Map<String, Value>;

const SCHEMA: &str = "PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS providers (
    id TEXT PRIMARY KEY, name TEXT NOT NULL, endpoint_url TEXT NOT NULL,
    auth_type TEXT NOT NULL, auth_header_name TEXT NOT NULL,
    secret TEXT NOT NULL, headers TEXT NOT NULL, timeout_ms INTEGER NOT NULL,
    enabled INTEGER NOT NULL, log_request_body INTEGER NOT NULL,
    log_response_body INTEGER NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS model_routes (
    id TEXT PRIMARY KEY, public_model TEXT NOT NULL UNIQUE,
    provider_id TEXT NOT NULL REFERENCES providers(id) ON DELETE RESTRICT,
    upstream_model TEXT NOT NULL, enabled INTEGER NOT NULL,
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);";

const BLOCKED_HEADERS: [&str; 14] = [
    "host",
    "content-length",
    "transfer-encoding",
    "connection",
    "keep-alive",
    "upgrade",
    "te",
    "trailer",
    "proxy-authorization",
    "proxy-authenticate",
    "authorization",
    "cookie",
    "content-type",
    "accept-encoding",
];

/// A configured upstream. `api_key` is internal only and never serialized.
#[derive(Debug, Clone, Serialize)]
pub struct Provider {
    pub id: String,
    pub name: String,
    pub endpoint_url: String,
    pub auth_type: String,
    pub auth_header_name: String,
    pub timeout_ms: i64,
    pub enabled: bool,
    pub log_request_body: bool,
    pub log_response_body: bool,
    pub created_at: String,
    pub updated_at: String,
    pub has_api_key: bool,
    pub extra_headers: Headers,
    #[serde(skip)]
    pub api_key: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Route {
    pub id: String,
    pub public_model: String,
    pub provider_id: String,
    pub upstream_model: String,
    pub enabled: bool,
    pub created_at: String,
    pub updated_at: String,
    pub provider_name: String,
    pub provider_enabled: bool,
}

/// An immutable snapshot used by one in-flight call.
#[derive(Debug, Clone)]
pub struct ResolvedRoute {
    pub provider: Provider,
    pub upstream_model: String,
}

impl ResolvedRoute {
    pub fn provider_id(&self) -> &str {
        &self.provider.id
    }

    pub fn api_key(&self) -> &str {
        &self.provider.api_key
    }

    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.provider.timeout_ms.max(0) as u64)
    }

    pub fn extra_headers(&self) -> impl Iterator<Item = (&str, &str)> {
        self.provider
            .extra_headers
            .iter()
            .filter_map(|(k, v)| v.as_str().map(|value| (k.as_str(), value)))
    }
}

struct RawProvider {
    id: String,
    name: String,
    endpoint_url: String,
    auth_type: String,
    auth_header_name: String,
    secret: String,
    headers: String,
    timeout_ms: i64,
    enabled: bool,
    log_request_body: bool,
    log_response_body: bool,
    created_at: String,
    updated_at: String,
}

impl RawProvider {
    fn from_row(row: &Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            id: row.get("id")?,
            name: row.get("name")?,
            endpoint_url: row.get("endpoint_url")?,
            auth_type: row.get("auth_type")?,
            auth_header_name: row.get("auth_header_name")?,
            secret: row.get("secret")?,
            headers: row.get("headers")?,
            timeout_ms: row.get("timeout_ms")?,
            enabled: row.get("enabled")?,
            log_request_body: row.get("log_request_body")?,
            log_response_body: row.get("log_response_body")?,
            created_at: row.get("created_at")?,
            updated_at: row.get("updated_at")?,
        })
    }

    fn decrypt(self, cipher: &Fernet, secret: bool) -> Result<Provider> {
        let api_key = cipher.decrypt_text(&self.secret)?;
        let stored: Headers = serde_json::from_slice(&cipher.decrypt(&self.headers)?)
            .ok()
            .filter(|value: &Value| value.is_object())
            .and_then(|value| match value {
                Value::Object(map) => Some(map),
                _ => None,
            })
            .unwrap_or_default();
        let extra_headers = if secret {
            stored
        } else {
            stored
                .into_iter()
                .map(|(name, value)| {
                    let masked = masked_header(&name, &value);
                    (name, masked)
                })
                .collect()
        };
        Ok(Provider {
            has_api_key: !api_key.is_empty(),
            extra_headers,
            api_key: if secret { api_key } else { String::new() },
            id: self.id,
            name: self.name,
            endpoint_url: self.endpoint_url,
            auth_type: self.auth_type,
            auth_header_name: self.auth_header_name,
            timeout_ms: self.timeout_ms,
            enabled: self.enabled,
            log_request_body: self.log_request_body,
            log_response_body: self.log_response_body,
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
    }
}

pub struct ConfigStore {
    conn: Mutex<Connection>,
    cipher: Fernet,
}

impl ConfigStore {
    pub fn open(db_path: &Path, master_key: Option<&str>) -> Result<Self> {
        if let Some(parent) = db_path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
                set_mode(parent, 0o700)?;
            }
        }
        let cipher = match master_key.map(str::trim).filter(|key| !key.is_empty()) {
            Some(secret) => Fernet::from_secret(secret),
            None => {
                let mut key_path = PathBuf::from(db_path);
                key_path.as_mut_os_string().push(".key");
                let generated = Fernet::generate_key();
                let stored = private_file(&key_path, generated.as_bytes())?;
                Fernet::from_secret(&String::from_utf8_lossy(&stored))
            }
        };
        let conn = Connection::open(db_path)?;
        set_mode(db_path, 0o600)?;
        conn.execute_batch(SCHEMA)?;
        // Fail early if a supplied encryption key cannot decrypt existing configuration.
        for token in Self::all_secrets(&conn)? {
            cipher.decrypt(&token)?;
        }
        Ok(Self {
            conn: Mutex::new(conn),
            cipher,
        })
    }

    fn lock(&self) -> MutexGuard<'_, Connection> {
        self.conn
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn all_secrets(conn: &Connection) -> Result<Vec<String>> {
        let mut statement = conn.prepare("SELECT secret FROM providers")?;
        let tokens = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(tokens)
    }

    fn query(conn: &Connection, sql: &str, args: &[&dyn ToSql]) -> Result<Vec<RawProvider>> {
        let mut statement = conn.prepare(sql)?;
        let rows = statement
            .query_map(args, RawProvider::from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    fn provider_with(
        conn: &Connection,
        cipher: &Fernet,
        id: &str,
        secret: bool,
    ) -> Result<Provider> {
        let rows = Self::query(conn, "SELECT * FROM providers WHERE id=?1", params![id])?;
        match rows.into_iter().next() {
            Some(row) => row.decrypt(cipher, secret),
            None => Err(ApiError::not_found("provider not found")),
        }
    }

    pub fn providers(&self) -> Result<Vec<Provider>> {
        let conn = self.lock();
        let rows = Self::query(&conn, "SELECT * FROM providers ORDER BY name", params![])?;
        rows.into_iter()
            .map(|row| row.decrypt(&self.cipher, false))
            .collect()
    }

    pub fn provider(&self, id: &str, secret: bool) -> Result<Provider> {
        let conn = self.lock();
        Self::provider_with(&conn, &self.cipher, id, secret)
    }

    pub fn save_provider(
        &self,
        data: &Map<String, Value>,
        provider_id: Option<&str>,
    ) -> Result<Provider> {
        let mut conn = self.lock();
        let old = match provider_id {
            Some(id) => Some(Self::provider_with(&conn, &self.cipher, id, true)?),
            None => None,
        };
        let name = text_field(data, "name")?;
        let endpoint_url = endpoint(data.get("endpoint_url"))?;
        let auth_type = match data.get("auth_type") {
            None => "bearer".to_string(),
            Some(Value::String(value)) => value.clone(),
            Some(_) => String::new(),
        };
        if !matches!(
            auth_type.as_str(),
            "none" | "bearer" | "api_key" | "custom_header"
        ) {
            return Err(ApiError::bad("invalid auth_type"));
        }
        let auth_header_name = if auth_type == "custom_header" {
            header_name(data.get("auth_header_name"), true)?
        } else {
            String::new()
        };
        let mut api_key = match data.get("api_key") {
            Some(value) => header_value(value)?,
            None => old
                .as_ref()
                .map(|provider| provider.api_key.clone())
                .unwrap_or_default(),
        };
        if auth_type == "none" {
            api_key.clear();
        } else if api_key.is_empty() {
            return Err(ApiError::bad("api_key is required"));
        }
        let supplied = match data.get("extra_headers") {
            Some(value) => value.clone(),
            None => old
                .as_ref()
                .map(|provider| Value::Object(provider.extra_headers.clone()))
                .unwrap_or_else(|| Value::Object(Headers::new())),
        };
        let headers = match &supplied {
            Value::Object(map) if map.len() <= 32 => map.clone(),
            _ => {
                return Err(ApiError::bad(
                    "extra_headers must be an object with at most 32 headers",
                ))
            }
        };
        for (key, value) in &headers {
            header_name(Some(&Value::String(key.clone())), false)?;
            header_value(value)?;
            if key.eq_ignore_ascii_case("api-key") || key.eq_ignore_ascii_case(&auth_header_name) {
                return Err(ApiError::bad(
                    "use the authentication fields for authentication headers",
                ));
            }
        }
        if headers
            .keys()
            .map(|key| key.to_lowercase())
            .collect::<HashSet<_>>()
            .len()
            != headers.len()
        {
            return Err(ApiError::bad("duplicate header names"));
        }
        let timeout_ms = match data.get("timeout_ms") {
            None => 120_000,
            Some(Value::Number(number)) if number.is_i64() || number.is_u64() => {
                number.as_i64().unwrap_or(-1)
            }
            Some(_) => -1,
        };
        if !(100..=3_600_000).contains(&timeout_ms) {
            return Err(ApiError::bad(
                "timeout_ms must be an integer between 100 and 3600000",
            ));
        }
        let id = provider_id
            .map(str::to_string)
            .unwrap_or_else(|| uid("provider"));
        let created_at = old
            .as_ref()
            .map(|provider| provider.created_at.clone())
            .unwrap_or_else(now);
        let row = ProviderRow {
            id,
            name,
            endpoint_url,
            auth_type,
            auth_header_name,
            secret: self.cipher.encrypt(api_key.as_bytes()),
            headers: self
                .cipher
                .encrypt(serde_json::to_vec(&headers)?.as_slice()),
            timeout_ms,
            enabled: flag(data, "enabled", true)?,
            log_request_body: flag(data, "log_request_body", true)?,
            log_response_body: flag(data, "log_response_body", true)?,
            created_at,
            updated_at: now(),
        };
        let update = old.is_some();
        let transaction = conn.transaction()?;
        let written = if update {
            row.update(&transaction)
        } else {
            row.insert(&transaction)
        };
        match written {
            Ok(_) => transaction.commit()?,
            Err(err) if is_constraint(&err) => {
                return Err(ApiError::conflict("provider id is already in use"))
            }
            Err(err) => return Err(err.into()),
        }
        Self::provider_with(&conn, &self.cipher, &row.id, false)
    }

    fn routes_with(conn: &Connection) -> Result<Vec<Route>> {
        let mut statement = conn.prepare(
            "SELECT r.*, p.name provider_name, p.enabled provider_enabled
             FROM model_routes r JOIN providers p ON r.provider_id=p.id ORDER BY public_model",
        )?;
        let routes = statement
            .query_map([], |row| {
                Ok(Route {
                    id: row.get("id")?,
                    public_model: row.get("public_model")?,
                    provider_id: row.get("provider_id")?,
                    upstream_model: row.get("upstream_model")?,
                    enabled: row.get("enabled")?,
                    created_at: row.get("created_at")?,
                    updated_at: row.get("updated_at")?,
                    provider_name: row.get("provider_name")?,
                    provider_enabled: row.get("provider_enabled")?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(routes)
    }

    fn route_with(conn: &Connection, id: &str) -> Result<Route> {
        Self::routes_with(conn)?
            .into_iter()
            .find(|route| route.id == id)
            .ok_or_else(|| ApiError::not_found("model route not found"))
    }

    pub fn routes(&self) -> Result<Vec<Route>> {
        let conn = self.lock();
        Self::routes_with(&conn)
    }

    pub fn route(&self, id: &str) -> Result<Route> {
        let conn = self.lock();
        Self::route_with(&conn, id)
    }

    pub fn save_route(&self, data: &Map<String, Value>, route_id: Option<&str>) -> Result<Route> {
        let mut conn = self.lock();
        let old = match route_id {
            Some(id) => Some(Self::route_with(&conn, id)?),
            None => None,
        };
        let public_model = text_field(data, "public_model")?;
        let upstream_model = text_field(data, "upstream_model")?;
        let provider_id = text_field(data, "provider_id")?;
        Self::provider_with(&conn, &self.cipher, &provider_id, false)?;
        let id = route_id.map(str::to_string).unwrap_or_else(|| uid("route"));
        let created_at = old
            .as_ref()
            .map(|route| route.created_at.clone())
            .unwrap_or_else(now);
        let values = params![
            id,
            public_model,
            provider_id,
            upstream_model,
            flag(data, "enabled", true)?,
            created_at,
            now()
        ];
        let sql = if old.is_some() {
            "UPDATE model_routes SET public_model=?2, provider_id=?3, upstream_model=?4,
             enabled=?5, updated_at=?7 WHERE id=?1"
        } else {
            "INSERT INTO model_routes (id, public_model, provider_id, upstream_model, enabled, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"
        };
        let transaction = conn.transaction()?;
        match transaction.execute(sql, values) {
            Ok(_) => transaction.commit()?,
            Err(err) if is_constraint(&err) => {
                return Err(ApiError::conflict("public_model is already mapped"))
            }
            Err(err) => return Err(err.into()),
        }
        Self::route_with(&conn, &id)
    }

    /// Each call takes one immutable snapshot; in-flight calls keep their original config.
    pub fn resolve(&self, model: &str) -> Result<ResolvedRoute> {
        let conn = self.lock();
        let found = conn
            .query_row(
                "SELECT provider_id, enabled, upstream_model FROM model_routes WHERE public_model=?1",
                params![model],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, bool>(1)?, row.get::<_, String>(2)?)),
            )
            .optional()?;
        let (provider_id, upstream_model) = match found {
            Some((provider_id, true, upstream_model)) => (provider_id, upstream_model),
            _ => {
                return Err(ApiError::new(
                    404,
                    "model is not configured or is disabled",
                    "model_not_found",
                ))
            }
        };
        let provider = Self::provider_with(&conn, &self.cipher, &provider_id, true)?;
        if !provider.enabled {
            return Err(ApiError::new(
                404,
                "model provider is disabled",
                "model_not_found",
            ));
        }
        Ok(ResolvedRoute {
            provider,
            upstream_model,
        })
    }

    pub fn delete(&self, resource: &str, id: &str) -> Result<()> {
        let mut conn = self.lock();
        let transaction = conn.transaction()?;
        if resource == "providers" {
            Self::provider_with(&transaction, &self.cipher, id, false)?;
        } else {
            Self::route_with(&transaction, id)?;
        }
        let sql = if resource == "providers" {
            "DELETE FROM providers WHERE id=?1"
        } else {
            "DELETE FROM model_routes WHERE id=?1"
        };
        match transaction.execute(sql, params![id]) {
            Ok(_) => transaction.commit()?,
            Err(err) if is_constraint(&err) => {
                return Err(ApiError::conflict(
                    "delete or reassign this provider's model routes first",
                ))
            }
            Err(err) => return Err(err.into()),
        }
        Ok(())
    }
}

struct ProviderRow {
    id: String,
    name: String,
    endpoint_url: String,
    auth_type: String,
    auth_header_name: String,
    secret: String,
    headers: String,
    timeout_ms: i64,
    enabled: bool,
    log_request_body: bool,
    log_response_body: bool,
    created_at: String,
    updated_at: String,
}

impl ProviderRow {
    fn insert(&self, conn: &Connection) -> rusqlite::Result<usize> {
        conn.execute(
            "INSERT INTO providers (id, name, endpoint_url, auth_type, auth_header_name, secret, headers,
             timeout_ms, enabled, log_request_body, log_response_body, created_at, updated_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
            params![
                self.id,
                self.name,
                self.endpoint_url,
                self.auth_type,
                self.auth_header_name,
                self.secret,
                self.headers,
                self.timeout_ms,
                self.enabled,
                self.log_request_body,
                self.log_response_body,
                self.created_at,
                self.updated_at
            ],
        )
    }

    fn update(&self, conn: &Connection) -> rusqlite::Result<usize> {
        conn.execute(
            "UPDATE providers SET name=?2, endpoint_url=?3, auth_type=?4, auth_header_name=?5, secret=?6,
             headers=?7, timeout_ms=?8, enabled=?9, log_request_body=?10, log_response_body=?11,
             created_at=?12, updated_at=?13 WHERE id=?1",
            params![
                self.id,
                self.name,
                self.endpoint_url,
                self.auth_type,
                self.auth_header_name,
                self.secret,
                self.headers,
                self.timeout_ms,
                self.enabled,
                self.log_request_body,
                self.log_response_body,
                self.created_at,
                self.updated_at
            ],
        )
    }
}

fn is_constraint(err: &rusqlite::Error) -> bool {
    matches!(err, rusqlite::Error::SqliteFailure(failure, _) if failure.code == ErrorCode::ConstraintViolation)
}

pub fn text_field(data: &Map<String, Value>, name: &str) -> Result<String> {
    text_value(data.get(name), name)
}

pub fn text_value(value: Option<&Value>, name: &str) -> Result<String> {
    let invalid = ApiError::bad(format!(
        "{name} must be a non-empty string (max 8192 characters)"
    ));
    match value {
        Some(Value::String(text)) if !text.trim().is_empty() && text.chars().count() <= 8192 => {
            Ok(text.trim().to_string())
        }
        _ => Err(invalid),
    }
}

pub fn flag(data: &Map<String, Value>, name: &str, default: bool) -> Result<bool> {
    match data.get(name) {
        Some(Value::Bool(value)) => Ok(*value),
        Some(_) => Err(ApiError::bad(format!("{name} must be a boolean"))),
        None => Ok(default),
    }
}

pub fn header_name(value: Option<&Value>, auth: bool) -> Result<String> {
    let name = match value {
        Some(Value::String(text)) => text.clone(),
        _ => return Err(ApiError::bad("invalid header name")),
    };
    if name.is_empty() || !name.chars().all(is_token_char) {
        return Err(ApiError::bad("invalid header name"));
    }
    let lower = name.to_lowercase();
    if BLOCKED_HEADERS.contains(&lower.as_str()) && !(auth && lower == "authorization") {
        return Err(ApiError::bad(format!("header {name} is reserved")));
    }
    Ok(name)
}

fn is_token_char(c: char) -> bool {
    c.is_ascii_alphanumeric()
        || matches!(
            c,
            '!' | '#'
                | '$'
                | '%'
                | '&'
                | '\''
                | '*'
                | '+'
                | '.'
                | '^'
                | '_'
                | '`'
                | '|'
                | '~'
                | '-'
        )
}

pub fn header_value(value: &Value) -> Result<String> {
    let invalid = || ApiError::bad("header values must contain printable Latin-1 characters");
    match value {
        Value::String(text)
            if !text
                .chars()
                .any(|c| (c as u32) < 32 || (c as u32) > 255 || c as u32 == 127) =>
        {
            Ok(text.clone())
        }
        _ => Err(invalid()),
    }
}

pub fn masked_header(name: &str, value: &Value) -> Value {
    let lower = name.to_lowercase();
    let secret = [
        "authorization",
        "api-key",
        "api_key",
        "apikey",
        "token",
        "secret",
        "password",
    ]
    .iter()
    .any(|needle| lower.contains(needle));
    if secret {
        Value::String("[saved]".to_string())
    } else {
        value.clone()
    }
}

pub fn endpoint(value: Option<&Value>) -> Result<String> {
    let raw = text_value(value, "endpoint_url")?;
    let valid = Url::parse(&raw).is_ok_and(|url| {
        matches!(url.scheme(), "http" | "https")
            && url.has_host()
            && url.port() != Some(0)
            && url.username().is_empty()
            && url.password().is_none()
            && url.fragment().is_none_or(str::is_empty)
    });
    if !valid {
        return Err(ApiError::bad(
            "endpoint_url must be an http(s) URL without credentials or fragment",
        ));
    }
    if raw.chars().any(|c| (c as u32) <= 32 || (c as u32) > 126) {
        return Err(ApiError::bad(
            "endpoint_url must be ASCII; encode non-ASCII URL characters",
        ));
    }
    Ok(raw)
}
