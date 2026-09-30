//! Browser management API: upstream providers, model routes, call logs and the client key.
use serde_json::{json, Value};

use crate::error::{ApiError, Result};
use crate::http::{Conn, Request};
use crate::proxy::{self, Supplied};
use crate::store::{text_field, ResolvedRoute};
use crate::util::now;
use crate::App;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Resource {
    Providers,
    Routes,
}

impl Resource {
    fn parse(segment: &str) -> Option<Self> {
        match segment {
            "providers" => Some(Self::Providers),
            "model-routes" => Some(Self::Routes),
            _ => None,
        }
    }
}

pub fn dispatch(
    app: &App,
    method: &str,
    parts: &[&str],
    request: &Request,
    conn: &mut Conn,
) -> Result<()> {
    match (method, parts) {
        ("GET", ["access"]) => {
            return respond(conn, 200, &json!({ "client_api_key": app.proxy_key }));
        }
        ("GET", ["logs"]) => return logs(app, request, conn),
        ("GET", ["logs", request_id]) => {
            let record = app.logger.read_record(&log_day(request), request_id)?;
            return respond(conn, 200, &record);
        }
        _ => {}
    }
    let Some((resource, rest)) = parts.split_first() else {
        return Err(ApiError::not_found("not found"));
    };
    let Some(resource) = Resource::parse(resource).filter(|_| rest.len() <= 2) else {
        return Err(ApiError::not_found("not found"));
    };
    if let [id, action] = rest {
        if resource == Resource::Providers && *action == "test" && method == "POST" {
            return test_provider(app, id, request, conn);
        }
        return Err(ApiError::not_found("not found"));
    }
    let item_id = rest.first().copied();
    match (method, item_id) {
        ("GET", None) => {
            let items = match resource {
                Resource::Providers => serde_json::to_value(app.store.providers()?)?,
                Resource::Routes => serde_json::to_value(app.store.routes()?)?,
            };
            respond(conn, 200, &json!({ "items": items }))
        }
        ("GET", Some(id)) => {
            let item = match resource {
                Resource::Providers => serde_json::to_value(app.store.provider(id, false)?)?,
                Resource::Routes => serde_json::to_value(app.store.route(id)?)?,
            };
            respond(conn, 200, &item)
        }
        ("POST", None) | ("PUT", Some(_)) => save(app, resource, item_id, request, conn),
        ("DELETE", Some(id)) => {
            match resource {
                Resource::Providers => app.store.delete_provider(id)?,
                Resource::Routes => app.store.delete_route(id)?,
            }
            let _ = conn.send_empty(204);
            Ok(())
        }
        _ => Err(ApiError::new(
            405,
            "method not allowed",
            "method_not_allowed",
        )),
    }
}

fn save(
    app: &App,
    resource: Resource,
    item_id: Option<&str>,
    request: &Request,
    conn: &mut Conn,
) -> Result<()> {
    let map = body(request, conn, app)?;
    let saved = match resource {
        Resource::Providers => serde_json::to_value(app.store.save_provider(&map, item_id)?)?,
        Resource::Routes => serde_json::to_value(app.store.save_route(&map, item_id)?)?,
    };
    respond(conn, if item_id.is_some() { 200 } else { 201 }, &saved)
}

/// Run one real upstream call so the browser can validate a provider.
fn test_provider(app: &App, id: &str, request: &Request, conn: &mut Conn) -> Result<()> {
    let provider = app.store.provider(id, true)?;
    if !provider.enabled {
        return Err(ApiError::conflict("provider is disabled"));
    }
    let map = body(request, conn, app)?;
    let model = text_field(&map, "model")?;
    let payload = json!({
        "model": model,
        "messages": [{ "role": "user", "content": "Reply with OK." }],
        "stream": false,
        "max_tokens": 8,
    });
    let route = ResolvedRoute {
        provider,
        upstream_model: model,
    };
    proxy::chat(app, request, conn, Some(Supplied { payload, route }));
    Ok(())
}

/// The `date` query parameter, defaulting to today (UTC).
fn log_day(request: &Request) -> String {
    request
        .query_param("date")
        .unwrap_or_else(|| now()[..10].to_string())
}

fn logs(app: &App, request: &Request, conn: &mut Conn) -> Result<()> {
    let term = request.query_param("q").unwrap_or_default();
    let limit = match request.query_param("limit") {
        None => 100,
        Some(text) => match text.trim().parse::<i64>() {
            Ok(value) if (1..=500).contains(&value) => value as usize,
            Ok(_) => 0,
            Err(_) => return Err(ApiError::bad("limit must be an integer")),
        },
    };
    let result = app.logger.read_day(&log_day(request), limit, &term)?;
    respond(conn, 200, &result)
}

fn body(request: &Request, conn: &mut Conn, app: &App) -> Result<serde_json::Map<String, Value>> {
    match conn.read_json(request, app.max_body_bytes, app.body_timeout)? {
        Value::Object(map) => Ok(map),
        _ => Err(ApiError::bad("request body must be a JSON object")),
    }
}

/// A failed write means the peer is gone; there is nothing left to report.
fn respond(conn: &mut Conn, status: u16, value: &Value) -> Result<()> {
    let _ = conn.send_json(status, value);
    Ok(())
}
