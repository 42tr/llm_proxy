//! Browser management API: upstream providers, model routes, call logs and the client key.
use serde_json::{json, Value};

use crate::error::{ApiError, Result};
use crate::http::{Conn, Request};
use crate::proxy::{self, Supplied};
use crate::store::{text_field, ResolvedRoute};
use crate::util::now;
use crate::App;

pub fn dispatch(
    app: &App,
    method: &str,
    parts: &[&str],
    request: &Request,
    conn: &mut Conn,
) -> Result<()> {
    if parts == ["access"] && method == "GET" {
        return respond(conn, 200, &json!({ "client_api_key": app.proxy_key }));
    }
    if parts == ["logs"] && method == "GET" {
        return logs(app, request, conn);
    }
    let Some((resource, rest)) = parts.split_first() else {
        return Err(ApiError::not_found("not found"));
    };
    if !matches!(*resource, "providers" | "model-routes") || rest.len() > 2 {
        return Err(ApiError::not_found("not found"));
    }
    if rest.len() == 2 {
        if *resource == "providers" && rest[1] == "test" && method == "POST" {
            return test_provider(app, rest[0], request, conn);
        }
        return Err(ApiError::not_found("not found"));
    }
    let item_id = rest.first().copied();
    match (method, item_id) {
        ("GET", None) => {
            let items = if *resource == "providers" {
                serde_json::to_value(app.store.providers()?)?
            } else {
                serde_json::to_value(app.store.routes()?)?
            };
            respond(conn, 200, &json!({ "items": items }))
        }
        ("GET", Some(id)) => {
            let item = if *resource == "providers" {
                serde_json::to_value(app.store.provider(id, false)?)?
            } else {
                serde_json::to_value(app.store.route(id)?)?
            };
            respond(conn, 200, &item)
        }
        ("POST", None) | ("PUT", Some(_)) => save(app, resource, item_id, request, conn),
        ("DELETE", Some(id)) => {
            app.store.delete(resource, id)?;
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
    resource: &str,
    item_id: Option<&str>,
    request: &Request,
    conn: &mut Conn,
) -> Result<()> {
    let map = body(request, conn, app)?;
    let saved = if resource == "providers" {
        serde_json::to_value(app.store.save_provider(&map, item_id)?)?
    } else {
        serde_json::to_value(app.store.save_route(&map, item_id)?)?
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

fn logs(app: &App, request: &Request, conn: &mut Conn) -> Result<()> {
    let day = request
        .query_param("date")
        .unwrap_or_else(|| now()[..10].to_string());
    let term = request.query_param("q").unwrap_or_default();
    let limit = match request.query_param("limit") {
        None => 100,
        Some(text) => match text.trim().parse::<i64>() {
            Ok(value) if (1..=500).contains(&value) => value as usize,
            Ok(_) => 0,
            Err(_) => return Err(ApiError::bad("limit must be an integer")),
        },
    };
    let result = app.logger.read_day(&day, limit, &term)?;
    respond(conn, 200, &result)
}

fn body(request: &Request, conn: &mut Conn, app: &App) -> Result<serde_json::Map<String, Value>> {
    match conn.read_json(request, app.max_body_bytes)? {
        Value::Object(map) => Ok(map),
        _ => Err(ApiError::bad("request body must be a JSON object")),
    }
}

/// A failed write means the peer is gone; there is nothing left to report.
fn respond(conn: &mut Conn, status: u16, value: &Value) -> Result<()> {
    let _ = conn.send_json(status, value);
    Ok(())
}
