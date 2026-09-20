//! End-to-end tests: a fake upstream, the real proxy, and the admin API.
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use llm_proxy::store::ResolvedRoute;
use llm_proxy::{bind, serve, App};
use serde_json::{json, Map, Value};

static COUNTER: AtomicUsize = AtomicUsize::new(0);

struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        let unique = COUNTER.fetch_add(1, Ordering::SeqCst);
        let path =
            std::env::temp_dir().join(format!("llm-proxy-{tag}-{}-{unique}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).expect("a writable temporary directory");
        Self(path)
    }

    fn join(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// A stand-in OpenAI compatible upstream that echoes the model it was asked for.
fn fake_upstream() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a free port for the fake upstream");
    let port = listener.local_addr().expect("a local address").port();
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { break };
            thread::spawn(move || {
                let _ = stream.set_nodelay(true);
                upstream_response(stream);
            });
        }
    });
    port
}

fn upstream_response(mut stream: TcpStream) {
    let Ok(payload) = read_json_request(&mut stream) else {
        return;
    };
    let model = payload.get("model").and_then(Value::as_str).unwrap_or("");
    if model == "bad" {
        let body = br#"{"error":{"message":"bad upstream"}}"#;
        write_all(
            &mut stream,
            &format!(
                "HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            ),
            body,
        );
        return;
    }
    if payload
        .get("stream")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        let _ = stream.write_all(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n",
        );
        for chunk in [
            b"data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n".as_slice(),
            b"data: [DONE]\n\n",
        ] {
            if stream.write_all(chunk).is_err() || stream.flush().is_err() {
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
        return;
    }
    let body = json!({ "model": model, "choices": [] }).to_string();
    write_all(
        &mut stream,
        &format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        ),
        body.as_bytes(),
    );
}

fn write_all(stream: &mut TcpStream, head: &str, body: &[u8]) {
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(body);
    let _ = stream.flush();
}

fn read_json_request(stream: &mut TcpStream) -> std::io::Result<Value> {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut line = Vec::new();
    if reader.read_until(b'\n', &mut line)? == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "no request",
        ));
    }
    let mut length = 0usize;
    loop {
        let mut raw = Vec::new();
        if reader.read_until(b'\n', &mut raw)? == 0 {
            break;
        }
        let header = String::from_utf8_lossy(&raw).trim().to_lowercase();
        if header.is_empty() {
            break;
        }
        if let Some(value) = header.strip_prefix("content-length:") {
            length = value.trim().parse().unwrap_or(0);
        }
    }
    let mut body = vec![0u8; length];
    reader.read_exact(&mut body)?;
    Ok(serde_json::from_slice(&body).unwrap_or(Value::Null))
}

struct Harness {
    base: String,
    app: Arc<App>,
    upstream: u16,
    _temp: TempDir,
}

impl Harness {
    fn new(tag: &str) -> Self {
        let temp = TempDir::new(tag);
        let app = Arc::new(
            App::new(
                &temp.join("proxy.sqlite3"),
                &temp.join("logs"),
                Some("admin".into()),
                Some("client".into()),
            )
            .expect("the data directory must open"),
        );
        let upstream = fake_upstream();
        let listener = bind("127.0.0.1", 0).expect("a free port for the proxy");
        let port = listener.local_addr().expect("a local address").port();
        let serving = Arc::clone(&app);
        thread::spawn(move || serve(listener, serving));
        let base = format!("http://127.0.0.1:{port}");
        Self::wait_for(&base);
        Self {
            base,
            app,
            upstream,
            _temp: temp,
        }
    }

    fn wait_for(base: &str) {
        for _ in 0..100 {
            if get(&format!("{base}/healthz"), None).is_some() {
                return;
            }
            thread::sleep(Duration::from_millis(20));
        }
        panic!("the proxy did not start at {base}");
    }

    fn add_provider(&self, name: &str, body_logging: bool) -> String {
        let provider = json!({
            "name": name,
            "endpoint_url": format!("http://127.0.0.1:{}/v1/chat/completions", self.upstream),
            "auth_type": "none",
            "log_response_body": body_logging,
            "log_request_body": body_logging,
        });
        let saved = self
            .app
            .store
            .save_provider(provider.as_object().expect("an object"), None)
            .expect("a saved provider");
        saved.id
    }

    fn add_route(&self, public_model: &str, upstream_model: &str, provider_id: &str) {
        let route = json!({ "public_model": public_model, "upstream_model": upstream_model, "provider_id": provider_id });
        self.app
            .store
            .save_route(route.as_object().expect("an object"), None)
            .expect("a saved route");
    }

    /// A provider with one route, matching the Python fixture.
    fn with_demo(tag: &str) -> Self {
        let harness = Self::new(tag);
        let provider = harness.add_provider("fake", true);
        harness.add_route("demo", "up-demo", &provider);
        harness
    }

    fn chat(&self, payload: Value, auth: &str) -> Reply {
        post(
            &format!("{}/v1/chat/completions", self.base),
            &payload,
            Some(auth),
        )
    }

    fn admin(&self, method: &str, path: &str, body: Option<&Value>, auth: Option<&str>) -> Reply {
        request(method, &format!("{}{path}", self.base), body, auth)
    }

    fn admin_json(&self, path: &str) -> Value {
        let reply = self.admin("GET", path, None, Some("admin"));
        assert_eq!(reply.status, 200, "GET {path} failed: {}", reply.body);
        reply.json()
    }
}

struct Reply {
    status: u16,
    body: String,
    content_type: String,
    request_id: String,
}

impl Reply {
    fn json(&self) -> Value {
        serde_json::from_str(&self.body).unwrap_or(Value::Null)
    }

    fn error_kind(&self) -> String {
        self.json()
            .get("error")
            .and_then(|error| error.get("type"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string()
    }

    fn error_message(&self) -> String {
        self.json()
            .get("error")
            .and_then(|error| error.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string()
    }
}

fn client() -> ureq::Agent {
    ureq::Agent::config_builder()
        .http_status_as_error(false)
        .max_redirects(0)
        .timeout_global(Some(Duration::from_secs(10)))
        .build()
        .new_agent()
}

fn get(url: &str, auth: Option<&str>) -> Option<Reply> {
    let reply = request("GET", url, None, auth);
    (reply.status != 0).then_some(reply)
}

fn post(url: &str, body: &Value, auth: Option<&str>) -> Reply {
    request("POST", url, Some(body), auth)
}

fn request(method: &str, url: &str, body: Option<&Value>, auth: Option<&str>) -> Reply {
    let agent = client();
    let token = auth.map(|value| format!("Bearer {value}"));
    let result = match (method, body) {
        ("GET", _) => {
            let mut builder = agent.get(url);
            if let Some(value) = &token {
                builder = builder.header("Authorization", value);
            }
            builder.call()
        }
        ("DELETE", _) => {
            let mut builder = agent.delete(url);
            if let Some(value) = &token {
                builder = builder.header("Authorization", value);
            }
            builder.call()
        }
        ("POST", Some(value)) | ("PUT", Some(value)) => {
            let mut builder = if method == "POST" {
                agent.post(url)
            } else {
                agent.put(url)
            };
            builder = builder.header("Content-Type", "application/json");
            if let Some(header) = &token {
                builder = builder.header("Authorization", header);
            }
            builder.send(value.to_string().as_str())
        }
        _ => panic!("unsupported test request {method}"),
    };
    match result {
        Ok(response) => {
            let status = response.status().as_u16();
            let content_type = response
                .headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok())
                .unwrap_or("")
                .to_string();
            let request_id = response
                .headers()
                .get("x-request-id")
                .and_then(|value| value.to_str().ok())
                .unwrap_or("")
                .to_string();
            let mut text = String::new();
            response
                .into_body()
                .into_reader()
                .read_to_string(&mut text)
                .ok();
            Reply {
                status,
                body: text,
                content_type,
                request_id,
            }
        }
        // A transport level failure means the server never answered.
        Err(_) => Reply {
            status: 0,
            body: String::new(),
            content_type: String::new(),
            request_id: String::new(),
        },
    }
}

fn messages() -> Value {
    json!([{ "role": "user", "content": "hi" }])
}

#[test]
fn non_stream_rewrites_model_and_returns_response() {
    let harness = Harness::with_demo("non-stream");
    let reply = harness.chat(json!({ "model": "demo", "messages": messages() }), "client");
    assert_eq!(reply.status, 200, "{}", reply.body);
    assert_eq!(
        reply.json().get("model").and_then(Value::as_str),
        Some("up-demo")
    );
    assert!(
        reply.request_id.starts_with("req_"),
        "request id header: {}",
        reply.request_id
    );
}

#[test]
fn stream_is_forwarded() {
    let harness = Harness::with_demo("stream");
    let reply = harness.chat(
        json!({ "model": "demo", "messages": messages(), "stream": true }),
        "client",
    );
    assert_eq!(reply.status, 200);
    assert_eq!(
        reply.content_type.split(';').next(),
        Some("text/event-stream")
    );
    assert!(
        reply.body.contains("data: [DONE]"),
        "body was {reply_body}",
        reply_body = reply.body
    );
}

#[test]
fn unknown_model_and_auth_are_rejected() {
    let harness = Harness::with_demo("rejects");
    let unknown = harness.chat(
        json!({ "model": "missing", "messages": messages() }),
        "client",
    );
    assert_eq!(unknown.status, 404);
    assert_eq!(unknown.error_kind(), "model_not_found");
    assert!(
        !unknown.request_id.is_empty(),
        "error bodies carry a request id"
    );

    let unauthorized = harness.chat(json!({ "model": "demo", "messages": messages() }), "wrong");
    assert_eq!(unauthorized.status, 401);
    assert_eq!(unauthorized.error_kind(), "unauthorized");

    let malformed = harness.chat(json!({ "model": "demo" }), "client");
    assert_eq!(malformed.status, 400);
    assert_eq!(
        malformed.error_message(),
        "messages must be a non-empty array"
    );

    let bad_stream = harness.chat(
        json!({ "model": "demo", "messages": messages(), "stream": "yes" }),
        "client",
    );
    assert_eq!(bad_stream.status, 400);
    assert_eq!(bad_stream.error_message(), "stream must be a boolean");
}

#[test]
fn upstream_error_status_and_body_are_forwarded() {
    let harness = Harness::with_demo("upstream-error");
    let provider = harness.admin_json("/api/admin/providers")["items"][0]["id"]
        .as_str()
        .expect("a provider id")
        .to_string();
    harness.add_route("bad", "bad", &provider);
    let reply = harness.chat(json!({ "model": "bad", "messages": messages() }), "client");
    assert_eq!(reply.status, 401);
    assert_eq!(reply.body, r#"{"error":{"message":"bad upstream"}}"#);
}

#[test]
fn admin_can_add_mapping_without_restart() {
    let harness = Harness::with_demo("live-config");
    let providers = harness.admin_json("/api/admin/providers");
    assert_eq!(providers["items"].as_array().expect("items").len(), 1);
    let provider_id = providers["items"][0]["id"].as_str().expect("a provider id");
    let created = harness.admin(
        "POST",
        "/api/admin/model-routes",
        Some(&json!({ "public_model": "second", "provider_id": provider_id, "upstream_model": "second-up" })),
        Some("admin"),
    );
    assert_eq!(created.status, 201, "{}", created.body);
    assert_eq!(created.json()["upstream_model"], json!("second-up"));

    let reply = harness.chat(
        json!({ "model": "second", "messages": messages() }),
        "client",
    );
    assert_eq!(reply.status, 200, "{}", reply.body);
    assert_eq!(reply.json()["model"], json!("second-up"));

    let models = get(&format!("{}/v1/models", harness.base), Some("client")).expect("a reply");
    assert_eq!(models.status, 200);
    let listed = models.json();
    let ids: Vec<&str> = listed["data"]
        .as_array()
        .expect("models")
        .iter()
        .filter_map(|item| item["id"].as_str())
        .collect();
    assert!(
        ids.contains(&"demo") && ids.contains(&"second"),
        "models were {ids:?}"
    );
    assert_eq!(
        get(&format!("{}/v1/models", harness.base), Some("admin"))
            .expect("a reply")
            .status,
        401
    );
}

#[test]
fn admin_api_requires_the_admin_key() {
    let harness = Harness::with_demo("admin-auth");
    assert_eq!(
        harness
            .admin("GET", "/api/admin/providers", None, None)
            .status,
        401
    );
    assert_eq!(
        harness
            .admin("GET", "/api/admin/providers", None, Some("client"))
            .status,
        401
    );
    assert_eq!(
        harness.admin_json("/api/admin/access")["client_api_key"],
        json!("client")
    );
    assert_eq!(
        harness
            .admin("GET", "/api/admin/unknown", None, Some("admin"))
            .status,
        404
    );
    assert_eq!(
        harness
            .admin("DELETE", "/api/admin/providers", None, Some("admin"))
            .status,
        405
    );
}

#[test]
fn admin_page_and_static_assets_are_served() {
    let harness = Harness::with_demo("static");
    let page = get(&format!("{}/admin", harness.base), None).expect("a reply");
    assert_eq!(page.status, 200);
    assert!(page.body.contains("LLM Proxy"), "the admin page is HTML");
    assert_eq!(
        get(&format!("{}/static/admin.js", harness.base), None)
            .expect("a reply")
            .status,
        200
    );
    assert_eq!(
        get(&format!("{}/static/admin.css", harness.base), None)
            .expect("a reply")
            .status,
        200
    );
    assert_eq!(
        get(&format!("{}/healthz", harness.base), None)
            .expect("a reply")
            .json(),
        json!({ "status": "ok" })
    );
    assert_eq!(
        get(&format!("{}/nope", harness.base), None)
            .expect("a reply")
            .status,
        404
    );
}

#[test]
fn provider_secrets_are_never_returned() {
    let harness = Harness::new("secrets");
    let created = harness.admin(
        "POST",
        "/api/admin/providers",
        Some(&json!({
            "name": "keyed",
            "endpoint_url": format!("http://127.0.0.1:{}/v1/chat/completions", harness.upstream),
            "auth_type": "bearer",
            "api_key": "sk-super-secret",
            "extra_headers": { "api-version": "2024-06-01", "X-Auth-Token": "token-secret" },
        })),
        Some("admin"),
    );
    assert_eq!(created.status, 201, "{}", created.body);
    assert_eq!(created.json()["has_api_key"], json!(true));
    assert_eq!(
        created.json()["extra_headers"]["api-version"],
        json!("2024-06-01")
    );
    assert_eq!(
        created.json()["extra_headers"]["X-Auth-Token"],
        json!("[saved]")
    );
    assert!(
        !created.body.contains("sk-super-secret"),
        "the API key leaked: {}",
        created.body
    );
    assert!(
        !created.body.contains("token-secret"),
        "a secret header leaked: {}",
        created.body
    );

    // Editing without an api_key keeps the stored one.
    let saved = created.json();
    let id = saved["id"].as_str().expect("an id").to_string();
    let updated = harness.admin(
        "PUT",
        &format!("/api/admin/providers/{id}"),
        Some(&json!({ "name": "renamed", "endpoint_url": format!("http://127.0.0.1:{}/v1/chat/completions", harness.upstream), "auth_type": "bearer" })),
        Some("admin"),
    );
    assert_eq!(updated.status, 200, "{}", updated.body);
    assert_eq!(updated.json()["name"], json!("renamed"));
    assert_eq!(updated.json()["has_api_key"], json!(true));
    assert_eq!(
        harness
            .app
            .store
            .provider(&id, true)
            .expect("a provider")
            .api_key,
        "sk-super-secret"
    );
}

#[test]
fn invalid_provider_and_route_input_is_rejected() {
    let harness = Harness::new("validation");
    let bad_url = harness.admin(
        "POST",
        "/api/admin/providers",
        Some(&json!({ "name": "x", "endpoint_url": "ftp://example.com/v1" })),
        Some("admin"),
    );
    assert_eq!(bad_url.status, 400);
    assert_eq!(
        bad_url.error_message(),
        "endpoint_url must be an http(s) URL without credentials or fragment"
    );

    let missing_key = harness.admin("POST", "/api/admin/providers", Some(&json!({ "name": "x", "endpoint_url": "https://example.com/v1/chat/completions", "auth_type": "bearer" })), Some("admin"));
    assert_eq!(missing_key.status, 400);
    assert_eq!(missing_key.error_message(), "api_key is required");

    let reserved = harness.admin(
        "POST",
        "/api/admin/providers",
        Some(&json!({ "name": "x", "endpoint_url": "https://example.com/v1", "auth_type": "none", "extra_headers": { "Host": "evil" } })),
        Some("admin"),
    );
    assert_eq!(reserved.status, 400);
    assert_eq!(reserved.error_message(), "header Host is reserved");

    let provider_id = harness.add_provider("fake", false);
    harness.add_route("demo", "up-demo", &provider_id);
    let duplicate = harness.admin(
        "POST",
        "/api/admin/model-routes",
        Some(&json!({ "public_model": "demo", "upstream_model": "x", "provider_id": provider_id })),
        Some("admin"),
    );
    assert_eq!(duplicate.status, 409);
    assert_eq!(duplicate.error_kind(), "conflict");

    let blocked_delete = harness.admin(
        "DELETE",
        &format!("/api/admin/providers/{provider_id}"),
        None,
        Some("admin"),
    );
    assert_eq!(blocked_delete.status, 409);
    assert_eq!(
        blocked_delete.error_message(),
        "delete or reassign this provider's model routes first"
    );

    let missing = harness.admin(
        "GET",
        "/api/admin/providers/provider_nope",
        None,
        Some("admin"),
    );
    assert_eq!(missing.status, 404);
    assert_eq!(missing.error_kind(), "not_found");
}

#[test]
fn admin_can_read_call_logs_by_date() {
    let harness = Harness::with_demo("logs");
    let reply = harness.chat(json!({ "model": "demo", "messages": messages() }), "client");
    assert_eq!(reply.status, 200);
    let day = llm_proxy::util::now();
    let today = day[..10].to_string();
    let query = format!("/api/admin/logs?date={}&q=demo&limit=10", &day[..10]);
    let mut result = harness.admin_json(&query);
    for _ in 0..50 {
        if !result["items"].as_array().expect("items").is_empty() {
            break;
        }
        thread::sleep(Duration::from_millis(20));
        result = harness.admin_json(&query);
    }
    let items = result["items"].as_array().expect("items");
    assert!(!items.is_empty(), "the call was not logged");
    assert_eq!(items[0]["model"], json!("demo"));
    assert_eq!(items[0]["status"], json!(200));
    assert_eq!(items[0]["outcome"], json!("completed"));
    assert_eq!(items[0]["upstream_model"], json!("up-demo"));
    assert_eq!(items[0]["request_id"], json!(reply.request_id));
    assert!(result["days"]
        .as_array()
        .expect("days")
        .iter()
        .any(|item| *item == json!(today)));

    let logged = items[0]["response"]
        .as_object()
        .expect("a captured response");
    assert_eq!(logged["encoding"], json!("utf-8"));
    assert!(logged["body"].as_str().expect("a body").contains("up-demo"));

    // A well formed but unused day is an empty result, not an error.
    let empty = harness.admin_json(&format!("/api/admin/logs?date={}", "2001-02-03"));
    assert_eq!(empty["items"], json!([]));
    assert_eq!(empty["date"], json!("2001-02-03"));
    assert_eq!(
        harness
            .admin("GET", "/api/admin/logs?date=2026-1-1", None, Some("admin"))
            .status,
        400
    );
    assert_eq!(
        harness
            .admin("GET", "/api/admin/logs?date=nope", None, Some("admin"))
            .error_message(),
        "date must use YYYY-MM-DD format"
    );
    assert_eq!(
        harness
            .admin("GET", "/api/admin/logs?limit=0", None, Some("admin"))
            .status,
        400
    );
    assert_eq!(
        harness
            .admin("GET", "/api/admin/logs?limit=abc", None, Some("admin"))
            .error_message(),
        "limit must be an integer"
    );
}

#[test]
fn concurrency_limit_is_enforced_and_released() {
    let harness = Harness::with_demo("concurrency");
    let provider = harness
        .app
        .store
        .provider(
            harness.admin_json("/api/admin/providers")["items"][0]["id"]
                .as_str()
                .expect("an id"),
            true,
        )
        .expect("a provider");
    let route = ResolvedRoute {
        provider,
        upstream_model: "up-demo".to_string(),
    };
    // Fill the permit pool, then confirm a call is refused and permits are released afterwards.
    let held: Vec<bool> = (0..32).map(|_| harness.app.calls.try_acquire()).collect();
    assert!(
        held.iter().all(|acquired| *acquired),
        "32 permits must be available"
    );
    assert!(
        !harness.app.calls.try_acquire(),
        "the 33rd permit must be refused"
    );
    let refused = harness.chat(json!({ "model": "demo", "messages": messages() }), "client");
    assert_eq!(refused.status, 429);
    assert_eq!(refused.error_kind(), "rate_limit_error");
    drop(held);
    for _ in 0..32 {
        harness.app.calls.release();
    }
    assert_eq!(harness.app.calls.in_use(), 0);
    assert_eq!(
        harness
            .chat(json!({ "model": "demo", "messages": messages() }), "client")
            .status,
        200
    );
    // The server writes the response before finishing the handler and releasing
    // the permit. Give that worker a chance to finish before checking the count.
    for _ in 0..100 {
        if harness.app.calls.in_use() == 0 {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        harness.app.calls.in_use(),
        0,
        "permits are released after a call"
    );
    assert_eq!(route.upstream_model, "up-demo");
}

#[test]
fn request_body_limits_are_enforced() {
    let harness = Harness::with_demo("limits");
    let wrong_type = request(
        "POST",
        &format!("{}/v1/chat/completions", harness.base),
        Some(&json!({ "model": "demo", "messages": messages() })),
        Some("client"),
    );
    assert_eq!(wrong_type.status, 200);

    let mut stream = TcpStream::connect(format!(
        "127.0.0.1:{}",
        harness
            .base
            .rsplit(':')
            .next()
            .unwrap()
            .trim_end_matches('/')
    ))
    .expect("a connection");
    let _ = stream.write_all(b"POST /v1/chat/completions HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer client\r\nContent-Type: text/plain\r\nContent-Length: 2\r\n\r\n{}");
    let mut text = String::new();
    let _ = stream.read_to_string(&mut text);
    assert!(text.starts_with("HTTP/1.1 415"), "unexpected reply: {text}");

    let mut stream = TcpStream::connect(format!(
        "127.0.0.1:{}",
        harness
            .base
            .rsplit(':')
            .next()
            .unwrap()
            .trim_end_matches('/')
    ))
    .expect("a connection");
    let _ = stream.write_all(b"POST /v1/chat/completions HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer client\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n");
    let mut text = String::new();
    let _ = stream.read_to_string(&mut text);
    assert!(text.starts_with("HTTP/1.1 400"), "unexpected reply: {text}");
    assert!(
        text.contains("chunked request bodies are not supported"),
        "unexpected reply: {text}"
    );
}

#[test]
fn admin_provider_test_uses_the_upstream() {
    let harness = Harness::new("provider-test");
    let provider_id = harness.add_provider("fake", false);
    let reply = harness.admin(
        "POST",
        &format!("/api/admin/providers/{provider_id}/test"),
        Some(&json!({ "model": "probe" })),
        Some("admin"),
    );
    assert_eq!(reply.status, 200, "{}", reply.body);
    assert_eq!(reply.json()["model"], json!("probe"));

    let logs = Arc::new(Mutex::new(harness.admin_json("/api/admin/logs?limit=50")));
    let mut logged = logs.lock().expect("a lock").clone();
    for _ in 0..50 {
        if logged["items"]
            .as_array()
            .expect("items")
            .iter()
            .any(|item| item["source"] == json!("admin_test"))
        {
            break;
        }
        thread::sleep(Duration::from_millis(20));
        logged = harness.admin_json("/api/admin/logs?limit=50");
    }
    let item = logged["items"]
        .as_array()
        .expect("items")
        .iter()
        .find(|item| item["source"] == json!("admin_test"))
        .expect("an admin_test record");
    assert_eq!(item["status"], json!(200));

    let disabled = harness.admin("PUT", &format!("/api/admin/providers/{provider_id}"), Some(&json!({ "name": "fake", "endpoint_url": format!("http://127.0.0.1:{}/v1/chat/completions", harness.upstream), "auth_type": "none", "enabled": false })), Some("admin"));
    assert_eq!(disabled.status, 200);
    let refused = harness.admin(
        "POST",
        &format!("/api/admin/providers/{provider_id}/test"),
        Some(&json!({ "model": "probe" })),
        Some("admin"),
    );
    assert_eq!(refused.status, 409);
    assert_eq!(refused.error_message(), "provider is disabled");
}

#[test]
fn store_persists_configuration_across_reopen() {
    let temp = TempDir::new("reopen");
    let db = temp.join("proxy.sqlite3");
    let logs = temp.join("logs");
    let provider_id = {
        let store =
            App::new(&db, &logs, Some("admin".into()), Some("client".into())).expect("an app");
        let provider = store.store.save_provider(
            json!({ "name": "kept", "endpoint_url": "https://example.com/v1/chat/completions", "auth_type": "bearer", "api_key": "sk-kept" }).as_object().expect("an object"),
            None,
        ).expect("a provider");
        store.store.save_route(json!({ "public_model": "kept", "upstream_model": "kept-up", "provider_id": provider.id }).as_object().expect("an object"), None).expect("a route");
        provider.id
    };
    let reopened =
        App::new(&db, &logs, Some("admin".into()), Some("client".into())).expect("an app");
    let resolved = reopened.store.resolve("kept").expect("a resolved route");
    assert_eq!(resolved.upstream_model, "kept-up");
    assert_eq!(resolved.provider.id, provider_id);
    assert_eq!(
        resolved.api_key(),
        "sk-kept",
        "the encrypted key survives a restart"
    );

    // A wrong master key must fail fast instead of serving undecryptable configuration.
    std::env::set_var("LLM_PROXY_MASTER_KEY", "the-wrong-secret");
    let failure = App::new(&db, &logs, Some("admin".into()), Some("client".into()));
    std::env::remove_var("LLM_PROXY_MASTER_KEY");
    assert!(
        failure.is_err(),
        "a wrong master key must not open the store"
    );

    let map: Map<String, Value> = Map::new();
    assert!(reopened.store.save_provider(&map, None).is_err());
}
