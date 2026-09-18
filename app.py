#!/usr/bin/env python3
"""Single-process LLM proxy with a browser-managed SQLite configuration."""
from __future__ import annotations

import argparse
import http.client
import json
import os
import re
import secrets
import select
import socket
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

from storage import APIError, CallLogger, Capture, ConfigStore, now, text_field, token_file, uid

MAX_BODY = 10 * 1024 * 1024
MAX_RESPONSE = 50 * 1024 * 1024
MAX_LOG = 2 * 1024 * 1024
STATIC = Path(__file__).parent / "static"
SECRET_NAMES = re.compile(r"^(authorization|proxy-authorization|api[-_]?key|access_token|password|secret)$", re.I)


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()


def redact(value):
    if isinstance(value, dict):
        return {k: "[REDACTED]" if SECRET_NAMES.match(k) else redact(v) for k, v in value.items()}
    if isinstance(value, list):
        return [redact(v) for v in value]
    return value


def mask_secrets(data, route):
    for value in [route.get("api_key", ""), *route.get("extra_headers", {}).values()]:
        if value:
            data = data.replace(value.encode(), b"[REDACTED]")
    return data


class App:
    def __init__(self, db_path="./data/proxy.sqlite3", log_dir="./data/logs", *,
                 admin_key=None, proxy_key=None, max_concurrency=32):
        self.store = ConfigStore(db_path, os.environ.get("LLM_PROXY_MASTER_KEY"))
        parent = Path(db_path).parent
        self.admin_key = admin_key or os.environ.get("LLM_PROXY_ADMIN_KEY") or token_file(parent / "admin.token")
        self.proxy_key = proxy_key or os.environ.get("LLM_PROXY_API_KEY") or token_file(parent / "client.token")
        if self.admin_key == self.proxy_key:
            raise ValueError("admin and client API keys must be different")
        self.logger = CallLogger(log_dir)
        self.calls = threading.BoundedSemaphore(max_concurrency)
        self.max_body_bytes, self.max_response_bytes, self.max_log_bytes = MAX_BODY, MAX_RESPONSE, MAX_LOG

    def close(self):
        self.logger.close()
        self.store.close()


class UpstreamGuard:
    """Abort a pending upstream read on deadline or downstream disconnect."""
    def __init__(self, connection, client, timeout):
        self.connection, self.client = connection, client
        self.deadline = time.monotonic() + timeout
        self.reason = None
        self.sock = None
        self.done = threading.Event()
        self.worker = threading.Thread(target=self._watch, daemon=True)

    def __enter__(self):
        self.worker.start()
        return self

    def attach(self):
        self.sock = self.connection.sock
        self.check()

    def check(self):
        if self.reason == "client_disconnected":
            raise BrokenPipeError("client disconnected")
        if self.reason == "timeout" or time.monotonic() >= self.deadline:
            raise TimeoutError("upstream deadline exceeded")

    def _watch(self):
        while not self.done.wait(0.05):
            if time.monotonic() >= self.deadline:
                self.reason = "timeout"
            else:
                try:
                    ready, _, _ = select.select([self.client], [], [], 0)
                    if ready and self.client.recv(1, socket.MSG_PEEK) == b"":
                        self.reason = "client_disconnected"
                except OSError:
                    self.reason = "client_disconnected"
            if self.reason:
                sock = self.sock or self.connection.sock
                if sock:
                    try:
                        sock.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                return

    def __exit__(self, *exc):
        self.done.set()
        self.worker.join()
        self.connection.close()


def upstream_connection(route):
    parsed = urlsplit(route["endpoint_url"])
    cls = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
    conn = cls(parsed.hostname, parsed.port, timeout=min(10, route["timeout_ms"] / 1000))
    path = parsed.path or "/"
    if parsed.query:
        path += "?" + parsed.query
    headers = {"Content-Type": "application/json", "Accept-Encoding": "identity"}
    headers.update(route["extra_headers"])
    if route["auth_type"] == "bearer":
        headers["Authorization"] = "Bearer " + route["api_key"]
    elif route["auth_type"] == "api_key":
        headers["api-key"] = route["api_key"]
    elif route["auth_type"] == "custom_header":
        headers[route["auth_header_name"]] = route["api_key"]
    return conn, path, headers


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "LLMProxy/1.0"

    @property
    def app(self):
        return self.server.app

    def log_message(self, *args):
        pass

    def setup(self):
        super().setup()
        self.connection.settimeout(15)

    def begin(self):
        self.request_id = uid("req")
        self.sent = False
        # Close-delimited SSE avoids buffering while keeping HTTP framing correct.
        self.close_connection = True

    def send_body(self, status, body=b"", content_type="application/json; charset=utf-8", extra=None):
        self.send_response(status)
        self.send_header("X-Request-ID", self.request_id)
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Cache-Control", "no-store")
        if status != 204:
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
        for key, value in (extra or {}).items():
            self.send_header(key, value)
        self.send_header("Connection", "close")
        self.end_headers()
        self.sent = True
        if status != 204:
            self.wfile.write(body)

    def respond(self, status, value):
        self.send_body(status, encode(value))

    def fail(self, exc):
        if not self.sent:
            self.respond(exc.status, {"error": {"type": exc.kind, "message": str(exc), "request_id": self.request_id}})

    def authenticate(self, admin=False):
        expected = self.app.admin_key if admin else self.app.proxy_key
        supplied = self.headers.get("Authorization", "")
        if not secrets.compare_digest(supplied.encode(), ("Bearer " + expected).encode()):
            raise APIError(401, "admin authorization required" if admin else "client authorization required", "unauthorized")

    def read_json(self):
        if self.headers.get("Transfer-Encoding"):
            raise APIError(400, "chunked request bodies are not supported; send Content-Length")
        lengths = self.headers.get_all("Content-Length", [])
        if len(lengths) != 1 or not lengths[0].isdigit():
            raise APIError(411, "a single valid Content-Length is required")
        length = int(lengths[0])
        if length > self.app.max_body_bytes:
            raise APIError(413, "request body is too large")
        if self.headers.get_content_type() != "application/json":
            raise APIError(415, "Content-Type must be application/json")
        try:
            raw = self.rfile.read(length)
        except TimeoutError as exc:
            raise APIError(408, "request body timeout", "timeout") from exc
        if len(raw) != length:
            raise APIError(400, "incomplete request body")
        try:
            value = json.loads(raw, parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        except (ValueError, UnicodeError, RecursionError) as exc:
            raise APIError(400, "invalid JSON") from exc
        if not isinstance(value, dict):
            raise APIError(400, "request body must be a JSON object")
        return value

    def _dispatch(self):
        self.begin()
        try:
            path = urlsplit(self.path).path
            if path == "/v1/chat/completions" and self.command == "POST":
                self.chat()
                return
            if path == "/v1/models" and self.command == "GET":
                self.authenticate()
                self.respond(200, {"object": "list", "data": [
                    {"id": r["public_model"], "object": "model", "created": 0, "owned_by": "llm-proxy"}
                    for r in self.app.store.routes() if r["enabled"] and r["provider_enabled"]]})
                return
            if path == "/healthz" and self.command == "GET":
                self.respond(200, {"status": "ok"})
                return
            if path in ("/", "/admin", "/admin/", "/static/admin.js", "/static/admin.css") and self.command == "GET":
                filename, mime = ("admin.js", "text/javascript") if path.endswith(".js") else (
                    ("admin.css", "text/css") if path.endswith(".css") else ("admin.html", "text/html; charset=utf-8"))
                self.send_body(200, (STATIC / filename).read_bytes(), mime, {
                    "Content-Security-Policy": "default-src 'self'; script-src 'self'; style-src 'self'; frame-ancestors 'none'; base-uri 'none'",
                    "Referrer-Policy": "no-referrer",
                })
                return
            parts = path.strip("/").split("/")
            if parts[:2] != ["api", "admin"]:
                raise APIError(404, "not found", "not_found")
            self.authenticate(admin=True)
            self.admin(parts[2:])
        except APIError as exc:
            self.fail(exc)
        except (BrokenPipeError, ConnectionResetError, TimeoutError):
            self.close_connection = True
        except Exception:
            # Never include exception strings: URLs and credentials can be embedded in them.
            print(f"ERROR: internal request failure {self.request_id}", file=sys.stderr)
            self.fail(APIError(500, "internal server error", "internal_error"))

    do_GET = do_POST = do_PUT = do_DELETE = _dispatch

    def admin(self, parts):
        if parts == ["access"] and self.command == "GET":
            self.respond(200, {"client_api_key": self.app.proxy_key})
            return
        if not parts or parts[0] not in ("providers", "model-routes") or len(parts) > 3:
            raise APIError(404, "not found", "not_found")
        resource, item_id = parts[0], parts[1] if len(parts) > 1 else None
        store = self.app.store
        if len(parts) == 3:
            if resource == "providers" and parts[2] == "test" and self.command == "POST":
                provider = store.provider(item_id, secret=True)
                if not provider["enabled"]:
                    raise APIError(409, "provider is disabled", "conflict")
                data = self.read_json()
                model = text_field(data, "model")
                payload = {"model": model, "messages": [{"role": "user", "content": "Reply with OK."}], "stream": False, "max_tokens": 8}
                self.chat(payload, dict(provider, provider_id=provider["id"], upstream_model=model))
                return
            raise APIError(404, "not found", "not_found")
        if self.command == "GET":
            if item_id:
                self.respond(200, store.provider(item_id) if resource == "providers" else store.route(item_id))
            else:
                self.respond(200, {"items": store.providers() if resource == "providers" else store.routes()})
        elif (self.command == "POST" and not item_id) or (self.command == "PUT" and item_id):
            data = self.read_json()
            saved = store.save_provider(data, item_id) if resource == "providers" else store.save_route(data, item_id)
            self.respond(200 if item_id else 201, saved)
        elif self.command == "DELETE" and item_id:
            store.delete(resource, item_id)
            self.send_body(204)
        else:
            raise APIError(405, "method not allowed", "method_not_allowed")

    def response_headers(self, upstream, length=None):
        self.send_response(upstream.status)
        self.send_header("X-Request-ID", self.request_id)
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("X-Accel-Buffering", "no")
        self.send_header("Connection", "close")
        connection_headers = {h.strip().lower() for h in upstream.getheader("Connection", "").split(",")}
        allowed = {"content-type", "content-encoding", "retry-after", "www-authenticate"}
        for key, value in upstream.getheaders():
            if key.lower() in allowed - connection_headers:
                self.send_header(key, value)
        if length is not None:
            self.send_header("Content-Length", str(length))
        self.end_headers()
        self.sent = True

    def chat(self, supplied_payload=None, supplied_route=None):
        started = time.monotonic()
        record = dict(request_id=self.request_id, time=now(), source="admin_test" if supplied_route else "client",
                      status=500, upstream_status=None, outcome="error", error=None, stream=False,
                      first_byte_latency_ms=None, response_bytes=0, request_bytes=0)
        payload, route, capture, guard = None, supplied_route, None, None
        acquired = False
        try:
            if supplied_payload is None:
                self.authenticate()
                payload = self.read_json()
            else:
                payload = supplied_payload
            model = text_field(payload, "model")
            stream = payload.get("stream", False)
            if not isinstance(stream, bool):
                raise APIError(400, "stream must be a boolean")
            if not isinstance(payload.get("messages"), list) or not payload["messages"]:
                raise APIError(400, "messages must be a non-empty array")
            # Header/body parsing has completed; long-running streams should not
            # be terminated by the request-header timeout.
            self.connection.settimeout(None)
            record.update(model=model, stream=stream, request_bytes=len(encode(payload)))
            route = route or self.app.store.resolve(model)
            record.update(provider_id=route["provider_id"], upstream_model=route["upstream_model"])
            acquired = self.app.calls.acquire(blocking=False)
            if not acquired:
                raise APIError(429, "proxy concurrency limit reached", "rate_limit_error")
            upstream_payload = dict(payload, model=route["upstream_model"])
            conn, path, headers = upstream_connection(route)
            headers["Accept"] = "text/event-stream" if stream else "application/json"
            capture = Capture(self.app.max_log_bytes, route["log_response_body"])
            with UpstreamGuard(conn, self.connection, route["timeout_ms"] / 1000) as guard:
                conn.connect()
                guard.attach()
                conn.sock.settimeout(route["timeout_ms"] / 1000)
                conn.request("POST", path, body=encode(upstream_payload), headers=headers)
                with conn.getresponse() as response:
                    guard.check()
                    record["upstream_status"] = response.status
                    is_sse = response.getheader("Content-Type", "").split(";", 1)[0].strip().lower() == "text/event-stream"
                    if is_sse:
                        self.response_headers(response)
                        record["status"] = response.status
                        while True:
                            chunk = response.read1(32 * 1024)
                            guard.check()
                            if not chunk:
                                if response.length not in (None, 0):
                                    raise http.client.IncompleteRead(b"")
                                break
                            if record["first_byte_latency_ms"] is None:
                                record["first_byte_latency_ms"] = int((time.monotonic() - started) * 1000)
                            capture.add(chunk)
                            if capture.total > self.app.max_response_bytes:
                                raise APIError(502, "upstream response exceeds size limit", "response_too_large")
                            self.wfile.write(chunk)
                            self.wfile.flush()
                    else:
                        chunks = bytearray()
                        while True:
                            chunk = response.read1(32 * 1024)
                            guard.check()
                            if not chunk:
                                if response.length not in (None, 0):
                                    raise http.client.IncompleteRead(b"")
                                break
                            if record["first_byte_latency_ms"] is None:
                                record["first_byte_latency_ms"] = int((time.monotonic() - started) * 1000)
                            capture.add(chunk)
                            if capture.total > self.app.max_response_bytes:
                                raise APIError(502, "upstream response exceeds size limit", "response_too_large")
                            chunks.extend(chunk)
                        self.response_headers(response, len(chunks))
                        record["status"] = response.status
                        self.wfile.write(chunks)
                    record["outcome"] = "completed" if response.status < 400 else "upstream_error"
        except APIError as exc:
            record["error"] = exc.kind
            if not self.sent:
                record["status"] = exc.status
            self.fail(exc)
        except (BrokenPipeError, ConnectionResetError, TimeoutError, OSError, http.client.HTTPException) as exc:
            reason = guard.reason if guard else None
            if reason == "client_disconnected" or isinstance(exc, BrokenPipeError):
                record["error"] = "client_disconnected"
                if not self.sent:
                    record["status"] = 499
            else:
                timed_out = reason == "timeout" or isinstance(exc, TimeoutError)
                status, kind = (504, "upstream_timeout") if timed_out else (502, "upstream_error")
                record["error"] = kind
                if not self.sent:
                    record["status"] = status
                self.fail(APIError(status, "upstream request timed out" if timed_out else "upstream request failed", kind))
        finally:
            if acquired:
                self.app.calls.release()
            record["latency_ms"] = int((time.monotonic() - started) * 1000)
            if route and payload and route["log_request_body"]:
                request_capture = Capture(self.app.max_log_bytes)
                request_capture.add(mask_secrets(encode(redact(payload)), route))
                record["request"] = request_capture.export()
            if capture:
                record["response_bytes"] = capture.total
                if route["log_response_body"]:
                    raw = bytes(capture.data)
                    # Redact structured secret fields where a complete JSON response is available.
                    try:
                        raw = encode(redact(json.loads(raw)))
                    except (ValueError, UnicodeError, RecursionError):
                        pass
                    capture.data = bytearray(mask_secrets(raw, route)[:self.app.max_log_bytes])
                    record["response"] = capture.export()
            self.app.logger.write(record)


class Server(ThreadingHTTPServer):
    daemon_threads = False
    block_on_close = True
    allow_reuse_address = True

    def __init__(self, address, app):
        self.app = app
        self.workers = threading.BoundedSemaphore(64)
        super().__init__(address, Handler)

    def process_request(self, request, client_address):
        if not self.workers.acquire(False):
            try:
                request.settimeout(1)
                request.sendall(b"HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Length: 0\r\n\r\n")
            except OSError:
                pass
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except Exception:
            self.workers.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self.workers.release()


def make_server(host, port, app):
    return Server((host, port), app)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default=os.environ.get("LLM_PROXY_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("LLM_PROXY_PORT", "8080")))
    parser.add_argument("--data-dir", default=os.environ.get("LLM_PROXY_DATA_DIR", "./data"), help="SQLite, keys and logs directory")
    args = parser.parse_args()
    if not 0 <= args.port <= 65535:
        parser.error("port must be between 0 and 65535")
    data = Path(args.data_dir)
    app = App(str(data / "proxy.sqlite3"), str(data / "logs"))
    server = make_server(args.host, args.port, app)
    print(f"LLM Proxy: http://{args.host}:{server.server_port}/admin", flush=True)
    if not os.environ.get("LLM_PROXY_ADMIN_KEY"):
        print(f"Admin login token: {data / 'admin.token'}", flush=True)
    if not os.environ.get("LLM_PROXY_API_KEY"):
        print(f"Client API token: {data / 'client.token'}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        app.close()


if __name__ == "__main__":
    main()
