"""SQLite configuration and bounded, append-only local call logs."""
import base64
import hashlib
import json
import os
import queue
import re
import secrets
import sqlite3
import sys
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

from cryptography.fernet import Fernet


def now():
    return datetime.now(timezone.utc).isoformat()


def uid(prefix):
    return f"{prefix}_{uuid.uuid4().hex}"


class APIError(Exception):
    def __init__(self, status, message, kind="invalid_request"):
        super().__init__(message)
        self.status, self.kind = status, kind


def private_file(path, initial):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        return path.read_bytes().strip()
    with os.fdopen(fd, "wb") as out:
        out.write(initial)
    return initial


def token_file(path):
    return private_file(path, secrets.token_urlsafe(32).encode()).decode()


def text_field(data, name, default=None):
    value = data.get(name, default)
    if not isinstance(value, str) or not value.strip() or len(value) > 8192:
        raise APIError(400, f"{name} must be a non-empty string (max 8192 characters)")
    return value.strip()


def flag(data, name, default=True):
    value = data.get(name, default)
    if not isinstance(value, bool):
        raise APIError(400, f"{name} must be a boolean")
    return value


HEADER_NAME = re.compile(r"^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")
BLOCKED_HEADERS = {
    "host", "content-length", "transfer-encoding", "connection", "keep-alive",
    "upgrade", "te", "trailer", "proxy-authorization", "proxy-authenticate",
    "authorization", "cookie", "content-type", "accept-encoding",
}


def header_name(value, auth=False):
    if not isinstance(value, str) or not HEADER_NAME.fullmatch(value):
        raise APIError(400, "invalid header name")
    if value.lower() in BLOCKED_HEADERS and not (auth and value.lower() == "authorization"):
        raise APIError(400, f"header {value} is reserved")
    return value


def header_value(value):
    if not isinstance(value, str) or any(ord(c) < 32 or ord(c) > 255 or ord(c) == 127 for c in value):
        raise APIError(400, "header values must contain printable Latin-1 characters")
    return value


def masked_header(name, value):
    if re.search(r"(authorization|api[-_]?key|token|secret|password)", name, re.I):
        return "[saved]"
    return value


def endpoint(value):
    value = text_field({"endpoint_url": value}, "endpoint_url")
    try:
        parsed = urlsplit(value)
        valid = parsed.scheme in ("http", "https") and parsed.hostname and parsed.port != 0
    except ValueError:
        valid = False
    if not valid or parsed.username or parsed.password or parsed.fragment:
        raise APIError(400, "endpoint_url must be an http(s) URL without credentials or fragment")
    if any(ord(c) <= 32 or ord(c) > 126 for c in value):
        raise APIError(400, "endpoint_url must be ASCII; encode non-ASCII URL characters")
    return value


class ConfigStore:
    def __init__(self, db_path, master_key=None):
        self.path = Path(db_path)
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if master_key:
            try:
                # Accept a regular environment secret as well as a Fernet key.
                Fernet(master_key.encode())
                key = master_key.encode()
            except (ValueError, TypeError):
                key = base64.urlsafe_b64encode(hashlib.sha256(master_key.encode()).digest())
        else:
            key = private_file(str(self.path) + ".key", Fernet.generate_key())
        self.cipher = Fernet(key)
        self.lock = threading.RLock()
        self.db = sqlite3.connect(self.path, check_same_thread=False)
        os.chmod(self.path, 0o600)
        self.db.row_factory = sqlite3.Row
        self.db.executescript('''
            PRAGMA foreign_keys=ON;
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
            );
        ''')
        self.db.commit()
        # Fail early if a supplied encryption key cannot decrypt existing configuration.
        for row in self.db.execute("SELECT secret FROM providers"):
            self.cipher.decrypt(row[0].encode())

    def close(self):
        with self.lock:
            self.db.close()

    def _provider(self, row, secret=False):
        item = dict(row)
        key = self.cipher.decrypt(item.pop("secret").encode()).decode()
        headers = json.loads(self.cipher.decrypt(item.pop("headers").encode()))
        item["has_api_key"] = bool(key)
        item["extra_headers"] = headers if secret else {k: masked_header(k, v) for k, v in headers.items()}
        if secret:
            item["api_key"] = key
        for name in ("enabled", "log_request_body", "log_response_body"):
            item[name] = bool(item[name])
        return item

    def providers(self):
        with self.lock:
            return [self._provider(row) for row in self.db.execute("SELECT * FROM providers ORDER BY name")]

    def provider(self, provider_id, secret=False):
        with self.lock:
            row = self.db.execute("SELECT * FROM providers WHERE id=?", (provider_id,)).fetchone()
            if not row:
                raise APIError(404, "provider not found", "not_found")
            return self._provider(row, secret)

    def save_provider(self, data, provider_id=None):
        with self.lock:
            old = self.provider(provider_id, True) if provider_id else {}
            name = text_field(data, "name")
            url = endpoint(data.get("endpoint_url"))
            auth = data.get("auth_type", "bearer")
            if auth not in ("none", "bearer", "api_key", "custom_header"):
                raise APIError(400, "invalid auth_type")
            auth_header = ""
            if auth == "custom_header":
                auth_header = header_name(data.get("auth_header_name"), auth=True)
            key = data.get("api_key", old.get("api_key", ""))
            header_value(key)
            if auth == "none":
                key = ""
            elif not key:
                raise APIError(400, "api_key is required")
            headers = data.get("extra_headers", old.get("extra_headers", {}))
            if not isinstance(headers, dict) or len(headers) > 32:
                raise APIError(400, "extra_headers must be an object with at most 32 headers")
            for k, v in headers.items():
                header_name(k)
                header_value(v)
                if k.lower() in {"api-key", auth_header.lower()}:
                    raise APIError(400, "use the authentication fields for authentication headers")
            if len({k.lower() for k in headers}) != len(headers):
                raise APIError(400, "duplicate header names")
            timeout = data.get("timeout_ms", 120000)
            if type(timeout) is not int or not 100 <= timeout <= 3600000:
                raise APIError(400, "timeout_ms must be an integer between 100 and 3600000")
            item = dict(
                id=provider_id or uid("provider"), name=name, endpoint_url=url,
                auth_type=auth, auth_header_name=auth_header,
                secret=self.cipher.encrypt(key.encode()).decode(),
                headers=self.cipher.encrypt(json.dumps(headers).encode()).decode(),
                timeout_ms=timeout, enabled=flag(data, "enabled"),
                log_request_body=flag(data, "log_request_body"),
                log_response_body=flag(data, "log_response_body"),
                created_at=old.get("created_at", now()), updated_at=now(),
            )
            with self.db:
                if old:
                    assignments = ",".join(f"{k}=:{k}" for k in item if k != "id")
                    self.db.execute(f"UPDATE providers SET {assignments} WHERE id=:id", item)
                else:
                    cols = ",".join(item)
                    placeholders = ",".join(f":{k}" for k in item)
                    self.db.execute(f"INSERT INTO providers ({cols}) VALUES ({placeholders})", item)
            return self.provider(item["id"])

    def routes(self):
        with self.lock:
            rows = self.db.execute('''SELECT r.*, p.name provider_name, p.enabled provider_enabled
                FROM model_routes r JOIN providers p ON r.provider_id=p.id ORDER BY public_model''')
            return [dict(row, enabled=bool(row["enabled"]), provider_enabled=bool(row["provider_enabled"])) for row in rows]

    def route(self, route_id):
        with self.lock:
            for route in self.routes():
                if route["id"] == route_id:
                    return route
            raise APIError(404, "model route not found", "not_found")

    def save_route(self, data, route_id=None):
        with self.lock:
            old = self.route(route_id) if route_id else {}
            public = text_field(data, "public_model")
            upstream = text_field(data, "upstream_model")
            provider_id = text_field(data, "provider_id")
            self.provider(provider_id)
            item = dict(id=route_id or uid("route"), public_model=public, upstream_model=upstream,
                        provider_id=provider_id, enabled=flag(data, "enabled"),
                        created_at=old.get("created_at", now()), updated_at=now())
            try:
                with self.db:
                    if old:
                        self.db.execute('''UPDATE model_routes SET public_model=:public_model,
                            upstream_model=:upstream_model, provider_id=:provider_id,
                            enabled=:enabled, updated_at=:updated_at WHERE id=:id''', item)
                    else:
                        self.db.execute('''INSERT INTO model_routes VALUES
                            (:id,:public_model,:provider_id,:upstream_model,:enabled,:created_at,:updated_at)''', item)
            except sqlite3.IntegrityError as exc:
                raise APIError(409, "public_model is already mapped", "conflict") from exc
            return self.route(item["id"])

    def resolve(self, model):
        # Each call takes one immutable snapshot; in-flight calls keep their original config.
        with self.lock:
            row = self.db.execute("SELECT * FROM model_routes WHERE public_model=?", (model,)).fetchone()
            if not row or not row["enabled"]:
                raise APIError(404, "model is not configured or is disabled", "model_not_found")
            provider = self.provider(row["provider_id"], True)
            if not provider["enabled"]:
                raise APIError(404, "model provider is disabled", "model_not_found")
            return dict(provider, provider_id=provider["id"], upstream_model=row["upstream_model"])

    def delete(self, resource, item_id):
        with self.lock:
            try:
                with self.db:
                    if resource == "providers":
                        self.provider(item_id)
                        self.db.execute("DELETE FROM providers WHERE id=?", (item_id,))
                    else:
                        self.route(item_id)
                        self.db.execute("DELETE FROM model_routes WHERE id=?", (item_id,))
            except sqlite3.IntegrityError as exc:
                raise APIError(409, "delete or reassign this provider's model routes first", "conflict") from exc


class Capture:
    def __init__(self, limit, enabled=True):
        self.limit = limit if enabled else 0
        self.data = bytearray()
        self.total = 0

    def add(self, chunk):
        self.total += len(chunk)
        self.data.extend(chunk[:max(0, self.limit - len(self.data))])

    def export(self):
        raw = bytes(self.data)
        try:
            content = raw.decode("utf-8")
            encoding = "utf-8"
        except UnicodeDecodeError:
            content = base64.b64encode(raw).decode()
            encoding = "base64"
        return dict(body=content, encoding=encoding, bytes=self.total, truncated=self.total > len(raw))


class CallLogger:
    """A bounded in-process queue; overload falls back to a synchronous append."""
    def __init__(self, directory):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.lock = threading.Lock()
        self.queue = queue.Queue(maxsize=16)
        self.failures = 0
        self.worker = threading.Thread(target=self._run, name="call-logger", daemon=True)
        self.worker.start()

    def _write(self, record):
        try:
            day = record["time"][:10]
            with self.lock:
                fd = os.open(self.directory / f"{day}.jsonl", os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
                with os.fdopen(fd, "a", encoding="utf-8") as out:
                    out.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
        except OSError:
            self.failures += 1
            print("ERROR: failed to write call log (check local disk)", file=sys.stderr)

    def _run(self):
        while True:
            record = self.queue.get()
            try:
                if record is None:
                    return
                self._write(record)
            finally:
                self.queue.task_done()

    def write(self, record):
        try:
            self.queue.put_nowait(record)
        except queue.Full:
            self._write(record)

    def list_days(self):
        with self.lock:
            return sorted((path.stem for path in self.directory.glob("*.jsonl")
                           if re.fullmatch(r"\d{4}-\d{2}-\d{2}", path.stem)), reverse=True)

    def read_day(self, day, limit=100, query=""):
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", day):
            raise APIError(400, "date must use YYYY-MM-DD format")
        if type(limit) is not int or not 1 <= limit <= 500:
            raise APIError(400, "limit must be between 1 and 500")
        query = query.strip().lower()[:200]
        # Make queued records visible before an administrator reads the file.
        self.queue.join()
        path = self.directory / f"{day}.jsonl"
        items = []
        if not path.exists():
            return {"date": day, "items": [], "days": self.list_days()}
        with self.lock:
            with path.open("r", encoding="utf-8", errors="replace") as handle:
                for line in handle:
                    try:
                        item = json.loads(line)
                    except (ValueError, TypeError):
                        continue
                    if query and query not in json.dumps(item, ensure_ascii=False).lower():
                        continue
                    items.append(item)
                    if len(items) > limit:
                        items.pop(0)
        items.reverse()
        return {"date": day, "items": items, "days": self.list_days()}

    def close(self):
        self.queue.join()
        self.queue.put(None)
        self.worker.join()
