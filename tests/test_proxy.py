import json
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import app


class FakeLLM(BaseHTTPRequestHandler):
    def do_POST(self):
        size = int(self.headers.get("Content-Length", 0))
        payload = json.loads(self.rfile.read(size))
        if payload.get("model") == "bad":
            body = b'{"error":{"message":"bad upstream"}}'
            self.send_response(401)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if payload.get("stream"):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            for chunk in (b'data: {"choices":[{"delta":{"content":"ok"}}]}\n\n', b"data: [DONE]\n\n"):
                self.wfile.write(chunk)
                self.wfile.flush()
                time.sleep(0.01)
            return
        body = json.dumps({"model": payload["model"], "choices": []}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        pass


class ProxyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.upstream = ThreadingHTTPServer(("127.0.0.1", 0), FakeLLM)
        threading.Thread(target=cls.upstream.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.upstream.shutdown()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.app = app.App(self.temp.name + "/proxy.sqlite3", self.temp.name + "/logs", admin_key="admin", proxy_key="client")
        provider = self.app.store.save_provider({
            "name": "fake",
            "endpoint_url": f"http://127.0.0.1:{self.upstream.server_port}/v1/chat/completions",
            "auth_type": "none",
            "log_response_body": True,
        })
        self.app.store.save_route({"public_model": "demo", "provider_id": provider["id"], "upstream_model": "up-demo"})
        self.server = app.make_server("127.0.0.1", 0, self.app)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.app.close()
        self.temp.cleanup()

    def request(self, payload, stream=False, auth="client"):
        body = json.dumps(payload).encode()
        req = Request(self.base + "/v1/chat/completions", data=body, method="POST", headers={
            "Content-Type": "application/json", "Authorization": f"Bearer {auth}"
        })
        return urlopen(req, timeout=3)

    def test_non_stream_rewrites_model_and_returns_response(self):
        response = self.request({"model": "demo", "messages": [{"role": "user", "content": "hi"}]})
        self.assertEqual(response.status, 200)
        self.assertEqual(json.loads(response.read())["model"], "up-demo")

    def test_stream_is_forwarded(self):
        response = self.request({"model": "demo", "messages": [{"role": "user", "content": "hi"}], "stream": True})
        body = response.read()
        self.assertIn(b"data: [DONE]", body)
        self.assertEqual(response.headers.get_content_type(), "text/event-stream")

    def test_unknown_model_and_auth_are_rejected(self):
        with self.assertRaises(HTTPError) as unknown:
            self.request({"model": "missing", "messages": [{"role": "user", "content": "hi"}]})
        self.assertEqual(unknown.exception.code, 404)
        with self.assertRaises(HTTPError) as unauthorized:
            self.request({"model": "demo", "messages": [{"role": "user", "content": "hi"}]}, auth="wrong")
        self.assertEqual(unauthorized.exception.code, 401)

    def test_admin_can_add_mapping_without_restart(self):
        providers = json.loads(urlopen(Request(self.base + "/api/admin/providers", headers={"Authorization": "Bearer admin"})).read())
        self.assertEqual(len(providers["items"]), 1)
        provider_id = providers["items"][0]["id"]
        body = json.dumps({"public_model": "second", "provider_id": provider_id, "upstream_model": "second-up"}).encode()
        req = Request(self.base + "/api/admin/model-routes", data=body, method="POST", headers={"Authorization": "Bearer admin", "Content-Type": "application/json"})
        self.assertEqual(urlopen(req).status, 201)
        self.assertEqual(self.request({"model": "second", "messages": [{"role": "user", "content": "hi"}]}).status, 200)


if __name__ == "__main__":
    unittest.main()
