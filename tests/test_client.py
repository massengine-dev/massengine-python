"""Self-contained unit & integration test suite for the MassEngine Python SDK."""

from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import threading
import unittest

from massengine import Doc, MassEngineClient, MassEngineError, SearchRequest, TenantQuota


class _MockGatewayHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:
        return

    def _send(self, code: int, payload: object, content_type: str = "application/json") -> None:
        raw = payload if isinstance(payload, bytes) else json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self) -> None:
        auth = self.headers.get("Authorization", "")
        if self.path == "/v1/health":
            self._send(200, {"status": "ok"})
        elif self.path == "/v1/ready":
            self._send(200, {"ready": True})
        elif self.path.endswith("/usage"):
            self._send(200, {"tenant": "acme", "plan": "pro", "live_docs": 2, "read_units": 4, "write_units": 3})
        elif self.path.endswith("/export"):
            ndjson = b'{"id":"py-1","vector":[1.0,0.0]}\n{"id":"py-2","vector":[0.0,1.0]}\n'
            self._send(200, ndjson, "application/x-ndjson")
        elif self.path.endswith("/docs/py-1"):
            if auth == "Bearer revoked":
                self._send(401, {"error": "unauthorized"})
            else:
                self._send(200, {"id": "py-1", "tenant": "acme", "metadata": {"tier": "gold"}})
        else:
            self._send(404, {"error": "not found"})

    def do_PUT(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        body = json.loads(self.rfile.read(length).decode("utf-8"))
        self._send(200, {"tenant": "acme", "quota": body})

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length).decode("utf-8")
        if self.path.endswith("/keys"):
            self._send(200, {"tenant": "acme", "key": "mk_test_key_123"})
        elif self.path.endswith("/ingest"):
            body = json.loads(raw)
            self._send(200, {"ingested": len(body["docs"]), "degraded": False})
        elif self.path == "/v1/search":
            self._send(200, {"hits": [{"id": "py-1", "doc_id": 1, "score": 0.99, "shard": 0}], "stats": {}})
        elif self.path == "/v1/search/batch":
            self._send(200, {"results": [{"hits": [{"id": "py-1"}]}, {"hits": [{"id": "py-2"}]}]})
        else:
            self._send(200, {"ok": True})

    def do_PATCH(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        body = json.loads(self.rfile.read(length).decode("utf-8"))
        self._send(200, {"id": "py-2", "category": body.get("category", 0), "metadata": body.get("metadata", {})})

    def do_DELETE(self) -> None:
        self._send(200, {"deleted": True, "docs_deleted": 2, "revoked": True})


class TestMassEngineClient(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.httpd = HTTPServer(("127.0.0.1", 0), _MockGatewayHandler)
        port = cls.httpd.server_address[1]
        cls.base_url = f"http://127.0.0.1:{port}"
        cls.thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.httpd.shutdown()
        cls.httpd.server_close()

    def test_sdk_lifecycle(self) -> None:
        admin = MassEngineClient(base_url=self.base_url, api_key="admin-key")
        self.assertEqual(admin.health()["status"], "ok")
        self.assertTrue(admin.ready()["ready"])

        q = admin.set_tenant_quota("acme", TenantQuota(plan="pro", max_docs=100, max_qps=500.0))
        self.assertEqual(q["plan"], "pro")
        self.assertEqual(q["max_docs"], 100)

        t_key = admin.create_tenant_key("acme")
        self.assertTrue(t_key.startswith("mk_"))

        client = MassEngineClient(base_url=self.base_url, api_key=t_key)
        ing = client.ingest(
            "acme",
            [
                Doc(id="py-1", vector=[1.0, 0.0], category=2, metadata={"tier": "gold"}),
                Doc(id="py-2", vector=[0.0, 1.0], category=3, metadata={"tier": "silver"}),
            ],
        )
        self.assertEqual(ing["ingested"], 2)

        sr = client.search(
            "acme",
            vector=[1.0, 0.0],
            k=5,
            metadata_filter={"tier": {"$eq": "gold"}},
        )
        self.assertEqual(len(sr["hits"]), 1)
        self.assertEqual(sr["hits"][0]["id"], "py-1")

        bs = client.batch_search(
            [
                SearchRequest(tenant="acme", vector=[1.0, 0.0], k=1),
                SearchRequest(tenant="acme", vector=[0.0, 1.0], k=1),
            ]
        )
        self.assertEqual(len(bs["results"]), 2)

        patched = client.patch_doc("acme", "py-2", category=9, metadata={"tier": "gold"})
        self.assertEqual(patched["category"], 9)

        exported = client.export_tenant("acme")
        self.assertEqual(len(exported), 2)

        usage = client.get_tenant_usage("acme")
        self.assertEqual(usage["plan"], "pro")
        self.assertEqual(usage["live_docs"], 2)

        revoked_client = MassEngineClient(base_url=self.base_url, api_key="revoked")
        with self.assertRaises(MassEngineError) as ctx:
            revoked_client.get_doc("acme", "py-1")
        self.assertEqual(ctx.exception.status_code, 401)


if __name__ == "__main__":
    unittest.main()
