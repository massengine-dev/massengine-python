"""Self-contained unit & integration test suite for the MassEngine Python SDK."""

from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import socket
import threading
import time
from typing import List
import unittest
import urllib.parse

import massengine
from massengine import Doc, MassEngineClient, MassEngineError, SearchRequest, TenantQuota


class _MockGatewayHandler(BaseHTTPRequestHandler):
    last_user_agent: str = ""
    redirect_target_url: str = ""
    ready_mode: str = "200"
    get_db_405: bool = False
    recorded_paths: List[str] = []
    recorded_bodies: List[str] = []

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
        _MockGatewayHandler.last_user_agent = self.headers.get("User-Agent", "")
        _MockGatewayHandler.recorded_paths.append(f"GET {self.path}")
        auth = self.headers.get("Authorization", "")
        parsed_url = urllib.parse.urlsplit(self.path)
        p = parsed_url.path
        qs = urllib.parse.parse_qs(parsed_url.query)

        if p == "/v1/health":
            self._send(200, {"status": "ok"})
        elif p == "/v1/ready":
            mode = _MockGatewayHandler.ready_mode
            if mode == "503_json":
                self._send(503, {"ready": False, "unavailable_shards": [1, 3]})
            elif mode == "503_text":
                self._send(503, b"503 Service Unavailable", "text/plain")
            elif mode == "503_err":
                self._send(503, {"error": "cluster upstream not configured"})
            elif mode == "500":
                self._send(500, {"error": "boom"})
            else:
                self._send(200, {"ready": True})
        elif p in ("/v1/tenants/redirect-me", "/v1/tenants/redirect-me/export"):
            self.send_response(302)
            self.send_header("Location", _MockGatewayHandler.redirect_target_url)
            self.send_header("Content-Length", "0")
            self.end_headers()
        elif p == "/v1/tenants/oversized":
            self._send(200, {"tenant": "oversized", "pad": "x" * 2048})
        elif p.endswith("/usage"):
            self._send(200, {"tenant": "acme", "plan": "pro", "live_docs": 2, "read_units": 4, "write_units": 3})
        elif p == "/v1/export" or p.endswith("/export"):
            db_name = qs.get("database", [""])[0]
            doc1 = {"id": "py-1", "vector": [1.0, 0.0]}
            doc2 = {"id": "py-2", "vector": [0.0, 1.0]}
            if db_name:
                doc1["database"] = db_name
                doc2["database"] = db_name
            ndjson = (json.dumps(doc1) + "\n" + json.dumps(doc2) + "\n").encode("utf-8")
            self._send(200, ndjson, "application/x-ndjson")
        elif p == "/v1/databases" or p.endswith("/databases"):
            self._send(
                200,
                {
                    "tenant": "acme",
                    "databases": [
                        {
                            "tenant": "acme/analytics",
                            "database": "analytics",
                            "num_shards": 2,
                            "rf": 2,
                            "data_type": "int8",
                            "mode": "multi",
                            "doc_count": 5,
                        }
                    ],
                },
            )
        elif "/databases/" in p:
            if _MockGatewayHandler.get_db_405:
                self._send(405, b"Method Not Allowed", "text/plain")
                return
            db_name = urllib.parse.unquote(p.rsplit("/databases/", 1)[1])
            self._send(
                200,
                {
                    "tenant": f"acme/{db_name}",
                    "database": db_name,
                    "num_shards": 2,
                    "rf": 2,
                    "data_type": "int8",
                    "doc_count": 5,
                },
            )
        elif "/docs/" in p:
            if auth == "Bearer revoked":
                self._send(401, {"error": "unauthorized"})
            else:
                raw_id = p.split("/docs/", 1)[1]
                doc_id = "/".join(urllib.parse.unquote(seg) for seg in raw_id.split("/"))
                resp = {"id": doc_id, "tenant": "acme", "metadata": {"tier": "gold"}}
                if qs.get("database"):
                    resp["database"] = qs["database"][0]
                self._send(200, resp)
        else:
            self._send(404, {"error": "not found"})

    def do_PUT(self) -> None:
        _MockGatewayHandler.recorded_paths.append(f"PUT {self.path}")
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length).decode("utf-8")
        _MockGatewayHandler.recorded_bodies.append(raw)
        body = json.loads(raw)
        self._send(200, {"tenant": "acme", "quota": body})

    def do_POST(self) -> None:
        _MockGatewayHandler.recorded_paths.append(f"POST {self.path}")
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length).decode("utf-8")
        _MockGatewayHandler.recorded_bodies.append(raw)
        parsed_url = urllib.parse.urlsplit(self.path)
        p = parsed_url.path

        if p == "/v1/tenants/redirect-me/import":
            self.send_response(302)
            self.send_header("Location", _MockGatewayHandler.redirect_target_url)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if p == "/v1/flush":
            time.sleep(0.15)
            self._send(200, {"flushed": True})
        elif p.endswith("/keys"):
            self._send(200, {"tenant": "acme", "key": "mk_test_key_123"})
        elif p == "/v1/databases" or p.endswith("/databases"):
            body = json.loads(raw)
            self._send(
                200,
                {
                    "ok": True,
                    "tenant": "acme",
                    "database": {
                        "tenant": f"acme/{body['database']}",
                        "database": body["database"],
                        "num_shards": body.get("num_shards", 1),
                        "rf": body.get("rf", 1),
                        "data_type": body.get("data_type", "int8"),
                        "mode": body.get("mode", "pin"),
                        "doc_count": 0,
                    },
                    "partition": body["database"],
                },
            )
        elif p == "/v1/ingest" or p.endswith("/ingest"):
            body = json.loads(raw)
            self._send(200, {"ingested": len(body["docs"]), "degraded": False, "database": body.get("database", "")})
        elif p == "/v1/import" or p.endswith("/import"):
            lines = [ln for ln in raw.splitlines() if ln.strip()]
            self._send(200, {"imported": len(lines), "degraded": False})
        elif p == "/v1/docs/batch-get" or p.endswith("/docs/batch-get"):
            body = json.loads(raw)
            docs = [{"id": doc_id, "tenant": "acme", "database": body.get("database", "")} for doc_id in body.get("ids", [])]
            self._send(200, {"docs": docs})
        elif p == "/v1/search":
            self._send(200, {"hits": [{"id": "py-1", "doc_id": 1, "score": 0.99, "shard": 0}], "took_ms": 1.2})
        elif p == "/v1/search/batch":
            body = json.loads(raw)
            self._send(200, {"results": [{"hits": [{"id": f"py-{i + 1}"}]} for i in range(len(body["queries"]))]})
        else:
            self._send(200, {"ok": True})

    def do_PATCH(self) -> None:
        _MockGatewayHandler.recorded_paths.append(f"PATCH {self.path}")
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length).decode("utf-8")
        _MockGatewayHandler.recorded_bodies.append(raw)
        body = json.loads(raw)
        raw_id = urllib.parse.urlsplit(self.path).path.split("/docs/", 1)[1]
        doc_id = "/".join(urllib.parse.unquote(seg) for seg in raw_id.split("/"))
        self._send(
            200,
            {
                "id": doc_id,
                "database": body.get("database", ""),
                "category": body.get("category", 0),
                "degraded": False,
                "metadata": body.get("metadata", {}),
            },
        )

    def do_DELETE(self) -> None:
        _MockGatewayHandler.recorded_paths.append(f"DELETE {self.path}")
        self._send(200, {"deleted": True, "docs_deleted": 2, "revoked": True})


class _RedirectTrapHandler(BaseHTTPRequestHandler):
    contacted: bool = False
    leaked_auth: str = ""

    def log_message(self, format: str, *args: object) -> None:
        return

    def do_GET(self) -> None:
        _RedirectTrapHandler.contacted = True
        _RedirectTrapHandler.leaked_auth = self.headers.get("Authorization", "")
        self.send_response(200)
        self.end_headers()

    def do_POST(self) -> None:
        _RedirectTrapHandler.contacted = True
        _RedirectTrapHandler.leaked_auth = self.headers.get("Authorization", "")
        self.send_response(200)
        self.end_headers()


class TestMassEngineClient(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.trap_httpd = HTTPServer(("127.0.0.1", 0), _RedirectTrapHandler)
        trap_port = cls.trap_httpd.server_address[1]
        _MockGatewayHandler.redirect_target_url = f"http://127.0.0.1:{trap_port}/steal"
        cls.trap_thread = threading.Thread(target=cls.trap_httpd.serve_forever, daemon=True)
        cls.trap_thread.start()

        cls.httpd = HTTPServer(("127.0.0.1", 0), _MockGatewayHandler)
        port = cls.httpd.server_address[1]
        cls.base_url = f"http://127.0.0.1:{port}"
        cls.thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.httpd.shutdown()
        cls.httpd.server_close()
        cls.trap_httpd.shutdown()
        cls.trap_httpd.server_close()

    def setUp(self) -> None:
        _MockGatewayHandler.ready_mode = "200"
        _MockGatewayHandler.get_db_405 = False

    def test_sdk_lifecycle_and_user_agent(self) -> None:
        self.assertEqual(massengine.__version__, "1.1.4")
        admin = MassEngineClient(base_url=self.base_url, api_key="  admin-key  ")
        self.assertEqual(admin.health()["status"], "ok")
        self.assertEqual(_MockGatewayHandler.last_user_agent, "massengine-python/1.1.4")
        r_ok = admin.ready()
        self.assertTrue(r_ok["ready"])
        self.assertEqual(r_ok["unavailable_shards"], [])
        self.assertNotIn("admin-key", repr(admin))

        q = admin.set_tenant_quota(
            "acme",
            TenantQuota(plan="pro", max_docs=100, max_bytes=1073741824, max_qps=500.0),
        )
        self.assertEqual(q["plan"], "pro")
        self.assertEqual(q["max_docs"], 100)
        self.assertEqual(q["max_bytes"], 1073741824)

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

    def test_query_text_and_hybrid_alpha_removed_and_empty_vector_rejected(self) -> None:
        client = MassEngineClient(base_url=self.base_url, api_key="mk_valid")
        with self.assertRaises(TypeError):
            SearchRequest(tenant="acme", vector=[1.0, 0.0], query_text="removed")  # type: ignore[call-arg]
        with self.assertRaises(TypeError):
            SearchRequest(tenant="acme", vector=[1.0, 0.0], hybrid_alpha=0.5)  # type: ignore[call-arg]
        with self.assertRaises(TypeError):
            client.search("acme", vector=[1.0, 0.0], query_text="removed")  # type: ignore[call-arg]
        with self.assertRaises(TypeError):
            client.search("acme", vector=[1.0, 0.0], hybrid_alpha=0.5)  # type: ignore[call-arg]

        before_count = len(_MockGatewayHandler.recorded_paths)
        with self.assertRaises(ValueError):
            client.search("acme", vector=[])
        with self.assertRaises(ValueError):
            client.batch_search([])
        with self.assertRaises(ValueError):
            client.batch_search([SearchRequest(tenant="acme", vector=[])])
        with self.assertRaises(ValueError):
            client.batch_search([{"tenant": "acme", "vector": [], "k": 5}])
        self.assertEqual(len(_MockGatewayHandler.recorded_paths), before_count)

    def test_ready_503_returns_status_with_unavailable_shards(self) -> None:
        client = MassEngineClient(base_url=self.base_url, api_key="mk_valid")
        _MockGatewayHandler.ready_mode = "503_json"
        st = client.ready()
        self.assertFalse(st["ready"])
        self.assertEqual(st["unavailable_shards"], [1, 3])

        for err_mode in ("503_text", "503_err", "500"):
            _MockGatewayHandler.ready_mode = err_mode
            with self.assertRaises(MassEngineError):
                client.ready()

    def test_slash_doc_ids_and_doc_id_validation(self) -> None:
        client = MassEngineClient(base_url=self.base_url, api_key="mk_valid")
        slash_id = "docs/2026/q1 report.pdf#0"

        client.ingest("acme", [Doc(id=slash_id, vector=[1.0, 0.0])])
        _MockGatewayHandler.recorded_paths.clear()
        got = client.get_doc("acme", slash_id)
        self.assertEqual(got["id"], slash_id)
        self.assertEqual(
            _MockGatewayHandler.recorded_paths[-1],
            "GET /v1/tenants/acme/docs/docs/2026/q1%20report.pdf%230",
        )
        patched = client.patch_doc("acme", slash_id, category=4)
        self.assertEqual(patched["id"], slash_id)
        bg = client.batch_get_docs("acme", [slash_id])
        self.assertEqual(len(bg), 1)
        self.assertEqual(bg[0]["id"], slash_id)
        client.delete_doc("acme", slash_id)

        before = len(_MockGatewayHandler.recorded_paths)
        bad_ids = ["", ".", "..", "batch-get", "doc\x00", "doc\r\n", "doc\x7f", "a/../b", "a/./b", "/lead", "trail/", "a//b", None]
        for bad_id in bad_ids:
            with self.assertRaises(ValueError):
                client.ingest("acme", [{"id": bad_id, "vector": [1.0, 0.0]}])  # type: ignore[dict-item]
            with self.assertRaises(ValueError):
                client.import_tenant("acme", [{"id": bad_id, "vector": [1.0, 0.0]}])  # type: ignore[dict-item]
            with self.assertRaises(ValueError):
                client.get_doc("acme", bad_id)  # type: ignore[arg-type]
            with self.assertRaises(ValueError):
                client.patch_doc("acme", bad_id, category=1)  # type: ignore[arg-type]
            with self.assertRaises(ValueError):
                client.batch_get_docs("acme", [bad_id])  # type: ignore[list-item]
            with self.assertRaises(ValueError):
                client.delete_doc("acme", bad_id)  # type: ignore[arg-type]
        self.assertEqual(len(_MockGatewayHandler.recorded_paths), before)

    def test_import_and_export_tenant_non_2xx_raises_error(self) -> None:
        _RedirectTrapHandler.contacted = False
        client = MassEngineClient(base_url=self.base_url, api_key="mk_secret")
        with self.assertRaises(MassEngineError) as ctx1:
            client.import_tenant("redirect-me", [Doc(id="d1", vector=[1.0, 0.0])])
        self.assertEqual(ctx1.exception.status_code, 302)

        with self.assertRaises(MassEngineError) as ctx2:
            client.export_tenant("redirect-me")
        self.assertEqual(ctx2.exception.status_code, 302)
        self.assertFalse(_RedirectTrapHandler.contacted)

        # Also test non-2xx response objects returned directly by opener without HTTPError
        class _FakeNon2xxResp:
            status = 502

            def __init__(self, payload: bytes) -> None:
                self._buf = payload

            def __enter__(self) -> "_FakeNon2xxResp":
                return self

            def __exit__(self, *args: object) -> None:
                return None

            def read(self, n: int = -1) -> bytes:
                out, self._buf = self._buf, b""
                return out

            def readline(self, n: int = -1) -> bytes:
                out, self._buf = self._buf, b""
                return out

        class _FakeOpener:
            def open(self, req: object, timeout: float = 30.0) -> _FakeNon2xxResp:
                return _FakeNon2xxResp(b'{"error":"bad gateway"}')

        custom_client = MassEngineClient(base_url=self.base_url, api_key="mk_secret")
        custom_client._opener = _FakeOpener()  # type: ignore[assignment]
        with self.assertRaises(MassEngineError) as ctx3:
            custom_client.import_tenant("acme", [Doc(id="d1", vector=[1.0, 0.0])])
        self.assertEqual(ctx3.exception.status_code, 502)

        with self.assertRaises(MassEngineError) as ctx4:
            custom_client.export_tenant("acme")
        self.assertEqual(ctx4.exception.status_code, 502)

    def test_multi_database_crud_and_scoped_data_plane(self) -> None:
        client = MassEngineClient(base_url=self.base_url, api_key="mk_scoped")
        _MockGatewayHandler.recorded_paths.clear()

        for tenant in ("acme", ""):
            created = client.create_database(
                tenant,
                "analytics",
                num_shards=2,
                rf=2,
                data_type="int8",
                mode="multi",
            )
            self.assertEqual(created["database"], "analytics")
            self.assertEqual(created["num_shards"], 2)

            dbs = client.list_databases(tenant)
            self.assertEqual(len(dbs), 1)
            self.assertEqual(dbs[0]["database"], "analytics")

            _MockGatewayHandler.get_db_405 = False
            got_db = client.get_database(tenant, "analytics")
            self.assertEqual(got_db["database"], "analytics")

            _MockGatewayHandler.get_db_405 = True
            got_fb = client.get_database(tenant, "analytics")
            self.assertEqual(got_fb["database"], "analytics")

            deleted = client.delete_database(tenant, "analytics")
            self.assertEqual(deleted, 2)

            ing = client.ingest(tenant, [Doc(id="d1", vector=[1.0, 0.0])], database="analytics")
            self.assertEqual(ing["database"], "analytics")

            client.search(tenant, vector=[1.0, 0.0], database="analytics", k=5)
            client.batch_search([SearchRequest(tenant=tenant, vector=[1.0, 0.0], k=5)], tenant=tenant, database="analytics")

            doc = client.get_doc(tenant, "d1", database="analytics")
            self.assertEqual(doc["database"], "analytics")

            pdoc = client.patch_doc(tenant, "d1", database="analytics", category=3)
            self.assertEqual(pdoc["database"], "analytics")

            bdocs = client.batch_get_docs(tenant, ["d1"], database="analytics")
            self.assertEqual(bdocs[0]["database"], "analytics")

            client.delete_doc(tenant, "d1", database="analytics")
            imp = client.import_tenant(tenant, [Doc(id="d1", vector=[1.0, 0.0])], database="analytics")
            self.assertEqual(imp, 1)

            exp = client.export_tenant(tenant, database="analytics")
            self.assertEqual(exp[0]["database"], "analytics")

        for p in _MockGatewayHandler.recorded_paths:
            self.assertNotIn("//", p)
        self.assertIn("POST /v1/import?database=analytics", _MockGatewayHandler.recorded_paths)
        self.assertIn("GET /v1/export?database=analytics", _MockGatewayHandler.recorded_paths)

    def test_dot_segment_and_control_char_rejection(self) -> None:
        client = MassEngineClient(base_url=self.base_url, api_key="mk_valid")
        for bad_tenant in ["", ".", "..", "acme/evil", "acme\\evil", "acme\x00", "acme\n", "acme\x7f", 123]:
            with self.assertRaises(ValueError):
                client.get_tenant(bad_tenant)  # type: ignore[arg-type]

        for bad_scoped in [".", "..", "acme/evil", "acme\\evil", "acme\x00", "acme\n", "acme\x7f", 123, None]:
            with self.assertRaises(ValueError):
                client.get_doc(bad_scoped, "doc-1")  # type: ignore[arg-type]

    def test_redirect_blocked_no_auth_leak(self) -> None:
        _RedirectTrapHandler.contacted = False
        _RedirectTrapHandler.leaked_auth = ""
        client = MassEngineClient(base_url=self.base_url, api_key="mk_secret_token")
        with self.assertRaises(MassEngineError) as ctx:
            client.get_tenant("redirect-me")
        self.assertEqual(ctx.exception.status_code, 302)
        self.assertFalse(_RedirectTrapHandler.contacted)
        self.assertEqual(_RedirectTrapHandler.leaked_auth, "")

    def test_invalid_url_and_apikey_no_leak(self) -> None:
        for bad_url in ["ftp://example.com", "file:///etc/passwd", "javascript:alert(1)", "http://u:p@example.com"]:
            with self.assertRaises(ValueError):
                MassEngineClient(base_url=bad_url)

        secret = "mk_top_secret_value_999"
        for bad_key in [
            f"{secret}\r\nX-Injected: 1",
            f"{secret}\n",
            f"{secret}\x00",
            f"{secret} space",
            "   ",
        ]:
            with self.assertRaises(ValueError) as ctx:
                MassEngineClient(base_url=self.base_url, api_key=bad_key)
            self.assertNotIn(secret, str(ctx.exception))

    def test_timeout_and_max_response_bytes(self) -> None:
        small_client = MassEngineClient(base_url=self.base_url, max_response_bytes=128)
        with self.assertRaises(MassEngineError) as ctx:
            small_client.get_tenant("oversized")
        self.assertIn("max_response_bytes", str(ctx.exception))

        tiny_export_client = MassEngineClient(base_url=self.base_url, max_response_bytes=20)
        with self.assertRaises(MassEngineError) as ctx2:
            tiny_export_client.export_tenant("acme")
        self.assertIn("max_response_bytes", str(ctx2.exception))

        fast_client = MassEngineClient(base_url=self.base_url, timeout=0.03)
        with self.assertRaises((TimeoutError, socket.timeout, OSError)):
            fast_client.flush()


if __name__ == "__main__":
    unittest.main()
