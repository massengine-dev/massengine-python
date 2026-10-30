"""End-to-end integration test for the MassEngine Python SDK."""

import os
import unittest

from massengine import Doc, MassEngineClient, MassEngineError, SearchRequest, TenantQuota


class TestMassEngineClient(unittest.TestCase):
    def setUp(self) -> None:
        self.base_url = os.environ.get("MASSENGINE_URL", "http://127.0.0.1:18081")
        self.admin_key = os.environ.get("MASSENGINE_ADMIN_KEY", "test-admin-secret")
        self.admin = MassEngineClient(base_url=self.base_url, api_key=self.admin_key)

    def test_end_to_end_lifecycle(self) -> None:
        tenant = "py-sdk-tenant"
        # Clean up if exists
        try:
            self.admin.delete_tenant(tenant)
        except MassEngineError:
            pass

        # 1. Health & Readiness
        self.assertEqual(self.admin.health()["status"], "ok")
        self.assertTrue(self.admin.ready()["ready"])

        # 2. Configure Tenant Plan Quota
        q = self.admin.set_tenant_quota(
            tenant,
            TenantQuota(plan="pro", max_docs=100, max_qps=500.0),
        )
        self.assertEqual(q["plan"], "pro")
        self.assertEqual(q["max_docs"], 100)

        # 3. Create Tenant-Scoped RBAC Key
        t_key = self.admin.create_tenant_key(tenant)
        self.assertTrue(t_key.startswith("mk_"))
        client = MassEngineClient(base_url=self.base_url, api_key=t_key)

        # 4. Ingest Documents
        docs = [
            Doc(
                id="py-1",
                vector=[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                category=2,
                metadata={"tier": "gold", "score_rank": 10, "tags": ["rag", "vector"], "text": "alpha vector search"},
            ),
            Doc(
                id="py-2",
                vector=[0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                category=3,
                metadata={"tier": "silver", "score_rank": 3, "tags": ["legacy"], "text": "beta keyword index"},
            ),
        ]
        ing = client.ingest(tenant, docs)
        self.assertEqual(ing["ingested"], 2)

        # 5. Filtered & Hybrid Search
        sr = client.search(
            tenant,
            vector=[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            k=5,
            metadata_filter={
                "tier": {"$in": ["gold", "platinum"]},
                "score_rank": {"$gte": 5},
                "tags": {"$contains": "rag"},
            },
        )
        self.assertEqual(len(sr["hits"]), 1)
        self.assertEqual(sr["hits"][0]["id"], "py-1")

        # 6. Batch Search
        bs = client.batch_search(
            [
                SearchRequest(tenant=tenant, vector=[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], k=1),
                SearchRequest(tenant=tenant, vector=[0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], k=1),
            ]
        )
        self.assertEqual(len(bs["results"]), 2)
        self.assertEqual(bs["results"][1]["hits"][0]["id"], "py-2")

        # 7. Patch Document In Place
        patched = client.patch_doc(
            tenant,
            "py-2",
            category=9,
            metadata={"tier": "gold", "patched": True},
        )
        self.assertEqual(patched["category"], 9)
        self.assertEqual(patched["metadata"]["tier"], "gold")

        # 8. Export & Import NDJSON
        exported = client.export_tenant(tenant)
        self.assertEqual(len(exported), 2)

        # 9. Check Usage Metering
        usage = client.get_tenant_usage(tenant)
        self.assertEqual(usage["plan"], "pro")
        self.assertEqual(usage["live_docs"], 2)
        self.assertGreaterEqual(usage["read_units"], 3)
        self.assertGreaterEqual(usage["write_units"], 3)

        # 10. Revoke Key & Delete Tenant
        self.admin.revoke_tenant_key(tenant, t_key)
        with self.assertRaises(MassEngineError) as ctx:
            client.get_doc(tenant, "py-1")
        self.assertEqual(ctx.exception.status_code, 401)
        self.admin.delete_tenant(tenant)


if __name__ == "__main__":
    unittest.main()
