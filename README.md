# MassEngine Python SDK (`massengine`)

Official zero-dependency Python client for [MassEngine](https://massengine.com) — the sub-5ms filtered vector search database.

## Installation

```bash
pip install massengine
```

## Quickstart

```python
from massengine import MassEngineClient, Doc

# Connect to a local quickstart cluster (http://localhost:8080) or cloud gateway:
client = MassEngineClient(
    base_url="http://localhost:8080",
    api_key="mk_your_tenant_or_meadm_admin_key",
)

# 1. Ingest dense vectors + structured metadata
client.ingest(
    "acme-prod",
    [
        Doc(
            id="docs/2026/policy-1",
            vector=[0.12, -0.44, 0.88, 0.05],
            category=1,
            metadata={"department": "security", "clearance": 3, "tags": ["soc2", "policy"]},
        )
    ],
)

# 2. Dense vector search with rich metadata filter operators
res = client.search(
    "acme-prod",
    vector=[0.12, -0.44, 0.88, 0.05],
    k=10,
    min_score=0.5,
    metadata_filter={
        "department": {"$in": ["security", "legal"]},
        "clearance": {"$gte": 2},
        "tags": {"$contains": "soc2"},
    },
)
print(res["hits"])

# 3. In-place metadata patching without re-embedding
client.patch_doc(
    "acme-prod",
    "docs/2026/policy-1",
    metadata={"clearance": 4, "reviewed": True},
)

# 4. Inspect tenant quota & billable usage
usage = client.get_tenant_usage("acme-prod")
print(usage)
```

## Multi-Database Scoping

Create isolated databases within a tenant and scope operations using `database=` (pass `tenant=""` when authenticating with a tenant-scoped `mk_...` key):

```python
client.create_database("", "analytics", data_type="int8")

client.ingest(
    "",
    [Doc(id="event-1", vector=[0.1, 0.2, 0.3, 0.4])],
    database="analytics",
)

hits = client.search("", vector=[0.1, 0.2, 0.3, 0.4], database="analytics", k=5)
```

## License

Apache License 2.0
