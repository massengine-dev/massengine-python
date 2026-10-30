# MassEngine Python SDK (`massengine`)

Official zero-dependency Python client for [MassEngine](https://massengine.com) — the sub-5ms filtered and hybrid vector search database.

## Installation

```bash
pip install git+https://github.com/massengine-dev/massengine-python.git@v1.1.0
```

## Quickstart

```python
from massengine import MassEngineClient, Doc

client = MassEngineClient(
    base_url="https://api.massengine.com",
    api_key="mk_your_tenant_api_key",
)

# 1. Ingest vectors + structured metadata
client.ingest(
    "acme-prod",
    [
        Doc(
            id="doc-1",
            vector=[0.12, -0.44, 0.88, 0.05],
            category=1,
            metadata={"department": "security", "clearance": 3, "tags": ["soc2", "policy"]},
        )
    ],
)

# 2. Hybrid dense + BM25 search with rich metadata filter operators
res = client.search(
    "acme-prod",
    vector=[0.12, -0.44, 0.88, 0.05],
    query_text="soc2 policy",
    hybrid_alpha=0.7,
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
    "doc-1",
    metadata={"clearance": 4, "reviewed": True},
)

# 4. Inspect tenant quota & billable usage
usage = client.get_tenant_usage("acme-prod")
print(usage)
```

## License

Apache License 2.0
