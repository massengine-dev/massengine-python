"""Zero-dependency Python HTTP/JSON client for MassEngine."""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, Dict, Iterable, List, Optional, Sequence, Union
import urllib.error
import urllib.parse
import urllib.request


class MassEngineError(Exception):
    """Raised when the MassEngine gateway returns a non-2xx HTTP status."""

    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(f"massengine api (status {status_code}): {message}")
        self.status_code = status_code
        self.message = message


@dataclass
class Doc:
    """Represents a vector document for ingestion or export."""

    id: str
    vector: Sequence[float]
    date: Optional[int] = None
    category: Optional[int] = None
    metadata: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "id": self.id,
            "vector": list(self.vector),
        }
        if self.date is not None:
            out["date"] = int(self.date)
        if self.category is not None:
            out["category"] = int(self.category)
        if self.metadata is not None:
            out["metadata"] = self.metadata
        return out


@dataclass
class SearchRequest:
    """Specifies a single vector, lexical, or hybrid query."""

    tenant: str
    k: int = 10
    vector: Optional[Sequence[float]] = None
    query_text: Optional[str] = None
    hybrid_alpha: Optional[float] = None
    metadata_filter: Optional[Dict[str, Any]] = None
    offset: int = 0
    min_score: Optional[float] = None
    rerank_pool: Optional[int] = None
    nprobe: Optional[int] = None
    min_date: Optional[int] = None
    max_date: Optional[int] = None
    category: Optional[int] = None
    categories: Optional[Sequence[int]] = None

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "tenant": self.tenant,
            "k": self.k,
        }
        if self.vector is not None:
            out["vector"] = list(self.vector)
        if self.query_text:
            out["query_text"] = self.query_text
        if self.hybrid_alpha is not None:
            out["hybrid_alpha"] = float(self.hybrid_alpha)
        if self.metadata_filter is not None:
            out["metadata_filter"] = self.metadata_filter
        if self.offset:
            out["offset"] = int(self.offset)
        if self.min_score is not None:
            out["min_score"] = float(self.min_score)
        if self.rerank_pool:
            out["rerank_pool"] = int(self.rerank_pool)
        if self.nprobe:
            out["nprobe"] = int(self.nprobe)
        if self.min_date is not None:
            out["min_date"] = int(self.min_date)
        if self.max_date is not None:
            out["max_date"] = int(self.max_date)
        if self.category is not None:
            out["category"] = int(self.category)
        if self.categories:
            out["categories"] = [int(c) for c in self.categories]
        return out


@dataclass
class TenantQuota:
    """Configures a tenant's commercial plan tier, limits, and suspension status."""

    plan: str = ""
    max_docs: int = 0
    max_qps: float = 0.0
    burst: int = 0
    suspended: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "plan": self.plan,
            "max_docs": int(self.max_docs),
            "max_qps": float(self.max_qps),
            "burst": int(self.burst),
            "suspended": bool(self.suspended),
        }


class MassEngineClient:
    """Official Python SDK client for the MassEngine HTTP REST API."""

    def __init__(
        self,
        base_url: str = "http://localhost:8081",
        api_key: Optional[str] = None,
        timeout: float = 30.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout

    def _headers(self, content_type: Optional[str] = "application/json") -> Dict[str, str]:
        hdrs: Dict[str, str] = {"User-Agent": "massengine-python/1.1.0"}
        if content_type:
            hdrs["Content-Type"] = content_type
        if self.api_key:
            hdrs["Authorization"] = f"Bearer {self.api_key}"
        return hdrs

    def _request_json(
        self,
        method: str,
        path: str,
        body: Optional[Any] = None,
    ) -> Any:
        data: Optional[bytes] = None
        content_type: Optional[str] = None
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            content_type = "application/json"

        req = urllib.request.Request(
            url=f"{self.base_url}{path}",
            data=data,
            headers=self._headers(content_type),
            method=method,
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read()
                if not raw:
                    return None
                return json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as e:
            raw_err = e.read().decode("utf-8", errors="replace")
            msg = raw_err.strip()
            try:
                parsed = json.loads(raw_err)
                if isinstance(parsed, dict) and parsed.get("error"):
                    msg = str(parsed["error"])
            except Exception:
                pass
            raise MassEngineError(e.code, msg) from None

    @staticmethod
    def _esc(segment: str) -> str:
        return urllib.parse.quote(segment, safe="")

    def ingest(
        self,
        tenant: str,
        docs: Sequence[Union[Doc, Dict[str, Any]]],
    ) -> Dict[str, Any]:
        """Inserts or upserts a batch of documents into `tenant`."""
        serialized = [d.to_dict() if isinstance(d, Doc) else d for d in docs]
        return self._request_json(
            "POST",
            f"/v1/tenants/{self._esc(tenant)}/ingest",
            {"docs": serialized},
        )

    def search(
        self,
        tenant: str,
        *,
        vector: Optional[Sequence[float]] = None,
        query_text: Optional[str] = None,
        k: int = 10,
        hybrid_alpha: Optional[float] = None,
        metadata_filter: Optional[Dict[str, Any]] = None,
        offset: int = 0,
        min_score: Optional[float] = None,
        rerank_pool: Optional[int] = None,
        nprobe: Optional[int] = None,
        min_date: Optional[int] = None,
        max_date: Optional[int] = None,
        category: Optional[int] = None,
        categories: Optional[Sequence[int]] = None,
    ) -> Dict[str, Any]:
        """Executes a dense vector, lexical BM25, or hybrid query."""
        sr = SearchRequest(
            tenant=tenant,
            k=k,
            vector=vector,
            query_text=query_text,
            hybrid_alpha=hybrid_alpha,
            metadata_filter=metadata_filter,
            offset=offset,
            min_score=min_score,
            rerank_pool=rerank_pool,
            nprobe=nprobe,
            min_date=min_date,
            max_date=max_date,
            category=category,
            categories=categories,
        )
        return self._request_json("POST", "/v1/search", sr.to_dict())

    def batch_search(
        self,
        queries: Sequence[Union[SearchRequest, Dict[str, Any]]],
    ) -> Dict[str, Any]:
        """Executes multiple search queries in a single round-trip."""
        serialized = [
            q.to_dict() if isinstance(q, SearchRequest) else q for q in queries
        ]
        return self._request_json("POST", "/v1/search/batch", {"queries": serialized})

    def get_doc(self, tenant: str, doc_id: str) -> Dict[str, Any]:
        """Fetches a document's metadata by external ID."""
        return self._request_json(
            "GET",
            f"/v1/tenants/{self._esc(tenant)}/docs/{self._esc(doc_id)}",
        )

    def patch_doc(
        self,
        tenant: str,
        doc_id: str,
        *,
        metadata: Optional[Dict[str, Any]] = None,
        date: Optional[int] = None,
        category: Optional[int] = None,
        replace_metadata: bool = False,
    ) -> Dict[str, Any]:
        """Updates a document's metadata, date, or category in place without re-embedding."""
        body: Dict[str, Any] = {}
        if metadata is not None:
            body["metadata"] = metadata
        if date is not None:
            body["date"] = int(date)
        if category is not None:
            body["category"] = int(category)
        if replace_metadata:
            body["replace_metadata"] = True
        return self._request_json(
            "PATCH",
            f"/v1/tenants/{self._esc(tenant)}/docs/{self._esc(doc_id)}",
            body,
        )

    def batch_get_docs(self, tenant: str, ids: Sequence[str]) -> List[Dict[str, Any]]:
        """Fetches multiple documents by external ID in a single request."""
        resp = self._request_json(
            "POST",
            f"/v1/tenants/{self._esc(tenant)}/docs/batch-get",
            {"ids": list(ids)},
        )
        return resp.get("docs", [])

    def delete_doc(self, tenant: str, doc_id: str) -> Dict[str, Any]:
        """Tombstones a document by external ID."""
        return self._request_json(
            "DELETE",
            f"/v1/tenants/{self._esc(tenant)}/docs/{self._esc(doc_id)}",
        )

    def import_tenant(
        self,
        tenant: str,
        docs: Iterable[Union[Doc, Dict[str, Any]]],
    ) -> int:
        """Streams a collection of documents into `tenant` via NDJSON."""
        lines = [
            json.dumps(d.to_dict() if isinstance(d, Doc) else d) for d in docs
        ]
        payload = ("\n".join(lines) + "\n").encode("utf-8")
        req = urllib.request.Request(
            url=f"{self.base_url}/v1/tenants/{self._esc(tenant)}/import",
            data=payload,
            headers=self._headers("application/x-ndjson"),
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                out = json.loads(resp.read().decode("utf-8"))
                return int(out.get("imported", 0))
        except urllib.error.HTTPError as e:
            raw_err = e.read().decode("utf-8", errors="replace")
            msg = raw_err.strip()
            try:
                parsed = json.loads(raw_err)
                if isinstance(parsed, dict) and parsed.get("error"):
                    msg = str(parsed["error"])
            except Exception:
                pass
            raise MassEngineError(e.code, msg) from None

    def export_tenant(self, tenant: str) -> List[Dict[str, Any]]:
        """Exports all live documents belonging to `tenant` via streaming NDJSON."""
        req = urllib.request.Request(
            url=f"{self.base_url}/v1/tenants/{self._esc(tenant)}/export",
            headers=self._headers(None),
            method="GET",
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read().decode("utf-8")
                docs: List[Dict[str, Any]] = []
                for line in raw.splitlines():
                    line = line.strip()
                    if line:
                        docs.append(json.loads(line))
                return docs
        except urllib.error.HTTPError as e:
            raw_err = e.read().decode("utf-8", errors="replace")
            raise MassEngineError(e.code, raw_err.strip()) from None

    def create_tenant_key(self, tenant: str, key: str = "") -> str:
        """Creates or registers a tenant-scoped RBAC API key (requires admin key)."""
        resp = self._request_json(
            "POST",
            f"/v1/tenants/{self._esc(tenant)}/keys",
            {"key": key},
        )
        return str(resp["key"])

    def revoke_tenant_key(self, tenant: str, key: str) -> Dict[str, Any]:
        """Revokes a tenant-scoped RBAC API key (requires admin key)."""
        return self._request_json(
            "DELETE",
            f"/v1/tenants/{self._esc(tenant)}/keys",
            {"key": key},
        )

    def set_tenant_quota(
        self,
        tenant: str,
        quota: Union[TenantQuota, Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Sets a tenant's plan tier, max_docs cap, max_qps limit, and suspension status."""
        q_dict = quota.to_dict() if isinstance(quota, TenantQuota) else quota
        resp = self._request_json(
            "PUT",
            f"/v1/tenants/{self._esc(tenant)}/quota",
            q_dict,
        )
        return resp.get("quota", resp)

    def get_tenant_quota(self, tenant: str) -> Dict[str, Any]:
        """Retrieves the active quota configuration for `tenant`."""
        resp = self._request_json(
            "GET",
            f"/v1/tenants/{self._esc(tenant)}/quota",
        )
        return resp.get("quota", resp)

    def get_tenant_usage(self, tenant: str) -> Dict[str, Any]:
        """Returns cumulative billable read/write counters and live doc count for `tenant`."""
        return self._request_json(
            "GET",
            f"/v1/tenants/{self._esc(tenant)}/usage",
        )

    def list_usage(self) -> Dict[str, Any]:
        """Returns the cluster-wide billable usage report across all tenants (admin key)."""
        return self._request_json("GET", "/v1/usage")

    def list_tenants(self) -> List[Dict[str, Any]]:
        """Lists all known tenants sorted by name (admin key)."""
        resp = self._request_json("GET", "/v1/tenants")
        return resp.get("tenants", [])

    def get_tenant(self, tenant: str) -> Dict[str, Any]:
        """Returns routing and document count metadata for `tenant`."""
        return self._request_json("GET", f"/v1/tenants/{self._esc(tenant)}")

    def delete_tenant(self, tenant: str) -> int:
        """Atomically purges all documents and routing state for `tenant` (admin key)."""
        resp = self._request_json("DELETE", f"/v1/tenants/{self._esc(tenant)}")
        return int(resp.get("docs_deleted", 0))

    def flush(self) -> Dict[str, Any]:
        """Flushes all mutable shard arenas to disk segments (admin key)."""
        return self._request_json("POST", "/v1/flush")

    def compact(self) -> Dict[str, Any]:
        """Triggers online lock-free segment compaction across all shards (admin key)."""
        return self._request_json("POST", "/v1/compact")

    def ready(self) -> Dict[str, Any]:
        """Checks cluster shard readiness."""
        return self._request_json("GET", "/v1/ready")

    def health(self) -> Dict[str, Any]:
        """Checks gateway health."""
        return self._request_json("GET", "/v1/health")
