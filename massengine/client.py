"""Zero-dependency Python HTTP/JSON client for MassEngine."""

from __future__ import annotations

from dataclasses import dataclass
import json
import ssl
from typing import Any, Dict, Iterable, List, Optional, Sequence, Union
import urllib.error
import urllib.parse
import urllib.request

SDK_VERSION = "1.1.2"
USER_AGENT = f"massengine-python/{SDK_VERSION}"
DEFAULT_MAX_RESPONSE_BYTES = 64 * 1024 * 1024  # 64 MiB
MAX_ERROR_RESPONSE_BYTES = 65536               # 64 KiB
_MAX_LINE_READ_CHUNK = 4 * 1024 * 1024


class MassEngineError(Exception):
    """Raised when the MassEngine gateway returns a non-2xx HTTP status."""

    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(f"massengine api (status {status_code}): {message}")
        self.status_code = status_code
        self.message = message


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Prevents urllib from following HTTP redirects and leaking Authorization headers."""

    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> None:
        return None


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
        max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES,
    ) -> None:
        self.base_url = self._validate_base_url(base_url)
        self._api_key = self._validate_api_key(api_key)
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or timeout <= 0:
            raise ValueError("timeout must be a positive number")
        if isinstance(max_response_bytes, bool) or not isinstance(max_response_bytes, int) or max_response_bytes <= 0:
            raise ValueError("max_response_bytes must be a positive integer")
        self.timeout = float(timeout)
        self.max_response_bytes = max_response_bytes

        ssl_ctx = ssl.create_default_context()
        ssl_ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        self._opener = urllib.request.build_opener(
            _NoRedirectHandler(),
            urllib.request.HTTPSHandler(context=ssl_ctx),
        )

    def __repr__(self) -> str:
        masked = "***" if self._api_key else None
        return f"MassEngineClient(base_url={self.base_url!r}, api_key={masked!r})"

    @staticmethod
    def _validate_base_url(base_url: str) -> str:
        if not isinstance(base_url, str) or not base_url.strip():
            raise ValueError("base_url must be a valid http or https URL")
        cleaned = base_url.strip().rstrip("/")
        if any(ord(ch) <= 0x20 or ord(ch) == 0x7F for ch in cleaned):
            raise ValueError("base_url contains invalid characters")
        parsed = urllib.parse.urlsplit(cleaned)
        if (
            parsed.scheme not in ("http", "https")
            or not parsed.netloc
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("base_url must use http or https scheme with a valid host")
        return cleaned

    @staticmethod
    def _validate_api_key(api_key: Optional[str]) -> Optional[str]:
        if api_key is None or api_key == "":
            return None
        if not isinstance(api_key, str):
            raise ValueError("invalid api_key format")
        trimmed = api_key.strip(" \t")
        if not trimmed or not all(0x21 <= ord(ch) <= 0x7E for ch in trimmed):
            raise ValueError("invalid api_key format")
        return trimmed

    def _headers(self, content_type: Optional[str] = "application/json") -> Dict[str, str]:
        hdrs: Dict[str, str] = {"User-Agent": USER_AGENT}
        if content_type:
            hdrs["Content-Type"] = content_type
        if self._api_key:
            hdrs["Authorization"] = f"Bearer {self._api_key}"
        return hdrs

    def _read_bounded(self, resp: Any) -> bytes:
        raw: bytes = resp.read(self.max_response_bytes + 1)
        if len(raw) > self.max_response_bytes:
            status = int(getattr(resp, "status", 200) or 200)
            raise MassEngineError(
                status,
                f"response body exceeds max_response_bytes ({self.max_response_bytes})",
            )
        return raw

    @staticmethod
    def _decode_error(e: urllib.error.HTTPError) -> MassEngineError:
        try:
            raw_err = e.read(MAX_ERROR_RESPONSE_BYTES).decode("utf-8", errors="replace")
        finally:
            try:
                e.close()
            except Exception:
                pass
        msg = raw_err.strip()
        try:
            parsed = json.loads(raw_err)
            if isinstance(parsed, dict) and parsed.get("error"):
                msg = str(parsed["error"])
        except Exception:
            pass
        return MassEngineError(e.code, msg)

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
            with self._opener.open(req, timeout=self.timeout) as resp:
                status = int(getattr(resp, "status", 200) or 200)
                raw = self._read_bounded(resp)
                if status < 200 or status >= 300:
                    raise MassEngineError(status, raw.decode("utf-8", errors="replace").strip())
                if not raw:
                    return None
                return json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as e:
            raise self._decode_error(e) from None

    @staticmethod
    def _esc(segment: str) -> str:
        if not isinstance(segment, str) or segment in ("", ".", ".."):
            raise ValueError("invalid URL path segment")
        if any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in segment):
            raise ValueError("invalid control character in URL path segment")
        return urllib.parse.quote(segment, safe="")

    @classmethod
    def _esc_tenant(cls, tenant: str) -> str:
        if not isinstance(tenant, str) or "/" in tenant or "\\" in tenant:
            raise ValueError("invalid tenant identifier")
        return cls._esc(tenant)

    @classmethod
    def _esc_doc_id(cls, doc_id: str) -> str:
        if doc_id == "batch-get":
            raise ValueError("reserved document identifier")
        return cls._esc(doc_id)

    def ingest(
        self,
        tenant: str,
        docs: Sequence[Union[Doc, Dict[str, Any]]],
    ) -> Dict[str, Any]:
        """Inserts or upserts a batch of documents into `tenant`."""
        serialized = [d.to_dict() if isinstance(d, Doc) else d for d in docs]
        return self._request_json(
            "POST",
            f"/v1/tenants/{self._esc_tenant(tenant)}/ingest",
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
        """Fetches a document's metadata by ID."""
        return self._request_json(
            "GET",
            f"/v1/tenants/{self._esc_tenant(tenant)}/docs/{self._esc_doc_id(doc_id)}",
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
            f"/v1/tenants/{self._esc_tenant(tenant)}/docs/{self._esc_doc_id(doc_id)}",
            body,
        )

    def batch_get_docs(self, tenant: str, ids: Sequence[str]) -> List[Dict[str, Any]]:
        """Fetches multiple documents by ID in a single request."""
        resp = self._request_json(
            "POST",
            f"/v1/tenants/{self._esc_tenant(tenant)}/docs/batch-get",
            {"ids": list(ids)},
        )
        return resp.get("docs", [])

    def delete_doc(self, tenant: str, doc_id: str) -> Dict[str, Any]:
        """Deletes a document by ID."""
        return self._request_json(
            "DELETE",
            f"/v1/tenants/{self._esc_tenant(tenant)}/docs/{self._esc_doc_id(doc_id)}",
        )

    def import_tenant(
        self,
        tenant: str,
        docs: Iterable[Union[Doc, Dict[str, Any]]],
    ) -> int:
        """Streams a collection of documents into `tenant` via NDJSON."""
        esc_tenant = self._esc_tenant(tenant)
        lines = [
            json.dumps(d.to_dict() if isinstance(d, Doc) else d) for d in docs
        ]
        payload = ("\n".join(lines) + "\n").encode("utf-8")
        req = urllib.request.Request(
            url=f"{self.base_url}/v1/tenants/{esc_tenant}/import",
            data=payload,
            headers=self._headers("application/x-ndjson"),
            method="POST",
        )
        try:
            with self._opener.open(req, timeout=self.timeout) as resp:
                raw = self._read_bounded(resp)
                out = json.loads(raw.decode("utf-8"))
                return int(out.get("imported", 0))
        except urllib.error.HTTPError as e:
            raise self._decode_error(e) from None

    def export_tenant(self, tenant: str) -> List[Dict[str, Any]]:
        """Exports all live documents belonging to `tenant` via streaming NDJSON."""
        esc_tenant = self._esc_tenant(tenant)
        req = urllib.request.Request(
            url=f"{self.base_url}/v1/tenants/{esc_tenant}/export",
            headers=self._headers(None),
            method="GET",
        )
        try:
            with self._opener.open(req, timeout=self.timeout) as resp:
                status = int(getattr(resp, "status", 200) or 200)
                docs: List[Dict[str, Any]] = []
                total_bytes = 0
                buf = b""
                while True:
                    remaining = self.max_response_bytes - total_bytes + 1
                    read_limit = min(remaining, _MAX_LINE_READ_CHUNK)
                    chunk: bytes = resp.readline(max(1, read_limit))
                    if not chunk:
                        break
                    total_bytes += len(chunk)
                    if total_bytes > self.max_response_bytes:
                        raise MassEngineError(
                            status,
                            f"response body exceeds max_response_bytes ({self.max_response_bytes})",
                        )
                    buf += chunk
                    if buf.endswith(b"\n"):
                        line = buf.decode("utf-8").strip()
                        buf = b""
                        if line:
                            docs.append(json.loads(line))
                if buf:
                    line = buf.decode("utf-8").strip()
                    if line:
                        docs.append(json.loads(line))
                return docs
        except urllib.error.HTTPError as e:
            raise self._decode_error(e) from None

    def create_tenant_key(self, tenant: str, key: str = "") -> str:
        """Creates or registers a tenant-scoped RBAC API key (requires admin key)."""
        resp = self._request_json(
            "POST",
            f"/v1/tenants/{self._esc_tenant(tenant)}/keys",
            {"key": key},
        )
        return str(resp["key"])

    def revoke_tenant_key(self, tenant: str, key: str) -> Dict[str, Any]:
        """Revokes a tenant-scoped RBAC API key (requires admin key)."""
        return self._request_json(
            "DELETE",
            f"/v1/tenants/{self._esc_tenant(tenant)}/keys",
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
            f"/v1/tenants/{self._esc_tenant(tenant)}/quota",
            q_dict,
        )
        return resp.get("quota", resp)

    def get_tenant_quota(self, tenant: str) -> Dict[str, Any]:
        """Retrieves the active quota configuration for `tenant`."""
        resp = self._request_json(
            "GET",
            f"/v1/tenants/{self._esc_tenant(tenant)}/quota",
        )
        return resp.get("quota", resp)

    def get_tenant_usage(self, tenant: str) -> Dict[str, Any]:
        """Returns cumulative billable read/write counters and live doc count for `tenant`."""
        return self._request_json(
            "GET",
            f"/v1/tenants/{self._esc_tenant(tenant)}/usage",
        )

    def list_usage(self) -> Dict[str, Any]:
        """Returns the cluster-wide billable usage report across all tenants (admin key)."""
        return self._request_json("GET", "/v1/usage")

    def list_tenants(self) -> List[Dict[str, Any]]:
        """Lists all known tenants sorted by name (admin key)."""
        resp = self._request_json("GET", "/v1/tenants")
        return resp.get("tenants", [])

    def get_tenant(self, tenant: str) -> Dict[str, Any]:
        """Returns document and storage volume metadata for `tenant`."""
        return self._request_json("GET", f"/v1/tenants/{self._esc_tenant(tenant)}")

    def delete_tenant(self, tenant: str) -> int:
        """Deletes all documents and state for `tenant` (admin key)."""
        resp = self._request_json("DELETE", f"/v1/tenants/{self._esc_tenant(tenant)}")
        return int(resp.get("docs_deleted", 0))

    def flush(self) -> Dict[str, Any]:
        """Persists buffered writes to durable storage (admin key)."""
        return self._request_json("POST", "/v1/flush")

    def compact(self) -> Dict[str, Any]:
        """Optimizes storage segments (admin key)."""
        return self._request_json("POST", "/v1/compact")

    def ready(self) -> Dict[str, Any]:
        """Checks service readiness."""
        return self._request_json("GET", "/v1/ready")

    def health(self) -> Dict[str, Any]:
        """Checks gateway health."""
        return self._request_json("GET", "/v1/health")
