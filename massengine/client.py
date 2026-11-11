"""Zero-dependency Python HTTP/JSON client for MassEngine."""

from __future__ import annotations

from dataclasses import dataclass
import json
import ssl
from typing import Any, Dict, Iterable, List, Optional, Sequence, Union
import urllib.error
import urllib.parse
import urllib.request

SDK_VERSION = "1.1.4"
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


def _validate_segment(segment: str) -> str:
    if not isinstance(segment, str) or segment in ("", ".", ".."):
        raise ValueError("invalid URL path segment")
    if any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in segment):
        raise ValueError("invalid control character in URL path segment")
    return urllib.parse.quote(segment, safe="")


def _validate_identifier(value: str, label: str) -> str:
    if not isinstance(value, str) or "/" in value or "\\" in value:
        raise ValueError(f"invalid {label} identifier")
    return _validate_segment(value)


def _escape_doc_id(doc_id: str) -> str:
    if not isinstance(doc_id, str) or doc_id == "":
        raise ValueError("document identifier must be a non-empty string")
    if doc_id == "batch-get":
        raise ValueError("reserved document identifier")
    parts = doc_id.split("/")
    return "/".join(_validate_segment(part) for part in parts)


def _validate_vector(vector: Any) -> List[float]:
    if vector is None or isinstance(vector, (str, bytes, bytearray)):
        raise ValueError("vector must be a non-empty sequence of numbers")
    try:
        vals = list(vector)
    except TypeError:
        raise ValueError("vector must be a non-empty sequence of numbers") from None
    if len(vals) == 0:
        raise ValueError("vector must be a non-empty sequence of numbers")
    return vals


def _extract_error_message(raw_text: str) -> str:
    msg = raw_text.strip()
    try:
        parsed = json.loads(raw_text)
        if isinstance(parsed, dict) and parsed.get("error"):
            msg = str(parsed["error"])
    except Exception:
        pass
    return msg


@dataclass
class Doc:
    """Represents a vector document for ingestion or export."""

    id: str
    vector: Sequence[float]
    date: Optional[int] = None
    category: Optional[int] = None
    metadata: Optional[Dict[str, Any]] = None
    database: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        _escape_doc_id(self.id)
        out: Dict[str, Any] = {
            "id": self.id,
            "vector": list(self.vector),
        }
        if self.database:
            _validate_identifier(self.database, "database")
            out["database"] = self.database
        if self.date is not None:
            out["date"] = int(self.date)
        if self.category is not None:
            out["category"] = int(self.category)
        if self.metadata is not None:
            out["metadata"] = self.metadata
        return out


@dataclass
class SearchRequest:
    """Specifies a single filtered dense vector query."""

    tenant: str = ""
    k: int = 10
    vector: Sequence[float] = ()
    database: str = ""
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
        vec = _validate_vector(self.vector)
        out: Dict[str, Any] = {
            "vector": vec,
            "k": int(self.k),
        }
        if self.tenant != "":
            _validate_identifier(self.tenant, "tenant")
            out["tenant"] = self.tenant
        if self.database != "":
            _validate_identifier(self.database, "database")
            out["database"] = self.database
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
    max_bytes: int = 0
    max_qps: float = 0.0
    burst: int = 0
    suspended: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "plan": self.plan,
            "max_docs": int(self.max_docs),
            "max_bytes": int(self.max_bytes),
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
        return MassEngineError(e.code, _extract_error_message(raw_err))

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
                    raise MassEngineError(status, _extract_error_message(raw.decode("utf-8", errors="replace")))
                if not raw:
                    return None
                return json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as e:
            raise self._decode_error(e) from None

    @staticmethod
    def _esc(segment: str) -> str:
        return _validate_segment(segment)

    @classmethod
    def _esc_tenant(cls, tenant: str) -> str:
        return _validate_identifier(tenant, "tenant")

    @classmethod
    def _esc_database(cls, database: str) -> str:
        return _validate_identifier(database, "database")

    @classmethod
    def _esc_doc_id(cls, doc_id: str) -> str:
        return _escape_doc_id(doc_id)

    @classmethod
    def _validate_optional_tenant(cls, tenant: str) -> str:
        if not isinstance(tenant, str):
            raise ValueError("invalid tenant identifier")
        if tenant == "":
            return ""
        return cls._esc_tenant(tenant)

    @classmethod
    def _validate_optional_database(cls, database: str) -> str:
        if not isinstance(database, str):
            raise ValueError("invalid database identifier")
        if database == "":
            return ""
        cls._esc_database(database)
        return database

    @classmethod
    def _append_db_query(cls, base_path: str, database: str) -> str:
        db = cls._validate_optional_database(database)
        if not db:
            return base_path
        return f"{base_path}?database={urllib.parse.quote(db, safe='')}"

    @classmethod
    def _serialize_doc(cls, d: Union[Doc, Dict[str, Any]]) -> Dict[str, Any]:
        if isinstance(d, Doc):
            return d.to_dict()
        if not isinstance(d, dict):
            raise ValueError("document must be a Doc or dict")
        cls._esc_doc_id(d.get("id"))  # type: ignore[arg-type]
        if d.get("database"):
            cls._esc_database(d["database"])
        return d

    def ingest(
        self,
        tenant: str,
        docs: Sequence[Union[Doc, Dict[str, Any]]],
        *,
        database: str = "",
    ) -> Dict[str, Any]:
        """Inserts or upserts a batch of documents into `tenant` (or the bound `mk_...` key's tenant when empty)."""
        esc_tenant = self._validate_optional_tenant(tenant)
        db = self._validate_optional_database(database)
        serialized = [self._serialize_doc(d) for d in docs]
        path = "/v1/ingest" if esc_tenant == "" else f"/v1/tenants/{esc_tenant}/ingest"
        body: Dict[str, Any] = {"docs": serialized}
        if db:
            body["database"] = db
        return self._request_json("POST", path, body)

    def search(
        self,
        tenant: str = "",
        *,
        vector: Sequence[float],
        database: str = "",
        k: int = 10,
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
        """Executes a dense vector search query with optional metadata/date/category filters."""
        sr = SearchRequest(
            tenant=tenant,
            k=k,
            vector=vector,
            database=database,
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
        *,
        tenant: str = "",
        database: str = "",
    ) -> Dict[str, Any]:
        """Executes multiple dense vector search queries in a single round-trip."""
        if not queries:
            raise ValueError("queries must be non-empty")
        esc_tenant = self._validate_optional_tenant(tenant)
        db = self._validate_optional_database(database)
        serialized: List[Dict[str, Any]] = []
        for q in queries:
            if isinstance(q, SearchRequest):
                serialized.append(q.to_dict())
            elif isinstance(q, dict):
                vec = _validate_vector(q.get("vector"))
                q_copy = dict(q)
                q_copy["vector"] = vec
                if q_copy.get("tenant"):
                    self._esc_tenant(q_copy["tenant"])
                if q_copy.get("database"):
                    self._esc_database(q_copy["database"])
                serialized.append(q_copy)
            else:
                raise ValueError("each query must be a SearchRequest or dict")
        body: Dict[str, Any] = {"queries": serialized}
        if esc_tenant != "":
            body["tenant"] = tenant
        if db:
            body["database"] = db
        return self._request_json("POST", "/v1/search/batch", body)

    def _doc_path(self, tenant: str, doc_id: str, database: str = "") -> str:
        esc_tenant = self._validate_optional_tenant(tenant)
        esc_id = self._esc_doc_id(doc_id)
        base = f"/v1/docs/{esc_id}" if esc_tenant == "" else f"/v1/tenants/{esc_tenant}/docs/{esc_id}"
        return self._append_db_query(base, database)

    def get_doc(self, tenant: str, doc_id: str, *, database: str = "") -> Dict[str, Any]:
        """Fetches a document's metadata by ID, optionally scoped to `database`."""
        return self._request_json("GET", self._doc_path(tenant, doc_id, database))

    def patch_doc(
        self,
        tenant: str,
        doc_id: str,
        *,
        database: str = "",
        metadata: Optional[Dict[str, Any]] = None,
        date: Optional[int] = None,
        category: Optional[int] = None,
        replace_metadata: bool = False,
    ) -> Dict[str, Any]:
        """Updates a document's metadata, date, or category in place without re-embedding."""
        db = self._validate_optional_database(database)
        body: Dict[str, Any] = {}
        if db:
            body["database"] = db
        if metadata is not None:
            body["metadata"] = metadata
        if date is not None:
            body["date"] = int(date)
        if category is not None:
            body["category"] = int(category)
        if replace_metadata:
            body["replace_metadata"] = True
        return self._request_json("PATCH", self._doc_path(tenant, doc_id), body)

    def batch_get_docs(
        self,
        tenant: str,
        ids: Sequence[str],
        *,
        database: str = "",
    ) -> List[Dict[str, Any]]:
        """Fetches multiple documents by ID in a single request."""
        esc_tenant = self._validate_optional_tenant(tenant)
        db = self._validate_optional_database(database)
        id_list = list(ids)
        for doc_id in id_list:
            self._esc_doc_id(doc_id)
        path = "/v1/docs/batch-get" if esc_tenant == "" else f"/v1/tenants/{esc_tenant}/docs/batch-get"
        body: Dict[str, Any] = {"ids": id_list}
        if db:
            body["database"] = db
        resp = self._request_json("POST", path, body)
        return resp.get("docs", [])

    def delete_doc(self, tenant: str, doc_id: str, *, database: str = "") -> Dict[str, Any]:
        """Deletes a document by ID, optionally scoped to `database`."""
        return self._request_json("DELETE", self._doc_path(tenant, doc_id, database))

    def import_tenant(
        self,
        tenant: str,
        docs: Iterable[Union[Doc, Dict[str, Any]]],
        *,
        database: str = "",
    ) -> int:
        """Streams a collection of documents into `tenant` via NDJSON."""
        esc_tenant = self._validate_optional_tenant(tenant)
        base_path = "/v1/import" if esc_tenant == "" else f"/v1/tenants/{esc_tenant}/import"
        path = self._append_db_query(base_path, database)
        lines = [json.dumps(self._serialize_doc(d)) for d in docs]
        payload = ("\n".join(lines) + "\n").encode("utf-8")
        req = urllib.request.Request(
            url=f"{self.base_url}{path}",
            data=payload,
            headers=self._headers("application/x-ndjson"),
            method="POST",
        )
        try:
            with self._opener.open(req, timeout=self.timeout) as resp:
                status = int(getattr(resp, "status", 200) or 200)
                if status < 200 or status >= 300:
                    raw_err = resp.read(MAX_ERROR_RESPONSE_BYTES).decode("utf-8", errors="replace")
                    raise MassEngineError(status, _extract_error_message(raw_err))
                raw = self._read_bounded(resp)
                out = json.loads(raw.decode("utf-8"))
                return int(out.get("imported", 0))
        except urllib.error.HTTPError as e:
            raise self._decode_error(e) from None

    def export_tenant(self, tenant: str = "", *, database: str = "") -> List[Dict[str, Any]]:
        """Exports live documents belonging to `tenant` via streaming NDJSON."""
        esc_tenant = self._validate_optional_tenant(tenant)
        base_path = "/v1/export" if esc_tenant == "" else f"/v1/tenants/{esc_tenant}/export"
        path = self._append_db_query(base_path, database)
        req = urllib.request.Request(
            url=f"{self.base_url}{path}",
            headers=self._headers(None),
            method="GET",
        )
        try:
            with self._opener.open(req, timeout=self.timeout) as resp:
                status = int(getattr(resp, "status", 200) or 200)
                if status < 200 or status >= 300:
                    raw_err = resp.read(MAX_ERROR_RESPONSE_BYTES).decode("utf-8", errors="replace")
                    raise MassEngineError(status, _extract_error_message(raw_err))
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

    def list_databases(self, tenant: str = "") -> List[Dict[str, Any]]:
        """Lists all databases configured under `tenant` (or the bound `mk_...` key's tenant)."""
        esc_tenant = self._validate_optional_tenant(tenant)
        path = "/v1/databases" if esc_tenant == "" else f"/v1/tenants/{esc_tenant}/databases"
        resp = self._request_json("GET", path)
        if isinstance(resp, dict):
            return resp.get("databases") or resp.get("partitions") or []
        return []

    def create_database(
        self,
        tenant: str,
        database: str,
        *,
        num_shards: int = 0,
        rf: int = 0,
        data_type: str = "",
        mode: str = "",
    ) -> Dict[str, Any]:
        """Creates or updates a named database under `tenant` (or the bound `mk_...` key's tenant)."""
        esc_tenant = self._validate_optional_tenant(tenant)
        self._esc_database(database)
        path = "/v1/databases" if esc_tenant == "" else f"/v1/tenants/{esc_tenant}/databases"
        body: Dict[str, Any] = {"database": database}
        if num_shards > 0:
            body["num_shards"] = int(num_shards)
        if rf > 0:
            body["rf"] = int(rf)
        if data_type:
            body["data_type"] = data_type
        if mode:
            body["mode"] = mode
        resp = self._request_json("POST", path, body)
        if isinstance(resp, dict) and isinstance(resp.get("database"), dict):
            db_obj = dict(resp["database"])
            if not db_obj.get("database"):
                db_obj["database"] = resp.get("partition") or database
            return db_obj
        return resp

    def get_database(self, tenant: str, database: str) -> Dict[str, Any]:
        """Fetches metadata and volume statistics for a single named database."""
        esc_tenant = self._validate_optional_tenant(tenant)
        esc_db = self._esc_database(database)
        path = f"/v1/databases/{esc_db}" if esc_tenant == "" else f"/v1/tenants/{esc_tenant}/databases/{esc_db}"
        try:
            resp = self._request_json("GET", path)
        except MassEngineError as err:
            if err.status_code != 405:
                raise
            for item in self.list_databases(tenant):
                if item.get("database") == database or item.get("partition") == database:
                    return item
            raise MassEngineError(404, "database not found") from None

        if isinstance(resp, dict) and isinstance(resp.get("database"), dict):
            out = dict(resp["database"])
        elif isinstance(resp, dict):
            out = dict(resp)
        else:
            out = {}
        if not out.get("database"):
            out["database"] = database
        return out

    def delete_database(self, tenant: str, database: str) -> int:
        """Deletes all documents and state for a named database."""
        esc_tenant = self._validate_optional_tenant(tenant)
        esc_db = self._esc_database(database)
        path = f"/v1/databases/{esc_db}" if esc_tenant == "" else f"/v1/tenants/{esc_tenant}/databases/{esc_db}"
        resp = self._request_json("DELETE", path)
        return int(resp.get("docs_deleted", 0))

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
        """Sets a tenant's plan tier, max_docs/max_bytes caps, max_qps limit, and suspension status."""
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
        """Returns cumulative billable read/write counters, byte footprint, and latency stats for `tenant`."""
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

    def get_tenant(self, tenant: str, *, database: str = "") -> Dict[str, Any]:
        """Returns document and storage volume metadata for `tenant` (or optional `database`)."""
        path = self._append_db_query(f"/v1/tenants/{self._esc_tenant(tenant)}", database)
        return self._request_json("GET", path)

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
        """Checks service readiness, returning ReadyStatus for both HTTP 200 and 503 JSON responses."""
        req = urllib.request.Request(
            url=f"{self.base_url}/v1/ready",
            headers=self._headers(None),
            method="GET",
        )

        def _parse_ready(status: int, raw: bytes) -> Dict[str, Any]:
            text = raw.decode("utf-8", errors="replace")
            try:
                parsed = json.loads(text)
            except Exception:
                raise MassEngineError(status, _extract_error_message(text)) from None
            if not isinstance(parsed, dict) or not isinstance(parsed.get("ready"), bool):
                raise MassEngineError(status, _extract_error_message(text))
            shards_raw = parsed.get("unavailable_shards")
            parsed["unavailable_shards"] = [int(s) for s in shards_raw] if shards_raw else []
            return parsed

        try:
            with self._opener.open(req, timeout=self.timeout) as resp:
                status = int(getattr(resp, "status", 200) or 200)
                raw = self._read_bounded(resp)
                if (200 <= status < 300) or status == 503:
                    return _parse_ready(status, raw)
                raise MassEngineError(status, _extract_error_message(raw.decode("utf-8", errors="replace")))
        except urllib.error.HTTPError as e:
            if e.code == 503:
                try:
                    raw = e.read(self.max_response_bytes + 1)
                finally:
                    try:
                        e.close()
                    except Exception:
                        pass
                if len(raw) > self.max_response_bytes:
                    raise MassEngineError(
                        503,
                        f"response body exceeds max_response_bytes ({self.max_response_bytes})",
                    ) from None
                return _parse_ready(503, raw)
            raise self._decode_error(e) from None

    def health(self) -> Dict[str, Any]:
        """Checks gateway health."""
        return self._request_json("GET", "/v1/health")
