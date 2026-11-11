"""Official Python client SDK for MassEngine."""

from .client import (
    MassEngineClient,
    MassEngineError,
    Doc,
    SDK_VERSION,
    SearchRequest,
    TenantQuota,
)

__all__ = [
    "MassEngineClient",
    "MassEngineError",
    "Doc",
    "SearchRequest",
    "TenantQuota",
]
__version__ = SDK_VERSION
