"""Official Python client SDK for MassEngine."""

from .client import (
    MassEngineClient,
    MassEngineError,
    Doc,
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
__version__ = "1.1.0"
