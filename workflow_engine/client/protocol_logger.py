# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0

"""Opt-in protocol-level request/response diagnostics.

Full payloads may contain customer data and are disabled unless
``WORKFLOW_ENGINE_PROTOCOL_LOGGING=true`` is set. Sensitive headers remain
redacted unless ``WORKFLOW_ENGINE_PROTOCOL_INCLUDE_SENSITIVE_HEADERS=true``
is also explicitly set.

Redaction combines a name pattern with an explicit registry: authentication
interceptors call :func:`register_sensitive_header` for every header name they
actually inject. A pattern alone is not enough, because the credential
configuration accepts an arbitrary ``auth_header`` name and a custom name such
as ``X-Corp-Cred`` matches none of the built-in patterns.

Header names are matched after stripping ``-`` and ``_``, so spelling variants
of the same credential header (``api_key``, ``api-key``, ``X-API-KEY``,
``ApiKey``) are all recognised. Matching the raw name alone silently missed
``api_key``, ``access_session``, ``pwd`` and ``passwd``.
"""

import json
import os
from typing import Any, Dict, Optional

from loguru import logger

from workflow_engine.client.sensitive_data import redact


# Substring patterns, matched against the separator-stripped lower-case name.
_SENSITIVE_HEADER_PARTS = (
    "authorization", "token", "secret", "password", "cookie", "apikey",
)

# Exact names, matched against the separator-stripped lower-case name.
_SENSITIVE_HEADER_NAMES = frozenset({"pwd", "passwd", "accesssession"})

# Header names declared as credential-bearing by the interceptors that inject them.
_SENSITIVE_HEADERS: set[str] = set()


def _normalize_header_name(name: str) -> str:
    """Lower-case and strip separators so spelling variants collapse together."""
    return name.strip().lower().replace("-", "").replace("_", "")


def register_sensitive_header(name: str) -> None:
    """Declare one header name as carrying a credential (idempotent)."""
    if name and isinstance(name, str):
        _SENSITIVE_HEADERS.add(_normalize_header_name(name))


def _is_sensitive(name: str) -> bool:
    if not name or not isinstance(name, str):
        return False
    key = name.strip().lower()
    if "://" in key:
        # A URL is not a credential header name; redacting it would hide the
        # endpoint being called, which is the point of the log.
        return False
    normalized = _normalize_header_name(name)
    return (
        normalized in _SENSITIVE_HEADERS
        or normalized in _SENSITIVE_HEADER_NAMES
        or any(part in normalized for part in _SENSITIVE_HEADER_PARTS)
    )


def _enabled() -> bool:
    return os.getenv("WORKFLOW_ENGINE_PROTOCOL_LOGGING", "").lower() == "true"


def _format_header(name: str, value: Any) -> str:
    include_sensitive = (
        os.getenv("WORKFLOW_ENGINE_PROTOCOL_INCLUDE_SENSITIVE_HEADERS", "").lower()
        == "true"
    )
    if not include_sensitive and _is_sensitive(name):
        return "***REDACTED***"
    return value if isinstance(value, str) else str(value)[:200]


def log_request(
    agent_name: str,
    endpoint: str,
    params: Any,
    headers: Optional[Dict[str, str]] = None,
) -> None:
    """Log an outgoing A2A request after client interceptors have run."""
    if not _enabled():
        return
    if isinstance(params, str):
        body = params
    else:
        try:
            body = json.dumps(params, ensure_ascii=False, indent=2, default=str)
        except Exception:
            body = str(params)
    header_lines = []
    if headers:
        for name, value in sorted(headers.items()):
            header_lines.append(f"  {name}: {_format_header(name, value)}")
    header_text = "\n".join(header_lines) if header_lines else "  (none)"
    logger.debug(
        f">>> [{agent_name}] REQUEST to {endpoint}\n"
        f"=== Headers ===\n{header_text}\n=== Body ===\n{redact(body)}"
    )


def log_response(agent_name: str, event_type: str, body: str) -> None:
    """Log an incoming A2A response event."""
    if _enabled():
        logger.debug(f"<<< [{agent_name}] RESPONSE [{event_type}]\n{redact(body)}")


__all__ = ["log_request", "log_response", "register_sensitive_header"]
