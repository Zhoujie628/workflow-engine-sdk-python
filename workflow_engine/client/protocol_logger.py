# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0

"""Opt-in protocol-level request/response diagnostics.

Full payloads may contain customer data and are disabled unless
``WORKFLOW_ENGINE_PROTOCOL_LOGGING=true`` is set. Sensitive headers remain
redacted unless ``WORKFLOW_ENGINE_PROTOCOL_INCLUDE_SENSITIVE_HEADERS=true``
is also explicitly set.
"""

import json
import os
from typing import Any, Dict, Optional
from loguru import logger


_SENSITIVE_HEADER_PARTS = (
    "authorization", "token", "secret", "api-key", "apikey", "cookie"
)


def _enabled() -> bool:
    return os.getenv("WORKFLOW_ENGINE_PROTOCOL_LOGGING", "").lower() == "true"


def _format_header(name: str, value: Any) -> str:
    normalized = name.lower()
    include_sensitive = (
        os.getenv("WORKFLOW_ENGINE_PROTOCOL_INCLUDE_SENSITIVE_HEADERS", "").lower()
        == "true"
    )
    if not include_sensitive and any(
        part in normalized for part in _SENSITIVE_HEADER_PARTS
    ):
        return "***REDACTED***"
    return value if isinstance(value, str) else str(value)[:200]


def log_request(agent_name: str, endpoint: str,
                params: Any, headers: Optional[Dict[str, str]] = None) -> None:
    """Log an outgoing A2A request (headers + body)."""
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
        for k, v in sorted(headers.items()):
            header_lines.append(f"  {k}: {_format_header(k, v)}")
    header_str = "\n".join(header_lines) if header_lines else "  (none)"
    logger.debug(f">>> [{agent_name}] REQUEST to {endpoint}\n=== Headers ===\n{header_str}\n=== Body ===\n{body}")


def log_response(agent_name: str, event_type: str, body: str) -> None:
    """Log an incoming A2A response (event type + body)."""
    if _enabled():
        logger.debug(f"<<< [{agent_name}] RESPONSE [{event_type}]\n{body}")


def log_response_event(agent_name: str, event: Any) -> None:
    """Log a structured SSE response event (mirrors Java ProtocolLogger.logResponseEvent).

    Extracts the inner payload from TaskUpdateEvent / MessageEvent wrappers
    and logs the full JSON for protocol-level debugging.
    """
    if not _enabled():
        return
    try:
        event_type = type(event).__name__
        payload = _extract_payload(event)
        if payload is None:
            logger.debug(f"<<< [{agent_name}] RESPONSE [{event_type}]: (no serializable payload)")
            return
        if isinstance(payload, str):
            body = payload
        else:
            try:
                body = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
            except Exception:
                body = str(payload)
        logger.debug(f"<<< [{agent_name}] RESPONSE [{event_type}]\n{body}")
    except Exception as e:
        logger.warning(f"<<< [{agent_name}] Failed to serialize response event: {e}")


def _extract_payload(event: Any) -> Any:
    """Extract the serializable protocol payload from a ClientEvent."""
    if hasattr(event, "task"):
        task = event.task
        if hasattr(task, "status_updates") and task.status_updates:
            return task.status_updates[-1]
        if hasattr(task, "artifacts") and task.artifacts:
            return task.artifacts[-1]
        return task
    if hasattr(event, "message"):
        return event.message
    if hasattr(event, "update_event"):
        return event.update_event
    return event
