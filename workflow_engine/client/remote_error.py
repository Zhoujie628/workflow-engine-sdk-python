# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""Stable projection of standard A2A error envelopes across HTTP and SSE."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any

import httpx

from workflow_engine.client.sensitive_data import redact


class EmptyA2AStreamError(RuntimeError):
    """The transport completed without a single protocol event."""


@dataclass(frozen=True)
class RemoteA2AError(Exception):
    http_status: int
    code: int
    message: str
    status: str = ""
    reason: str = ""
    domain: str = ""
    details: tuple[dict[str, Any], ...] = field(default_factory=tuple)
    retry_after: str = ""

    @property
    def workflow_code(self) -> str:
        reason = re.sub(r"[^a-z0-9]+", "_", self.reason.lower()).strip("_")
        return f"a2a.{reason}" if reason else f"a2a.http.{self.http_status}"

    @property
    def error_details(self) -> dict[str, Any]:
        result: dict[str, Any] = {"httpStatus": self.http_status, "code": self.code}
        for key, value in (
            ("status", self.status), ("reason", self.reason),
            ("domain", self.domain), ("retryAfter", self.retry_after),
        ):
            if value:
                result[key] = value
        if self.details:
            result["details"] = list(self.details)
        return result

    def __str__(self) -> str:
        return self.message


def from_payload(
    payload: str | dict[str, Any] | None,
    *,
    observed_http_status: int = 0,
    retry_after: str = "",
) -> RemoteA2AError | None:
    try:
        value = json.loads(payload) if isinstance(payload, str) else payload
    except ValueError:
        return None
    if not isinstance(value, dict) or not isinstance(value.get("error"), dict):
        return None
    error = value["error"]
    code = error.get("code")
    message = error.get("message")
    if type(code) is not int or not 400 <= code <= 599 or not isinstance(message, str) or not message.strip():
        return None
    status = observed_http_status if 400 <= observed_http_status <= 599 else code
    raw_details = error.get("details")
    details = tuple(
        json.loads(redact(json.dumps(item, ensure_ascii=False)))
        for item in raw_details
        if isinstance(item, dict)
    ) if isinstance(raw_details, list) else ()
    reason = ""
    domain = ""
    for detail in details:
        if isinstance(detail.get("reason"), str):
            reason = detail["reason"]
            domain = detail.get("domain", "") if isinstance(detail.get("domain"), str) else ""
            break
    return RemoteA2AError(
        http_status=status,
        code=code,
        message=redact(message),
        status=redact(error.get("status", "")) if isinstance(error.get("status"), str) else "",
        reason=reason,
        domain=domain,
        details=details,
        retry_after=redact(retry_after),
    )


def find_in_exception(error: BaseException) -> RemoteA2AError | None:
    """Recover a structured error from SDK wrappers without exposing raw bodies."""
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, RemoteA2AError):
            return current
        if isinstance(current, httpx.HTTPStatusError):
            response = current.response
            try:
                parsed = from_payload(
                    response.text,
                    observed_http_status=response.status_code,
                    retry_after=response.headers.get("Retry-After", ""),
                )
            except httpx.ResponseNotRead:
                parsed = None
            if parsed:
                return parsed
            if 400 <= response.status_code <= 599:
                return RemoteA2AError(
                    response.status_code, response.status_code,
                    f"A2A request failed with HTTP {response.status_code}",
                    retry_after=redact(response.headers.get("Retry-After", "")),
                )
        message = str(current)
        for prefix in ("SSE stream error event received: ", "SSE stream error: "):
            if message.startswith(prefix):
                parsed = from_payload(message[len(prefix):])
                if parsed:
                    return parsed
        current = current.__cause__ or current.__context__
    return None
