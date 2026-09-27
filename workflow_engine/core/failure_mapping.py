# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0

"""Map failures to stable workflow-facing diagnostics."""

from __future__ import annotations

import asyncio
import json

from workflow_engine.client.remote_error import EmptyA2AStreamError, find_in_exception
from workflow_engine.client.sensitive_data import redact
from workflow_engine.core.models import BusinessFailure, TaskResult


def failure_to_task_result(error: BaseException) -> TaskResult:
    current = error
    seen = set()
    while current.__cause__ is not None and id(current) not in seen:
        if isinstance(current, BusinessFailure):
            break
        seen.add(id(current))
        current = current.__cause__

    if isinstance(current, BusinessFailure):
        return TaskResult(
            success=False, error_code=current.code, error=str(current),
            error_details=current.details,
        )

    if isinstance(current, EmptyA2AStreamError):
        return TaskResult.failed("a2a.empty_stream", str(current))

    remote = find_in_exception(error)
    if remote is not None:
        return TaskResult(
            success=False,
            error_code=remote.workflow_code,
            error=remote.message,
            error_details=remote.error_details,
        )

    try:
        from a2a.utils.errors import A2AError, A2A_ERROR_MAPPING

        if isinstance(current, A2AError):
            # Walk the MRO: the mapping is keyed by exact type, so a subclass
            # raised by the SDK would otherwise fall through to the generic code.
            mapping = None
            for klass in type(current).__mro__:
                mapping = A2A_ERROR_MAPPING.get(klass)
                if mapping is not None:
                    break
            reason = mapping.reason if mapping else ""
            http_status = mapping.http_code if mapping else 500
            code = f"a2a.{reason.lower()}" if reason else f"a2a.http.{http_status}"
            metadata = dict(getattr(current, "data", None) or {})
            safe_metadata = json.loads(redact(json.dumps(metadata, ensure_ascii=False, default=str)))
            detail = {
                "@type": "type.googleapis.com/google.rpc.ErrorInfo",
                "domain": "a2a-protocol.org",
            }
            if reason:
                detail["reason"] = reason
            if safe_metadata:
                detail["metadata"] = safe_metadata
            details = {"httpStatus": http_status, "code": http_status}
            if mapping:
                details.update({
                    "status": mapping.grpc_status,
                    "reason": mapping.reason,
                    "domain": "a2a-protocol.org",
                })
            details["details"] = [detail]
            return TaskResult(
                success=False, error_code=code,
                error=redact(getattr(current, "message", type(current).__name__)),
                error_details=details,
            )
    except ImportError:
        pass

    if isinstance(current, (TimeoutError, asyncio.TimeoutError)):
        code = "workflow.timeout"
    elif isinstance(current, asyncio.CancelledError):
        code = "workflow.cancelled"
    else:
        code = "workflow.execution_failed"
    return TaskResult.failed(code, redact(str(current)) or type(current).__name__)
