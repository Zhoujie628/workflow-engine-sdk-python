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

"""SSE response normalization for non-standard agent responses.

Some A2A agents return bare Task or Message objects instead of properly
wrapped StreamResponse envelopes.  This module patches
``google.protobuf.json_format.ParseDict`` to coerce such responses into the
expected StreamResponse shape, mirroring the orchestration center's
exec_engine.

Only ``ParseDict`` is patched.  ``json_format.Parse`` resolves ``ParseDict``
from its module globals at call time, so normalizing the dict entry point
already covers the text entry point -- and patching ``Parse`` as well would
override a caller's explicit ``ignore_unknown_fields`` argument instead of
honouring it.

Import this module once at startup; the patch is process-global.
"""

import json as _json
import google.protobuf.json_format as _json_format
from loguru import logger

_STREAM_RESPONSE_KEYS = frozenset({"task", "message", "statusUpdate", "artifactUpdate"})

_original_parse_dict = _json_format.ParseDict
_APPLIED = False


def _normalize_stream_response(data: dict) -> dict:
    """Coerce a non-SSE dict into a StreamResponse-shaped dict."""
    if _STREAM_RESPONSE_KEYS.intersection(data):
        return data
    if "id" in data and "status" in data:
        return {"task": data}
    if "artifact" in data and "taskId" in data:
        return {"artifactUpdate": data}
    if "status" in data and "taskId" in data:
        return {"statusUpdate": data}
    return data


def _parse_dict_with_unknown(js, message, ignore_unknown_fields=False, *args, **kwargs):
    """Normalize a bare Task/Message payload before delegating to ParseDict.

    Non-``StreamResponse`` targets keep the caller's exact behaviour.  A
    ``StreamResponse`` target always parses with ``ignore_unknown_fields=True``:
    the payload has already been recognized as non-standard, and a newer agent
    adding a field must not turn a working stream into a hard failure.
    """
    from a2a.types.a2a_pb2 import StreamResponse
    if not isinstance(message, StreamResponse):
        return _original_parse_dict(js, message, ignore_unknown_fields, *args, **kwargs)
    if isinstance(js, dict):
        from workflow_engine.client.remote_error import from_payload

        remote_error = from_payload(js)
        if remote_error is not None:
            raise remote_error
        if not _STREAM_RESPONSE_KEYS.intersection(js):
            logger.warning(
                f"[A2A] Non-SSE response from server: keys={sorted(js)[:8]}"
            )
            from workflow_engine.client.sensitive_data import redact

            logger.trace(
                f"[A2A] Non-SSE response body: "
                f"{redact(_json.dumps(js, ensure_ascii=False, default=str))[:2048]}"
            )
        js = _normalize_stream_response(js)
    return _original_parse_dict(js, message, True, *args, **kwargs)


def apply_sse_normalization():
    """Apply the global ``ParseDict`` patch (idempotent)."""
    global _APPLIED
    if _APPLIED:
        return
    _json_format.ParseDict = _parse_dict_with_unknown
    _APPLIED = True
