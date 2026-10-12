# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0

"""Per-call transport activity observation for long-lived streams.

Mirrors the Java engine's ``TransportActivityMonitor``. An activity listener is
bound for the duration of one protocol call, and the HTTP layer invokes it for
every body chunk it actually reads -- including an SSE comment heartbeat, which
never reaches the protocol layer as a decoded event.

The binding is a :class:`contextvars.ContextVar` rather than a ``ThreadLocal``
because that is the asyncio equivalent: a task inherits its creator's context, so
the response hook installed on the transport's httpx client attributes activity
to the call that opened the stream. Each subscription has its own A2A client;
separate Authorization-T and Notification-T transports own separate httpx clients.

Why this matters: a notification stream that is merely quiet looks identical to a
dead one if liveness is judged only from decoded events. Judging it from observed
transport activity is what lets an idle subscription be reaped without tearing
down a healthy one that is receiving heartbeats.
"""

from __future__ import annotations

import contextlib
import contextvars
from typing import Callable, Iterator, Optional

import httpx
from loguru import logger


ActivityListener = Callable[[], None]

_ACTIVITY_LISTENER: contextvars.ContextVar[Optional[ActivityListener]] = (
    contextvars.ContextVar("workflow_engine_transport_activity", default=None)
)

_HOOK_MARKER = "_workflow_engine_transport_activity_hook"


@contextlib.contextmanager
def bind_activity_listener(listener: Optional[ActivityListener]) -> Iterator[None]:
    """Bind ``listener`` for the duration of one protocol call."""
    token = _ACTIVITY_LISTENER.set(listener)
    try:
        yield
    finally:
        _ACTIVITY_LISTENER.reset(token)


def current_activity_listener() -> Optional[ActivityListener]:
    """Return the listener bound to the current call, if any."""
    return _ACTIVITY_LISTENER.get()


def _notify(listener: Optional[ActivityListener]) -> None:
    if listener is None:
        return
    try:
        listener()
    except Exception as exc:  # never let observation break the stream
        logger.warning(f"Transport activity callback failed: {exc}")


class _ObservingStream(httpx.AsyncByteStream):
    """Forwards body chunks while reporting each one as transport activity."""

    def __init__(self, inner: httpx.AsyncByteStream, listener: ActivityListener):
        self._inner = inner
        self._listener = listener

    async def __aiter__(self):
        async for chunk in self._inner:
            _notify(self._listener)
            yield chunk

    async def aclose(self) -> None:
        await self._inner.aclose()


async def _observe_response(response: httpx.Response) -> None:
    """Response hook: capture the current listener and wrap the unread body.

    httpx runs response hooks after the headers arrive and before the body is
    read, so the stream can still be replaced here.
    """
    listener = _ACTIVITY_LISTENER.get()
    if listener is None:
        return
    stream = response.stream
    if isinstance(stream, httpx.AsyncByteStream):
        response.stream = _ObservingStream(stream, listener)
    # A response that carries no body is still activity: the round trip completed.
    _notify(listener)


setattr(_observe_response, _HOOK_MARKER, True)


def install_activity_hook(client: httpx.AsyncClient) -> bool:
    """Install the observing response hook on ``client`` (idempotent).

    Returns True when the hook was newly installed. The hook is a no-op unless a
    listener is bound, so installing it on a caller-provided client does not
    change behaviour for traffic that never opens a notification stream.
    """
    response_hooks = client.event_hooks.setdefault("response", [])
    if any(getattr(hook, _HOOK_MARKER, False) for hook in response_hooks):
        return False
    response_hooks.append(_observe_response)
    return True


__all__ = [
    "ActivityListener",
    "bind_activity_listener",
    "current_activity_listener",
    "install_activity_hook",
]
