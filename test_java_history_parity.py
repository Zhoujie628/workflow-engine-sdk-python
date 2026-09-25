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

"""Regression tests derived from the Java engine's fix history.

The Java engine's commit log records defects it already repaired. Each test here
locks down one of those defects on the Python side, so the two engines cannot
drift back apart. The mapping is recorded in the capability-alignment audit.
"""

import asyncio

import httpx
import pytest
from a2a.types import (
    AgentCapabilities,
    AgentCard,
    AgentExtension,
    AgentInterface,
    Part,
)
from a2a_t.core import MetadataContent

from workflow_engine import A2ATExtension, A2atMessages, MessageContent
from workflow_engine.client.a2a_transport import A2ATransport
from workflow_engine.client.extension_sender import ExtensionSender
from workflow_engine.client.stub_engine_client import StubWorkflowEngineClient
from workflow_engine.client.transport_activity import (
    bind_activity_listener,
    current_activity_listener,
    install_activity_hook,
)
from workflow_engine.control.control_points import ControlPoint
from workflow_engine.core.executor import WorkflowExecutor
from workflow_engine.core.models import (
    JumpCondition,
    StepType,
    Task,
    Workflow,
    WorkflowStep,
)


class _OrderControlPoint(ControlPoint):
    """Records the order in which steps actually dispatch."""

    def __init__(self):
        self.order = []

    async def on_task(self, request):
        self.order.append(request.step_name)
        return MessageContent.text(f"out:{request.step_name}")


def _step(name, targets, layer):
    return WorkflowStep(
        name=name,
        step_type=StepType.ALL_SUCCESS,
        subtasks=[Task(agent=f"agent_{name}", description=f"t_{name}")],
        next=[JumpCondition(step=target, condition="") for target in targets],
        layer=layer,
    )


def _late_branch_workflow() -> Workflow:
    """A merge whose two direct predecessors become ready in different batches.

    a -> b, c ; b -> d, q ; c -> d ; d -> s ; s -> p ; q -> m ; p -> m

    ``m`` merges ``p`` and ``q``. ``q`` is ready one batch before ``p``, because
    ``p`` waits on ``s`` which waits on ``d``. Judging readiness from *direct*
    predecessors alone hides ``p`` (not yet activated) and lets ``m`` run early.
    """
    return Workflow(name="late_branch_join", steps=[
        _step("a", ["b", "c"], 0),
        _step("b", ["d", "q"], 1),
        _step("c", ["d"], 1),
        _step("d", ["s"], 2),
        _step("s", ["p"], 3),
        _step("q", ["m"], 2),
        _step("p", ["m"], 4),
        _step("m", ["endNode"], 5),
    ])


@pytest.mark.asyncio
async def test_merge_waits_for_every_ancestor_not_just_direct_predecessors():
    """Java 366f221: a merge must not run while an ancestor branch is still live.

    Python judged readiness from direct predecessors, so a predecessor that had
    not been activated yet was filtered out entirely and the merge ran with only
    the other branch's output -- silently, with ``success=True``.
    """
    control = _OrderControlPoint()
    executor = WorkflowExecutor(
        workflow=_late_branch_workflow(),
        control_point=control,
        engine_client=StubWorkflowEngineClient(),
    )

    result = await executor.run()

    assert result.success, f"Workflow failed: {result.error}"
    assert set(control.order) == {"a", "b", "c", "d", "s", "q", "p", "m"}
    assert control.order.index("m") > control.order.index("p"), (
        f"merge ran before its own predecessor: {control.order}"
    )
    assert control.order.index("m") > control.order.index("q"), (
        f"merge ran before its own predecessor: {control.order}"
    )
    assert control.order[-1] == "m", f"merge must run last: {control.order}"


@pytest.mark.asyncio
async def test_merge_receives_both_branches_in_its_upstream_context():
    """The premature merge also lost an input; the fix must restore both."""
    workflow = _late_branch_workflow()
    control = _OrderControlPoint()
    executor = WorkflowExecutor(
        workflow=workflow,
        control_point=control,
        engine_client=StubWorkflowEngineClient(),
    )

    result = await executor.run()

    assert result.success
    # context_from is unset, so the merge's upstream is its direct predecessors.
    assert set(executor.step_outputs) == {"a", "b", "c", "d", "s", "q", "p", "m"}
    assert executor.step_execution_results["m"], "merge produced no task results"


@pytest.mark.asyncio
async def test_plain_diamond_join_still_works():
    """The fix must not disturb the ordinary two-branch diamond."""
    control = _OrderControlPoint()
    workflow = Workflow(name="diamond", steps=[
        _step("left", ["merge"], 0),
        _step("right", ["merge"], 0),
        _step("merge", ["endNode"], 1),
    ])
    executor = WorkflowExecutor(
        workflow=workflow,
        control_point=control,
        engine_client=StubWorkflowEngineClient(),
    )

    result = await executor.run()

    assert result.success
    assert control.order[-1] == "merge"
    assert set(control.order[:2]) == {"left", "right"}


# ======================================================================
# H5: sensitive header names must match across separator spellings
# ======================================================================

_REDACTED = "***REDACTED***"

_SENSITIVE_SPELLINGS = [
    "Authorization",
    "X-Auth-Token",
    "x_auth_token",
    "client_secret",
    "client-secret",
    "api_key",
    "api-key",
    "X-API-KEY",
    "ApiKey",
    "x_api_key",
    "access_session",
    "accessSession",
    "Access-Session",
    "pwd",
    "passwd",
    "Password",
    "Cookie",
    "x_cookie",
]

_NOT_SENSITIVE = ["Content-Type", "A2A-Extensions", "Accept", "X-Request-Id"]


def _render_request_headers(monkeypatch, headers) -> str:
    from loguru import logger as loguru_logger

    from workflow_engine.client.protocol_logger import log_request

    messages = []
    sink = loguru_logger.add(messages.append, format="{message}", level="DEBUG")
    try:
        monkeypatch.setenv("WORKFLOW_ENGINE_PROTOCOL_LOGGING", "true")
        monkeypatch.delenv(
            "WORKFLOW_ENGINE_PROTOCOL_INCLUDE_SENSITIVE_HEADERS", raising=False,
        )
        log_request("agent", "https://example.com", "payload", headers)
    finally:
        loguru_logger.remove(sink)
    return "".join(messages)


@pytest.mark.parametrize("name", _SENSITIVE_SPELLINGS)
def test_sensitive_header_is_redacted_in_every_spelling(monkeypatch, name):
    """Java 366f221: match after stripping '-' and '_'.

    Matching the raw lower-case name silently missed ``api_key``,
    ``access_session``, ``pwd`` and ``passwd``.
    """
    rendered = _render_request_headers(monkeypatch, {name: "top-secret-value"})
    assert "top-secret-value" not in rendered, f"{name} leaked into the log"
    assert _REDACTED in rendered


@pytest.mark.parametrize("name", _NOT_SENSITIVE)
def test_non_sensitive_header_stays_readable(monkeypatch, name):
    rendered = _render_request_headers(monkeypatch, {name: "visible-value"})
    assert "visible-value" in rendered


def test_registered_header_matches_any_spelling(monkeypatch):
    """A custom ``auth_header`` is registered by one spelling, seen in another."""
    from workflow_engine.client.protocol_logger import register_sensitive_header

    register_sensitive_header("X-Corp-Cred")
    rendered = _render_request_headers(
        monkeypatch, {"x_corp_cred": "top-secret-value"},
    )
    assert "top-secret-value" not in rendered
    assert _REDACTED in rendered


def test_url_shaped_name_is_not_treated_as_a_credential(monkeypatch):
    """A name carrying ``://`` is an endpoint, not a credential header."""
    rendered = _render_request_headers(
        monkeypatch, {"https://gateway.example/token": "visible-value"},
    )
    assert "visible-value" in rendered


# ======================================================================
# H2: notification liveness must come from transport activity
# ======================================================================

def _notification_card() -> AgentCard:
    return AgentCard(
        name="agent",
        capabilities=AgentCapabilities(
            extensions=[AgentExtension(uri=A2ATExtension.NOTIFICATION_T.uri)],
        ),
        supported_interfaces=[
            AgentInterface(url="https://agent.example/a2a", protocol_binding="HTTP+JSON")
        ],
    )


def _notification_content() -> MessageContent:
    return A2atMessages.from_generated(
        MetadataContent(
            "Notification-T/information-negotiation/propose/v1",
            "notification prompt",
            A2ATExtension.NOTIFICATION_T.uri,
        ),
        [Part(text="notification prompt")],
    )


class _HeartbeatClient:
    """Open forever, reporting transport activity but never yielding an event.

    Stands in for an SSE stream carrying only comment heartbeats: bytes keep
    arriving, but the protocol layer never sees a decoded event. It reports
    activity through the same public accessor the real httpx hook uses.
    """

    def __init__(self, interval: float = 0.02):
        self._interval = interval
        self.activity_reports = 0

    async def send_message(self, request):
        del request
        while True:
            await asyncio.sleep(self._interval)
            listener = current_activity_listener()
            if listener is not None:
                listener()
                self.activity_reports += 1
        yield None  # unreachable; keeps this an async generator


class _QuietAfterFirstEventClient:
    """Deliver one event, then hold the stream open with no further activity."""

    async def send_message(self, request):
        del request
        from a2a.types.a2a_pb2 import StreamResponse

        response = StreamResponse()
        response.message.message_id = "m1"
        response.message.context_id = "ctx"
        response.message.role = 1
        response.message.parts.add().text = "ack"
        yield response
        await asyncio.sleep(3600)


def _notification_transport(client) -> A2ATransport:
    transport = A2ATransport(agent_cards=[_notification_card()])
    transport.client_for = lambda agent_name: client
    return transport


@pytest.mark.asyncio
async def test_idle_notification_stream_is_released():
    """A dead stream used to leak its connection and its task forever."""
    transport = _notification_transport(_QuietAfterFirstEventClient())
    sender = ExtensionSender(transport, notification_idle_timeout_seconds=0.15)
    subscription = sender.open_notification(
        "agent", _notification_content(), lambda subscription, message: None,
    )
    try:
        with pytest.raises(TimeoutError, match="saw no activity"):
            await asyncio.wait_for(subscription.completion, timeout=5.0)
        assert not subscription.is_active
    finally:
        subscription.close()
        await transport.close()


@pytest.mark.asyncio
async def test_heartbeats_keep_a_notification_stream_alive():
    """The reaper must not tear down a healthy stream that is merely quiet."""
    client = _HeartbeatClient()
    transport = _notification_transport(client)
    sender = ExtensionSender(transport, notification_idle_timeout_seconds=0.2)
    subscription = sender.open_notification(
        "agent", _notification_content(), lambda subscription, message: None,
    )
    try:
        await asyncio.sleep(0.7)  # more than three idle budgets
        assert client.activity_reports > 0
        assert subscription.is_active, "a stream receiving heartbeats was reaped"
        assert subscription.is_healthy(0.5)
    finally:
        subscription.close()
        await transport.close()


@pytest.mark.asyncio
async def test_heartbeat_separates_transport_activity_from_business_events():
    """``last_event_at`` tracks transport activity; the event count does not move."""
    transport = _notification_transport(_HeartbeatClient())
    sender = ExtensionSender(transport, notification_idle_timeout_seconds=5.0)
    subscription = sender.open_notification(
        "agent", _notification_content(), lambda subscription, message: None,
    )
    try:
        await asyncio.sleep(0.2)
        heartbeat = subscription.heartbeat
        assert heartbeat.event_count == 0
        assert heartbeat.last_business_event_at is None
        assert heartbeat.last_event_at is not None
        assert heartbeat.last_event_at >= heartbeat.opened_at
    finally:
        subscription.close()
        await transport.close()


@pytest.mark.asyncio
async def test_is_healthy_is_true_for_a_quiet_but_live_stream():
    """Judging liveness from business events reported a live stream as dead."""
    transport = _notification_transport(_HeartbeatClient())
    sender = ExtensionSender(transport, notification_idle_timeout_seconds=5.0)
    subscription = sender.open_notification(
        "agent", _notification_content(), lambda subscription, message: None,
    )
    try:
        await asyncio.sleep(0.15)
        assert subscription.last_business_event_at is None
        assert subscription.is_healthy(0.5) is True
    finally:
        subscription.close()
        await transport.close()


@pytest.mark.asyncio
async def test_notification_idle_timeout_override_is_validated():
    transport = _notification_transport(_HeartbeatClient())
    try:
        with pytest.raises(ValueError, match="notification_idle_timeout_seconds"):
            ExtensionSender(transport, notification_idle_timeout_seconds=0)
    finally:
        await transport.close()


@pytest.mark.asyncio
async def test_host_callback_cannot_refresh_subscription_activity():
    """Protocol traffic issued by the host callback must not count as activity.

    Otherwise a callback that keeps calling the remote would keep a dead
    notification stream alive.
    """
    seen_inside_callback = []

    class _OneEventClient:
        async def send_message(self, request):
            del request
            from a2a.types.a2a_pb2 import StreamResponse

            response = StreamResponse()
            response.message.message_id = "m1"
            response.message.context_id = "ctx"
            response.message.role = 1
            response.message.parts.add().text = "event"
            yield response
            await asyncio.sleep(3600)

    transport = _notification_transport(_OneEventClient())
    sender = ExtensionSender(transport, notification_idle_timeout_seconds=5.0)

    def listener(subscription, message):
        del subscription, message
        seen_inside_callback.append(current_activity_listener())

    subscription = sender.open_notification(
        "agent", _notification_content(), listener,
    )
    try:
        await asyncio.sleep(0.2)
        assert seen_inside_callback, "the listener never ran"
        assert seen_inside_callback[0] is None, (
            "the activity listener stayed bound while the host callback ran"
        )
    finally:
        subscription.close()
        await transport.close()


@pytest.mark.asyncio
async def test_activity_hook_reports_body_chunks_and_is_scoped_to_the_call():
    """The mechanism the reaper relies on, at the httpx layer.

    A comment-only SSE chunk must count as activity, and traffic outside a bound
    call must not report anything -- otherwise one agent's traffic would keep
    another agent's dead subscription alive.
    """
    chunks = [b": heartbeat\n\n", b'data: {"a":1}\n\n', b": heartbeat\n\n"]
    reported = []

    class _Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            for chunk in chunks:
                yield chunk

    async def handler(request):
        del request
        return httpx.Response(
            200, headers={"content-type": "text/event-stream"}, stream=_Stream()
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    assert install_activity_hook(client) is True
    assert install_activity_hook(client) is False, "hook must install only once"
    async with client:
        # No listener bound: nothing is reported.
        async with client.stream("GET", "http://example/sse") as response:
            async for _ in response.aiter_lines():
                pass
        assert reported == []

        # Bound for the call: every chunk is reported.
        with bind_activity_listener(lambda: reported.append("activity")):
            async with client.stream("GET", "http://example/sse") as response:
                async for _ in response.aiter_lines():
                    pass

    # Every body chunk, plus one report for the response arriving at all: a slow
    # remote whose headers land late must not be reaped on the creation timestamp.
    assert len(reported) == len(chunks) + 1
    assert current_activity_listener() is None, "binding must be reset"
