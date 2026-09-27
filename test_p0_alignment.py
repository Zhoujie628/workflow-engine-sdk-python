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

"""Regression tests for the three P0 capability gaps against the Java engine.

Each test names the gap it locks down, so a failure maps straight back to a
finding in the capability-alignment audit:

- P0-1  pre-task negotiation: a Negotiation-T Propose on a *taskless* bare
        message must enter negotiation instead of being returned as a result.
- P0-2  an interrupted response stream must fall back to polling the remote
        task rather than abandoning (and cancelling) a task that still runs.
- P0-3  the first Notification-T acknowledgement must be bounded by a timeout.
"""

import asyncio
from types import SimpleNamespace

import pytest
from a2a.types import (
    AgentCapabilities,
    AgentCard,
    AgentExtension,
    AgentInterface,
    Part,
    Task,
    TaskState,
    TaskStatus,
)
from a2a_t.core import MetadataContent, NegotiationContext, NegotiationPerformative
from a2a_t.core.standard_templates import INFORMATION_NEGOTIATION_ACCEPT_REJECT_URI

from workflow_engine import (
    A2ATExtension,
    A2atMessages,
    BusinessInput,
    ControlPoint,
    MessageContent,
    NegotiationReply,
    ReceivedMessage,
    SendMessageResult,
    TaskRequest,
    WorkflowEngineClient,
)
from workflow_engine.client.a2a_transport import A2ATransport
from workflow_engine.client.extension_sender import ExtensionSender
from workflow_engine.control.control_points import EventType


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def _proposal(performative=NegotiationPerformative.PROPOSE, round_number=1):
    """A Negotiation-T message the remote would send to open a negotiation."""
    return A2atMessages.from_generated(
        MetadataContent(
            "Negotiation-T/information-negotiation/propose/v1",
            "negotiation prompt",
            A2ATExtension.NEGOTIATION_T.uri,
            NegotiationContext(
                "3dbc13b5-bd57-4c2b-b503-24e381b6c8d3", round_number, 5, performative,
            ),
        ),
        [Part(text="negotiation prompt")],
    )


def _task_request() -> TaskRequest:
    return TaskRequest(
        execution_id="execution", task_id="logical", input=BusinessInput.from_text("task"),
        agent_name="agent", skill="", instruction="task", step_name="step",
    )


class _RecordingControlPoint(ControlPoint):
    """Accepts the first proposal and records every negotiation it is asked about."""

    def __init__(self):
        self.requests = []

    async def on_negotiation(self, request):
        self.requests.append(request)
        proposal = A2atMessages.negotiation_context(request.received)
        generated = MetadataContent(
            INFORMATION_NEGOTIATION_ACCEPT_REJECT_URI,
            "clarification accepted",
            A2ATExtension.NEGOTIATION_T.uri,
            proposal.with_performative(NegotiationPerformative.ACCEPT),
        )
        return NegotiationReply.send(
            A2atMessages.from_generated(
                generated, [Part(text="clarification accepted")]
            )
        )


class _ScriptedTransport:
    """Transport double with scripted per-send results and a breakable stream."""

    _send_timeout_seconds = 5

    def __init__(
        self,
        results=(),
        *,
        break_on_send=None,
        stream_error=None,
        reveal_task_id=True,
        poll_states=(),
        task_poll_interval_seconds=0.01,
    ):
        self._results = list(results)
        self._break_on_send = break_on_send
        self._stream_error = stream_error or RuntimeError("gateway closed the stream")
        self._reveal_task_id = reveal_task_id
        self._poll_states = list(poll_states)
        self._task_poll_interval_seconds = task_poll_interval_seconds
        self.sends = 0
        self.task_ids = []
        self.cancelled = []
        self.polls = 0
        self.card = SimpleNamespace(
            name="agent",
            supported_interfaces=[SimpleNamespace(url="https://agent.example/a2a")],
        )

    @property
    def agent_names(self):
        return ["agent"]

    @property
    def send_timeout_seconds(self):
        return self._send_timeout_seconds

    @property
    def task_poll_interval_seconds(self):
        return self._task_poll_interval_seconds

    def get_card(self, name):
        return self.card if name == "agent" else None

    def create_a2a_client(self, card):
        return object()

    def client_for(self, agent_name):
        return self.create_a2a_client(self.get_card(agent_name))

    def validate_content_extensions(self, card, content):
        return None

    def build_send_request(self, content, context_id, task_id=None):
        self.task_ids.append(task_id or "")
        return SimpleNamespace(
            message=SimpleNamespace(context_id=context_id, task_id=task_id or ""),
        )

    async def consume_stream(self, client, request, callback, agent_name):
        self.sends += 1
        if self._break_on_send is not None and self.sends >= self._break_on_send:
            # Emulate a stream that delivered a task snapshot and then died: the
            # identity reaches the caller through the intermediate callback
            # before the transport fault surfaces.
            if self._reveal_task_id:
                callback(EventType.AGENT_STATUS_UPDATE, {
                    "agent": agent_name, "task_id": "remote-1",
                    "state": "TASK_STATE_WORKING", "is_final": False,
                    "text": "", "metadata": {},
                })
            raise self._stream_error
        result = self._results[self.sends - 1]
        # A real remote echoes the caller's contextId; identity validation
        # compares against it, so the double has to do the same.
        if result.task is not None:
            result.task.context_id = request.message.context_id
        return result

    async def get_task(self, agent_name, task_id):
        self.polls += 1
        state = self._poll_states.pop(0) if self._poll_states else "TASK_STATE_COMPLETED"
        return SendMessageResult(task_state=state, text="done")

    async def cancel_task(self, agent_name, task_id):
        self.cancelled.append((agent_name, task_id))
        return SendMessageResult(task_state="TASK_STATE_CANCELED")

    async def close(self):
        pass


def _completed(task_id="remote-1", text="complete") -> SendMessageResult:
    return SendMessageResult(
        task=Task(
            id=task_id, context_id="ctx",
            status=TaskStatus(state=TaskState.TASK_STATE_COMPLETED),
        ),
        task_state="TASK_STATE_COMPLETED",
        text=text,
        received_messages=(ReceivedMessage(message=MessageContent.text(text)),),
    )


def _bare_message_result(content) -> SendMessageResult:
    """A standalone message with no task at all -- A2A-T pre-task negotiation."""
    return SendMessageResult(
        task=None,
        task_state="",
        text="negotiation prompt",
        received_messages=(ReceivedMessage(message=content),),
    )


# ----------------------------------------------------------------------
# P0-1: a Propose on a taskless bare message must open a negotiation
# ----------------------------------------------------------------------

@pytest.mark.asyncio
async def test_taskless_bare_message_propose_enters_negotiation():
    """A pre-task Propose used to be returned to the host as the final answer."""
    transport = _ScriptedTransport([
        _bare_message_result(_proposal()),
        _completed(),
    ])
    control = _RecordingControlPoint()

    result = await WorkflowEngineClient(transport).dispatch(
        _task_request(), MessageContent.text("task"), control,
    )

    assert len(control.requests) == 1, "the host must be asked to negotiate"
    assert result.task_state == "TASK_STATE_COMPLETED"
    assert transport.sends == 2


@pytest.mark.asyncio
async def test_taskless_negotiation_continuation_carries_no_task_id():
    """Pre-task negotiation is correlated by contextId; no task exists yet."""
    transport = _ScriptedTransport([
        _bare_message_result(_proposal()),
        _completed(),
    ])

    await WorkflowEngineClient(transport).dispatch(
        _task_request(), MessageContent.text("task"), _RecordingControlPoint(),
    )

    assert transport.task_ids == ["", ""], "no taskId may be sent before one exists"


@pytest.mark.asyncio
async def test_bare_message_without_negotiation_metadata_is_still_a_final_answer():
    """A plain standalone message must keep completing the interaction."""
    transport = _ScriptedTransport([_bare_message_result(MessageContent.text("here you go"))])
    control = _RecordingControlPoint()

    result = await WorkflowEngineClient(transport).dispatch(
        _task_request(), MessageContent.text("task"), control,
    )

    assert control.requests == [], "a plain answer must not be treated as a proposal"
    assert result.is_success


@pytest.mark.asyncio
async def test_invalid_negotiation_metadata_on_a_bare_message_fails_loudly():
    """Malformed Negotiation-T metadata must fail, never pass as a result."""
    transport = _ScriptedTransport([
        _bare_message_result(_proposal(NegotiationPerformative.ACCEPT)),
    ])
    control = _RecordingControlPoint()

    with pytest.raises(ValueError, match="valid Negotiation-T Propose"):
        await WorkflowEngineClient(transport).dispatch(
            _task_request(), MessageContent.text("task"), control,
        )

    assert control.requests == []


@pytest.mark.asyncio
async def test_repeated_taskless_round_is_reported_instead_of_looping():
    """Without a task there is nothing to poll, so a repeat must not spin."""
    transport = _ScriptedTransport([
        _bare_message_result(_proposal()),
        _bare_message_result(_proposal()),
    ])

    with pytest.raises(RuntimeError, match="taskless negotiation round"):
        await WorkflowEngineClient(transport).dispatch(
            _task_request(), MessageContent.text("task"), _RecordingControlPoint(),
        )


# ----------------------------------------------------------------------
# P0-2: an interrupted stream must poll, not cancel a still-running task
# ----------------------------------------------------------------------

@pytest.mark.asyncio
async def test_interrupted_stream_polls_instead_of_cancelling_the_remote_task():
    """The old path cancelled a healthy task merely because a gateway dropped SSE."""
    transport = _ScriptedTransport(break_on_send=1)

    result = await WorkflowEngineClient(transport).dispatch(
        _task_request(), MessageContent.text("task"), _RecordingControlPoint(),
    )

    assert result.task_state == "TASK_STATE_COMPLETED"
    assert transport.polls == 1
    assert transport.cancelled == [], "a transport fault must never cancel the task"


@pytest.mark.asyncio
async def test_interrupted_stream_polls_until_the_task_reaches_a_terminal_state():
    transport = _ScriptedTransport(
        break_on_send=1,
        poll_states=("TASK_STATE_WORKING", "TASK_STATE_WORKING", "TASK_STATE_COMPLETED"),
    )

    result = await WorkflowEngineClient(transport).dispatch(
        _task_request(), MessageContent.text("task"), _RecordingControlPoint(),
    )

    assert result.task_state == "TASK_STATE_COMPLETED"
    assert transport.polls == 3
    assert transport.cancelled == []


@pytest.mark.asyncio
async def test_interrupted_stream_without_a_task_identity_still_propagates():
    """With no task identity there is nothing to poll, so the fault must surface."""
    transport = _ScriptedTransport(break_on_send=1, reveal_task_id=False)

    with pytest.raises(RuntimeError, match="gateway closed the stream"):
        await WorkflowEngineClient(transport).dispatch(
            _task_request(), MessageContent.text("task"), _RecordingControlPoint(),
        )

    assert transport.cancelled == []


@pytest.mark.asyncio
async def test_remote_rejection_is_not_mistaken_for_a_recoverable_stream_break():
    from workflow_engine.client.remote_error import RemoteA2AError

    error = RemoteA2AError(403, 403, "denied", reason="ACCESS_DENIED")
    transport = _ScriptedTransport(break_on_send=1, stream_error=error)

    with pytest.raises(RemoteA2AError, match="denied"):
        await WorkflowEngineClient(transport).dispatch(
            _task_request(), MessageContent.text("task"), _RecordingControlPoint(),
        )

    assert transport.polls == 0
    assert transport.cancelled == []


@pytest.mark.asyncio
async def test_empty_stream_does_not_poll_or_cancel_a_known_task():
    from workflow_engine.client.remote_error import EmptyA2AStreamError

    transport = _ScriptedTransport(
        break_on_send=1,
        stream_error=EmptyA2AStreamError("A2A response stream ended without an event"),
    )
    with pytest.raises(EmptyA2AStreamError):
        await WorkflowEngineClient(transport).dispatch(
            _task_request(), MessageContent.text("task"), _RecordingControlPoint(),
        )
    assert transport.polls == 0
    assert transport.cancelled == []


@pytest.mark.asyncio
async def test_local_negotiation_failure_still_cancels_the_remote_task():
    """The cancellation contract for local interaction failures must survive."""
    class RefusingControlPoint(ControlPoint):
        async def on_negotiation(self, request):
            raise ValueError("host cannot answer")

    transport = _ScriptedTransport([
        SendMessageResult(
            task=Task(
                id="remote-1", context_id="ctx",
                status=TaskStatus(state=TaskState.TASK_STATE_INPUT_REQUIRED),
            ),
            task_state="TASK_STATE_INPUT_REQUIRED",
            received_messages=(ReceivedMessage(message=_proposal()),),
        ),
    ])

    with pytest.raises(ValueError, match="host cannot answer"):
        await WorkflowEngineClient(transport).dispatch(
            _task_request(), MessageContent.text("task"), RefusingControlPoint(),
        )

    assert transport.cancelled == [("agent", "remote-1")]


@pytest.mark.asyncio
async def test_status_update_events_carry_the_task_id():
    """The identity that makes stream recovery possible must reach the callback."""
    from a2a.types.a2a_pb2 import StreamResponse

    transport = A2ATransport(agent_cards=[AgentCard(
        name="agent",
        capabilities=AgentCapabilities(),
        supported_interfaces=[
            AgentInterface(url="https://agent.example/a2a", protocol_binding="HTTP+JSON")
        ],
    )])
    response = StreamResponse()
    response.task.id = "remote-9"
    response.task.context_id = "ctx"
    response.task.status.state = TaskState.TASK_STATE_WORKING

    class _Client:
        async def send_message(self, request):
            del request
            yield response

    seen = []
    try:
        await transport.consume_stream(
            _Client(), object(),
            lambda event_type, data: seen.append((event_type, data)), "agent",
        )
    finally:
        await transport.close()

    assert seen[0][0] == EventType.AGENT_STATUS_UPDATE
    assert seen[0][1]["task_id"] == "remote-9"


def test_task_poll_interval_is_validated():
    """Mirrors the Java engine's 100ms floor for taskPollIntervalMillis."""
    with pytest.raises(ValueError, match="task_poll_interval_seconds"):
        A2ATransport(agent_cards=[], task_poll_interval_seconds=0.05)

    transport = A2ATransport(agent_cards=[], task_poll_interval_seconds=0.1)
    assert transport.task_poll_interval_seconds == 0.1


# ----------------------------------------------------------------------
# P0-3: the first Notification-T acknowledgement must be bounded
# ----------------------------------------------------------------------

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


class _SilentStreamClient:
    """Accepts the subscription and then never sends anything at all."""

    async def send_message(self, request):
        del request
        await asyncio.sleep(3600)
        yield None


class _ImmediateAckClient:
    """Acknowledges at once, then stays open and idle."""

    async def send_message(self, request):
        del request
        from a2a.types.a2a_pb2 import StreamResponse

        response = StreamResponse()
        from a2a.types.a2a_pb2 import TaskState
        response.status_update.task_id = "task-1"
        response.status_update.status.state = TaskState.TASK_STATE_WORKING
        yield response
        await asyncio.sleep(3600)


def _notification_transport(client, **kwargs) -> A2ATransport:
    transport = A2ATransport(agent_cards=[_notification_card()], **kwargs)
    transport.create_a2a_client = lambda card: client
    return transport


@pytest.mark.asyncio
async def test_notification_acknowledgement_times_out_when_the_remote_never_acks():
    """The acknowledgement future used to stay pending forever."""
    transport = _notification_transport(
        _SilentStreamClient(), notification_ack_timeout_seconds=0.05,
    )
    sender = ExtensionSender(transport)
    subscription = sender.open_notification(
        "agent", _notification_content(), lambda subscription, message: None,
    )
    try:
        with pytest.raises(TimeoutError, match="acknowledgement timed out"):
            await asyncio.wait_for(subscription.acknowledgement, timeout=5.0)
        assert not subscription.is_active
    finally:
        subscription.close()
        await transport.close()


@pytest.mark.asyncio
async def test_notification_ack_timeout_does_not_bound_sse_idle_after_the_ack():
    """Only the first ACK is bounded; a quiet-but-open stream must survive."""
    transport = _notification_transport(
        _ImmediateAckClient(), notification_ack_timeout_seconds=0.05,
    )
    sender = ExtensionSender(transport)
    subscription = sender.open_notification(
        "agent", _notification_content(), lambda subscription, message: None,
    )
    try:
        result = await asyncio.wait_for(subscription.acknowledgement, timeout=5.0)
        assert result.received_messages

        # Idle for well past the ACK window: the subscription must stay open.
        await asyncio.sleep(0.2)
        assert subscription.is_active
    finally:
        subscription.close()
        await transport.close()


def test_notification_ack_timeout_override_is_validated():
    transport = _notification_transport(_SilentStreamClient())
    with pytest.raises(ValueError, match="notification_ack_timeout_seconds"):
        ExtensionSender(transport, notification_ack_timeout_seconds=0)

    sender = ExtensionSender(transport, notification_ack_timeout_seconds=0.5)
    assert sender is not None
