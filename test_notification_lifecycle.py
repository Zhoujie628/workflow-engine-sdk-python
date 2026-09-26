# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Notification-T ACK and stream lifecycle regressions."""

import asyncio

import pytest
from a2a.types.a2a_pb2 import StreamResponse, TaskState

from test_p0_alignment import _notification_card, _notification_content
from workflow_engine.client.a2a_transport import A2ATransport
from workflow_engine.client.extension_sender import ExtensionSender


def _status(state):
    response = StreamResponse()
    response.status_update.task_id = "task-1"
    response.status_update.status.state = state
    return response


def _message():
    response = StreamResponse()
    response.message.message_id = "message-1"
    response.message.role = 1
    response.message.parts.add().text = "business event"
    return response


def _artifact():
    response = StreamResponse()
    response.artifact_update.task_id = "task-1"
    response.artifact_update.artifact.artifact_id = "artifact-1"
    response.artifact_update.artifact.parts.add().text = "business result"
    return response


async def _open(events, *, linger=False):
    class Client:
        async def send_message(self, request):
            for event in events:
                yield event
            if linger:
                await asyncio.sleep(3600)

    transport = A2ATransport(
        agent_cards=[_notification_card()], notification_ack_timeout_seconds=2,
    )
    transport.create_a2a_client = lambda card: Client()
    received = []
    subscription = ExtensionSender(transport).open_notification(
        "agent", _notification_content(), lambda _, message: received.append(message),
    )
    return transport, subscription, received


@pytest.mark.asyncio
async def test_empty_stream_fails_ack_immediately_and_cleans_up_timers():
    transport, subscription, _ = await _open([])
    try:
        with pytest.raises(RuntimeError, match="ended before acknowledgement"):
            await asyncio.wait_for(subscription.acknowledgement, 0.5)
        await asyncio.wait_for(subscription.completion, 0.5)
        await asyncio.sleep(0)
        assert not subscription.is_active
        assert subscription._ack_timer.done()
        assert subscription._idle_timer.done()
    finally:
        subscription.close()
        await transport.close()


@pytest.mark.asyncio
async def test_open_subscription_has_no_reported_transport_activity_yet():
    """Java exposes no heartbeat timestamp until bytes actually arrive."""
    transport, subscription, _ = await _open([], linger=True)
    try:
        assert subscription.heartbeat.last_event_at is None
        assert not subscription.is_healthy(1)
    finally:
        subscription.close()
        await asyncio.wait_for(subscription.completion, 0.5)
        await transport.close()


@pytest.mark.asyncio
async def test_business_message_and_artifact_do_not_acknowledge_subscription():
    transport, subscription, received = await _open(
        [_message(), _artifact(), _status(TaskState.TASK_STATE_WORKING)], linger=True,
    )
    try:
        result = await asyncio.wait_for(subscription.acknowledgement, 0.5)
        assert result.task_state == "TASK_STATE_WORKING"
        assert len(received) == 3
        assert subscription.is_active
        await asyncio.sleep(0)
        assert subscription._ack_timer.done()
    finally:
        subscription.close()
        await asyncio.wait_for(subscription.completion, 0.5)
        await transport.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("state", [TaskState.TASK_STATE_FAILED, TaskState.TASK_STATE_REJECTED])
async def test_failure_state_rejects_ack_and_ends_stream(state):
    transport, subscription, _ = await _open([_status(state)], linger=True)
    try:
        with pytest.raises(RuntimeError, match="subscription rejected"):
            await asyncio.wait_for(subscription.acknowledgement, 0.5)
        await asyncio.wait_for(subscription.completion, 0.5)
        assert not subscription.is_active
    finally:
        subscription.close()
        await transport.close()


@pytest.mark.asyncio
async def test_normal_eof_after_ack_releases_timers():
    transport, subscription, _ = await _open([_status(TaskState.TASK_STATE_WORKING)])
    try:
        await asyncio.wait_for(subscription.acknowledgement, 0.5)
        await asyncio.wait_for(subscription.completion, 0.5)
        await asyncio.sleep(0)
        assert subscription._ack_timer.done()
        assert subscription._idle_timer.done()
    finally:
        subscription.close()
        await transport.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("result_state", [
    TaskState.TASK_STATE_COMPLETED,
    TaskState.TASK_STATE_FAILED,
    TaskState.TASK_STATE_CANCELED,
    TaskState.TASK_STATE_REJECTED,
    TaskState.TASK_STATE_INPUT_REQUIRED,
    TaskState.TASK_STATE_AUTH_REQUIRED,
])
async def test_business_task_result_state_does_not_end_subscription(result_state):
    """Result state cannot cancel the independent, already acknowledged stream."""
    transport, subscription, received = await _open([
        _status(TaskState.TASK_STATE_WORKING),
        _status(result_state),
        _message(),
    ], linger=True)
    try:
        ack = await asyncio.wait_for(subscription.acknowledgement, 0.5)
        assert ack.task_state == "TASK_STATE_WORKING"
        await asyncio.wait_for(_wait_for_messages(received, 3), 0.5)
        assert subscription.is_active
        assert not subscription.completion.done()
    finally:
        subscription.close()
        await asyncio.wait_for(subscription.completion, 0.5)
        await transport.close()


async def _wait_for_messages(received, count):
    while len(received) < count:
        await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_each_notification_has_its_own_a2a_client_and_closes_its_stream():
    clients = []

    class Client:
        def __init__(self):
            self.stream_closed = False

        async def send_message(self, request):
            try:
                yield _status(TaskState.TASK_STATE_WORKING)
                await asyncio.sleep(3600)
            finally:
                self.stream_closed = True

    def make_client(card):
        client = Client()
        clients.append(client)
        return client

    transport = A2ATransport(agent_cards=[_notification_card()])
    transport.create_a2a_client = make_client
    sender = ExtensionSender(transport)
    first = sender.open_notification("agent", _notification_content(), lambda *_: None)
    second = sender.open_notification("agent", _notification_content(), lambda *_: None)
    try:
        await asyncio.wait_for(asyncio.gather(
            first.acknowledgement, second.acknowledgement,
        ), 0.5)
        assert len(clients) == 2
        assert clients[0] is not clients[1]
        first.close()
        await asyncio.wait_for(first.completion, 0.5)
        assert clients[0].stream_closed
        assert second.is_active
    finally:
        second.close()
        await asyncio.wait_for(second.completion, 0.5)
        await transport.close()
