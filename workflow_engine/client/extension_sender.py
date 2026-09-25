# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0

"""Independent Authorization-T and Notification-T operations."""

from __future__ import annotations

import asyncio
import inspect
import time
import uuid
from dataclasses import dataclass
from typing import Callable, Optional

from loguru import logger

from workflow_engine.client.a2a_transport import A2ATransport
from workflow_engine.client.extensions import A2ATExtension
from workflow_engine.client.transport_activity import bind_activity_listener
from workflow_engine.core.models import MessageContent, ReceivedMessage, SendMessageResult


@dataclass(frozen=True)
class NotificationHeartbeat:
    """Local liveness snapshot of one Notification-T subscription.

    ``last_event_at`` reports the last **transport** activity, including an SSE
    comment heartbeat, and ``event_count`` counts only decoded A2A business
    events. ``last_business_event_at`` exposes the latter directly. That split
    matches the Java engine's ``NotificationSubscription``: a long-lived
    notification stream may legitimately carry only heartbeats for a long time,
    so liveness cannot be judged from business events alone.
    """

    opened_at: float
    last_event_at: Optional[float]
    event_count: int
    active: bool
    last_business_event_at: Optional[float] = None


class NotificationSubscription:
    """Explicit lifecycle handle for one long-lived Notification-T stream."""

    def __init__(self, agent_name: str, context_id: str):
        loop = asyncio.get_running_loop()
        self.agent_name = agent_name
        self.context_id = context_id
        self.acknowledgement: asyncio.Future[SendMessageResult] = loop.create_future()
        self.completion: asyncio.Future[None] = loop.create_future()
        self._opened_at = time.time()
        # Opening the stream counts as activity, so a stream that never delivers
        # a single chunk is still reapable. The Java engine initialises its
        # lastActivityNanos the same way.
        self._last_activity_at: Optional[float] = self._opened_at
        self._last_activity_monotonic: Optional[float] = time.monotonic()
        self._last_business_event_at: Optional[float] = None
        self._event_count = 0
        self._task: Optional[asyncio.Task] = None
        self._ack_timer: Optional[asyncio.Task] = None
        self._idle_timer: Optional[asyncio.Task] = None
        self._closed = False

    @property
    def is_active(self) -> bool:
        return not self._closed and not self.completion.done()

    @property
    def heartbeat(self) -> NotificationHeartbeat:
        return NotificationHeartbeat(
            self._opened_at, self._last_activity_at, self._event_count,
            self.is_active, self._last_business_event_at,
        )

    @property
    def last_business_event_at(self) -> Optional[float]:
        """Last decoded A2A business event, excluding transport-only SSE comments."""
        return self._last_business_event_at

    def is_healthy(self, maximum_idle_seconds: float) -> bool:
        """Whether transport activity was seen within ``maximum_idle_seconds``.

        Judged from transport activity rather than business events: a healthy
        subscription that is only receiving heartbeats would otherwise be
        reported as dead, and a caller acting on that would tear down a live
        stream.
        """
        if maximum_idle_seconds < 0:
            raise ValueError("maximum_idle_seconds must not be negative")
        return (
            self.is_active
            and self._last_activity_monotonic is not None
            and time.monotonic() - self._last_activity_monotonic <= maximum_idle_seconds
        )

    def _record_activity(self) -> None:
        """Note that the transport delivered something (event or heartbeat)."""
        self._last_activity_at = time.time()
        self._last_activity_monotonic = time.monotonic()

    def _record_event(self) -> None:
        self._event_count += 1
        self._last_business_event_at = time.time()
        self._record_activity()

    def _attach(self, task: asyncio.Task) -> None:
        self._task = task

    def _attach_ack_timer(self, timer: asyncio.Task) -> None:
        self._ack_timer = timer

    def _attach_idle_timer(self, timer: asyncio.Task) -> None:
        self._idle_timer = timer

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._task is not None and not self._task.done():
            self._task.cancel()
        try:
            current = asyncio.current_task()
        except RuntimeError:
            current = None
        # The ACK deadline and the idle reaper may each fire close() from inside
        # their own task; never self-cancel, or the timeout path would raise out
        # of the timer instead of completing it.
        for timer in (self._ack_timer, self._idle_timer):
            if timer is not None and timer is not current and not timer.done():
                timer.cancel()
        if not self.acknowledgement.done():
            self.acknowledgement.cancel(
                "Notification-T subscription closed before acknowledgement"
            )
        if not self.completion.done():
            self.completion.set_result(None)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.close()
        try:
            await self.completion
        except asyncio.CancelledError:
            pass


class ExtensionSender:
    """Final-content sender on a caller-owned transport, outside workflow execution."""

    def __init__(
        self,
        transport: A2ATransport,
        notification_ack_timeout_seconds: Optional[float] = None,
        notification_idle_timeout_seconds: Optional[float] = None,
    ):
        if (
            notification_ack_timeout_seconds is not None
            and notification_ack_timeout_seconds <= 0
        ):
            raise ValueError("notification_ack_timeout_seconds must be positive")
        if (
            notification_idle_timeout_seconds is not None
            and notification_idle_timeout_seconds <= 0
        ):
            raise ValueError("notification_idle_timeout_seconds must be positive")
        self._transport = transport
        self._notification_ack_timeout_seconds = notification_ack_timeout_seconds
        self._notification_idle_timeout_seconds = notification_idle_timeout_seconds

    @property
    def transport(self) -> A2ATransport:
        return self._transport

    def _require_extension(
        self, agent_name: str, content: MessageContent, extension: A2ATExtension,
    ):
        card = self._transport.get_card(agent_name)
        if card is None:
            raise ValueError(f"Agent not found: {agent_name}")
        advertised = self._transport.get_extension_uris(card)
        if (
            extension.uri not in advertised
            or extension.uri not in content.extensions
            or extension.uri not in content.metadata
        ):
            raise ValueError(f"Target capability and content must use {extension.uri}")
        return card

    async def send_authorization(
        self, agent_name: str, content: MessageContent,
    ) -> SendMessageResult:
        card = self._require_extension(agent_name, content, A2ATExtension.AUTHORIZATION_T)
        self._transport.validate_content_extensions(card, content)
        context_id = str(uuid.uuid4())
        client = self._transport.client_for(agent_name)
        request = self._transport.build_send_request(content, context_id)
        deadline = time.monotonic() + self._transport.send_timeout_seconds
        result = await asyncio.wait_for(
            self._transport.consume_stream(client, request, agent_name=agent_name),
            timeout=max(0.001, deadline - time.monotonic()),
        )
        task_id = self._validate_task_identity(result, context_id)
        while result.task_state in {"TASK_STATE_SUBMITTED", "TASK_STATE_WORKING"}:
            if not task_id:
                raise ValueError("Authorization-T acknowledgement has no task identity")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Authorization-T operation timed out")
            await asyncio.sleep(min(0.25, remaining))
            result = await asyncio.wait_for(
                self._transport.get_task(agent_name, task_id),
                timeout=max(0.001, deadline - time.monotonic()),
            )
            self._validate_task_identity(result, context_id, task_id)
        return result

    @staticmethod
    def _validate_task_identity(
        result: SendMessageResult,
        context_id: str,
        task_id: Optional[str] = None,
    ) -> Optional[str]:
        if result.task is None:
            return task_id
        if not result.task.id or result.task.context_id != context_id:
            raise ValueError("Authorization-T task/context identity changed")
        if task_id is not None and result.task.id != task_id:
            raise ValueError("Authorization-T task/context identity changed")
        return result.task.id

    def open_notification(
        self,
        agent_name: str,
        content: MessageContent,
        listener: Callable[[NotificationSubscription, ReceivedMessage], object],
    ) -> NotificationSubscription:
        card = self._require_extension(agent_name, content, A2ATExtension.NOTIFICATION_T)
        self._transport.validate_content_extensions(card, content)
        if listener is None:
            raise ValueError("listener is required")
        context_id = str(uuid.uuid4())
        subscription = NotificationSubscription(agent_name, context_id)

        async def consume() -> None:
            try:
                client = self._transport.client_for(agent_name)
                request = self._transport.build_send_request(content, context_id)
                # Bound for the whole stream, not just the call: the request is
                # issued on the first iteration, and every body chunk the HTTP
                # layer reads afterwards -- including an SSE comment heartbeat
                # that never becomes a decoded event -- reports activity here.
                with bind_activity_listener(subscription._record_activity):
                    async for response in client.send_message(request):
                        self._transport.log_response_event(agent_name, response)
                        subscription._record_event()
                        result, received = self._incremental_result(response)
                        if not subscription.acknowledgement.done():
                            subscription.acknowledgement.set_result(result)
                        if received is not None:
                            # Unbind while the host callback runs: any protocol
                            # traffic it issues belongs to the host, not to this
                            # subscription, and must not keep a dead stream alive.
                            with bind_activity_listener(None):
                                returned = listener(subscription, received)
                                if inspect.isawaitable(returned):
                                    await returned
            except asyncio.CancelledError:
                if not subscription.completion.done():
                    subscription.completion.set_result(None)
            except Exception as exc:
                if not subscription.acknowledgement.done():
                    subscription.acknowledgement.set_exception(exc)
                if not subscription.completion.done():
                    subscription.completion.set_exception(exc)
            else:
                if not subscription.completion.done():
                    subscription.completion.set_result(None)
            finally:
                subscription._closed = True
                if not subscription.completion.done():
                    subscription.completion.set_result(None)

        task = asyncio.create_task(consume(), name=f"notification-t-{agent_name}")
        subscription._attach(task)

        ack_timeout = (
            self._notification_ack_timeout_seconds
            if self._notification_ack_timeout_seconds is not None
            else self._transport.notification_ack_timeout_seconds
        )

        async def enforce_ack_deadline() -> None:
            # Bounds only the *first* acknowledgement. A remote that accepts the
            # stream but never acknowledges would otherwise leave the caller
            # waiting forever. SSE idleness after the ACK is deliberately not
            # bounded here, matching the Java engine's notificationAckTimeoutSeconds.
            await asyncio.sleep(ack_timeout)
            if subscription.acknowledgement.done():
                return
            subscription.acknowledgement.set_exception(
                TimeoutError(
                    f"Notification-T acknowledgement timed out after {ack_timeout}s"
                )
            )
            logger.warning(
                f"Notification-T subscription for agent={agent_name} was not "
                f"acknowledged within {ack_timeout}s; closing the subscription"
            )
            subscription.close()

        subscription._attach_ack_timer(asyncio.create_task(
            enforce_ack_deadline(), name=f"notification-t-ack-{agent_name}",
        ))

        idle_timeout = (
            self._notification_idle_timeout_seconds
            if self._notification_idle_timeout_seconds is not None
            else float(self._transport.send_timeout_seconds)
        )

        async def enforce_idle_deadline() -> None:
            # Mirrors the Java engine, which bounds a notification stream by
            # sendTimeoutSeconds *without transport activity or a decoded event*.
            # Because activity includes SSE heartbeats, a healthy but quiet
            # subscription is never reaped -- only a genuinely dead one is
            # released, instead of leaking its connection and task forever.
            poll_seconds = min(1.0, max(0.05, idle_timeout / 10.0))
            while True:
                await asyncio.sleep(poll_seconds)
                if subscription.completion.done() or not subscription.is_active:
                    return
                last_activity = subscription._last_activity_monotonic
                if last_activity is None:
                    continue
                if time.monotonic() - last_activity < idle_timeout:
                    continue
                subscription.completion.set_exception(
                    TimeoutError(
                        f"Notification-T stream for agent={agent_name} saw no "
                        f"activity for {idle_timeout}s"
                    )
                )
                logger.warning(
                    f"Notification-T subscription for agent={agent_name} saw no "
                    f"transport activity for {idle_timeout}s; releasing it"
                )
                subscription.close()
                return

        subscription._attach_idle_timer(asyncio.create_task(
            enforce_idle_deadline(), name=f"notification-t-idle-{agent_name}",
        ))
        return subscription

    def _incremental_result(self, response):
        if response.HasField("task"):
            result = self._transport._result_from_task(response.task)
            return result, result.received_messages[0]
        if response.HasField("message"):
            received = ReceivedMessage(
                message=self._transport._message_content(response.message)
            )
            return SendMessageResult(received_messages=(received,)), received
        if response.HasField("status_update"):
            update = response.status_update
            message = self._transport._message_content(update.status.message)
            state = self._transport._extract_task_state(
                type("TaskView", (), {"status": update.status})()
            )
            failure_code, failure_message = self._transport._failure_from_state(
                state, message,
            )
            received = ReceivedMessage(
                message=message,
                task_metadata=self._transport._struct_dict(update.metadata),
            )
            return SendMessageResult(
                task_state=state,
                failure_code=failure_code,
                failure_message=failure_message,
                received_messages=(received,),
            ), received
        if response.HasField("artifact_update"):
            update = response.artifact_update
            artifact = self._transport._received_artifact(update.artifact)
            received = ReceivedMessage(
                task_metadata=self._transport._struct_dict(update.metadata),
                artifacts=(artifact,),
            )
            return SendMessageResult(received_messages=(received,)), received
        return SendMessageResult(), None
