"""Regression tests for workflow validation and credential security."""

import pytest

from workflow_engine.client.credential_crypto import decrypt_if_needed, encrypt
from workflow_engine.client.protocol_logger import log_request
from workflow_engine.control.control_points import ControlPoint
from workflow_engine.core.executor import WorkflowExecutor
from workflow_engine.core.models import (
    JumpCondition,
    RouteDecision,
    Task,
    TaskResponse,
    Workflow,
    WorkflowStep,
)
from workflow_engine.core.workflow_validator import validate_workflow


def test_credential_crypto_round_trip_requires_256_bit_key(monkeypatch):
    monkeypatch.setenv("A2AT_CRED_KEY", "ab" * 32)
    encrypted = encrypt("secret")
    assert decrypt_if_needed(encrypted) == "secret"


def test_encrypted_credential_fails_closed_without_key(monkeypatch):
    monkeypatch.delenv("A2AT_CRED_KEY", raising=False)
    with pytest.raises(RuntimeError, match="not configured"):
        decrypt_if_needed("enc:aXY=:Y2lwaGVydGV4dA==")


def test_invalid_key_is_rejected(monkeypatch):
    monkeypatch.setenv("A2AT_CRED_KEY", "not-a-256-bit-key")
    with pytest.raises(ValueError, match="64 hexadecimal"):
        encrypt("secret")


def test_protocol_logging_is_opt_in_and_redacts_headers(monkeypatch):
    from loguru import logger

    messages = []
    sink = logger.add(messages.append, format="{message}", level="DEBUG")
    try:
        monkeypatch.delenv("WORKFLOW_ENGINE_PROTOCOL_LOGGING", raising=False)
        log_request("agent", "https://example.com", "payload", {"Authorization": "secret"})
        assert messages == []

        monkeypatch.setenv("WORKFLOW_ENGINE_PROTOCOL_LOGGING", "true")
        log_request(
            "agent",
            "https://example.com",
            "payload",
            {"Authorization": "secret", "X-Request-ID": "request-1"},
        )
        rendered = "".join(messages)
        assert "***REDACTED***" in rendered
        assert "secret" not in rendered
        assert "request-1" in rendered
    finally:
        logger.remove(sink)


def test_validator_rejects_unknown_target():
    workflow = Workflow(
        name="invalid",
        steps=[
            WorkflowStep(
                name="start",
                layer=0,
                subtasks=[Task(agent="agent", description="work")],
                next=[JumpCondition(step="missing", condition="")],
            )
        ],
    )
    with pytest.raises(ValueError, match="unknown step 'missing'"):
        validate_workflow(workflow)


@pytest.mark.asyncio
async def test_invalid_runtime_route_fails_workflow():
    class InvalidRouteControlPoint(ControlPoint):
        async def on_task(self, request, engine_client):
            return TaskResponse(success=True, output="done")

        async def on_route(self, step_name, results, conditions):
            return RouteDecision(next_step="missing", reason="bad decision")

    workflow = Workflow(
        name="runtime-route",
        steps=[
            WorkflowStep(
                name="start",
                layer=0,
                subtasks=[Task(agent="agent-a", description="start")],
                next=[JumpCondition(step="finish", condition="choose")],
            ),
            WorkflowStep(
                name="finish",
                layer=1,
                subtasks=[Task(agent="agent-b", description="finish")],
            ),
        ],
    )
    engine_client = type(
        "Stub",
        (),
        {
            "set_control_point": lambda self, value: None,
            "set_event_callback": lambda self, value: None,
        },
    )()

    result = await WorkflowExecutor(
        workflow, InvalidRouteControlPoint(), engine_client
    ).run()

    assert not result.success
    assert "allowed targets" in result.error
