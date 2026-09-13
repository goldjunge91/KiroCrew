"""L1 of the recovery ladder end to end: handle classifies, chat runner retries.

The ACP handle owns the classification (one place, at the protocol layer);
chat_runner reads ``client.last_infra_error`` at end of turn and re-queues ONE
continuation on the shared schedule. The runner branch is pinned at source
level because its host function is the 12k-line turn loop.
"""

from __future__ import annotations

import asyncio
import inspect
from unittest.mock import AsyncMock, MagicMock

import pytest

from kiro_crew.acp.session_handle import AcpSessionHandle
from kiro_crew.acp.types import JsonRpcMessage
from kiro_crew.dashboard import chat_runner
from kiro_crew.dashboard.state import REFUSAL_RECOVERY_PREFIX
from kiro_crew.recovery import ladder as lad


class _Runtime:
    def __init__(self, queue: asyncio.Queue) -> None:
        self.pid = None
        self.is_alive = MagicMock(return_value=True)
        self.send_notification = AsyncMock()
        self.supports_image_prompt = False
        self.acp_backend = ""
        self._queue = queue

    def mark_turn_active(self, session_id: str, active: bool) -> None:
        pass


def _handle() -> AcpSessionHandle:
    queue: asyncio.Queue = asyncio.Queue()
    return AcpSessionHandle("sA", queue, _Runtime(queue))


def _tool_call(tool_id: str) -> JsonRpcMessage:
    return JsonRpcMessage(
        method="session/update",
        params={
            "sessionId": "sA",
            "update": {
                "sessionUpdate": "tool_call",
                "toolCallId": tool_id,
                "title": "call a tool",
                "kind": "other",
                "status": "in_progress",
            },
        },
    )


def _tool_result(tool_id: str, text: str, status: str = "failed") -> JsonRpcMessage:
    return JsonRpcMessage(
        method="session/update",
        params={
            "sessionId": "sA",
            "update": {
                "sessionUpdate": "tool_call_update",
                "toolCallId": tool_id,
                "status": status,
                "content": [{"type": "content", "content": {"type": "text", "text": text}}],
            },
        },
    )


_CAPACITY = 'MCP error -32001: {"class": "capacity", "retry_after_secs": 9} — gateway at capacity'


class TestHandleClassifies:
    def test_capacity_refusal_is_recorded_with_its_retry_hint(self):
        h = _handle()
        assert h.last_infra_error is None
        h._handle_update(_tool_call("t1"))
        h._handle_update(_tool_result("t1", _CAPACITY))
        assert h.last_infra_error is not None
        assert h.last_infra_error.error_class == lad.CLASS_CAPACITY
        assert h.last_infra_error.retry_after_secs == 9.0

    def test_a_later_ordinary_result_clears_it(self):
        h = _handle()
        h._handle_update(_tool_call("t1"))
        h._handle_update(_tool_result("t1", _CAPACITY))
        h._handle_update(_tool_call("t2"))
        h._handle_update(_tool_result("t2", "file contents here", status="completed"))
        assert h.last_infra_error is None

    def test_an_ordinary_failure_is_not_infra(self):
        h = _handle()
        h._handle_update(_tool_call("t1"))
        h._handle_update(_tool_result("t1", "Permission denied: /etc/shadow"))
        assert h.last_infra_error is None

    def test_a_new_turn_starts_clean(self):
        h = _handle()
        h._handle_update(_tool_call("t1"))
        h._handle_update(_tool_result("t1", _CAPACITY))
        assert h.last_infra_error is not None
        # The per-turn reset is the same one that clears the stop reason.
        src = inspect.getsource(AcpSessionHandle)
        idx = src.index("self.last_infra_error = None")
        assert 'self._last_stop_reason = ""' in src[:idx]

    def test_gateway_recoverable_infra_marker(self):
        h = _handle()
        h._handle_update(_tool_call("t1"))
        h._handle_update(_tool_result("t1", "BackendGone: the pooled backend exited"))
        assert h.last_infra_error is not None
        assert h.last_infra_error.error_class == lad.CLASS_RECOVERABLE_INFRA


class TestRunnerBranch:
    """Source-level pins on the L1 branch of the turn loop."""

    @pytest.fixture(scope="class")
    def src(self) -> str:
        return inspect.getsource(chat_runner)

    def test_reads_the_handles_verdict_not_a_regex(self, src):
        assert 'isinstance(getattr(client, "last_infra_error", None), InfraError)' in src
        assert "default_ladder().observe_failure(" in src
        assert "L1_TOOL_CALL," in src

    def test_uses_every_sibling_guard(self, src):
        start = src.index('isinstance(getattr(client, "last_infra_error", None), InfraError)')
        branch = src[start : start + 1200]
        for guard in (
            "_prompt_depth == 0",
            "_stop_reason == STOP_REASON_END_TURN",
            "not _armed_final",
            "not slot._in_stage_execution",
            "not _should_suppress_requeue(slot)",
            "_stop_gen_turn_start",
            "not _has_user_queued_followup(slot)",
            "_pending_steers",
        ):
            assert guard in branch or guard in src[start - 600 : start], guard

    def test_retry_waits_then_requeues_a_continuation_not_the_message(self, src):
        start = src.index("_l1 = default_ladder().observe_failure(")
        body = src[start : start + 2500]
        assert "await _recovery_delay(_l1.delay_secs)" in body
        assert "build_infra_retry_prompt(" in body
        assert "payload=RecoveryPayload.CONTINUATION" in body
        assert "build_recovery_requeue(" not in body  # never a verbatim replay
        assert "_recovering_infra = True" in body

    def test_un_landed_turn_is_excluded_from_success_accounting(self, src):
        assert src.count("and not _recovering_infra") >= 3

    def test_a_landed_turn_closes_the_l1_run(self, src):
        assert "default_ladder().observe_success(L1_TOOL_CALL, slot.key)" in src


class TestContinuationPrompt:
    def test_opens_with_the_refusal_card_marker(self):
        msg = chat_runner.build_infra_retry_prompt("capacity", 9.0)
        assert msg.split("\n", 1)[0] == REFUSAL_RECOVERY_PREFIX
        assert "9s" in msg
        assert "same arguments" in msg
        assert "Do not repeat any earlier tool call" in msg

    def test_without_a_hint(self):
        msg = chat_runner.build_infra_retry_prompt("recoverable_infra", None)
        assert "pause" not in msg
        assert "recoverable_infra" in msg


@pytest.mark.asyncio
async def test_recovery_delay_seam_sleeps_only_for_positive_values(monkeypatch):
    slept: list[float] = []

    async def fake_sleep(secs):
        slept.append(secs)

    monkeypatch.setattr(chat_runner.asyncio, "sleep", fake_sleep)
    await chat_runner._recovery_delay(0.0)
    await chat_runner._recovery_delay(0.05)
    assert slept == [0.05]
