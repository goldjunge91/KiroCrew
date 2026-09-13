"""The ladder: L1 retry honouring retry_after, escalation L2->L3, L5 never automatic."""

from __future__ import annotations

import random
from unittest.mock import patch

import pytest
from overload_fakes import Clock

from kiro_crew.metrics import events as ev
from kiro_crew.recovery import ladder as lad


class _Rec:
    def __init__(self) -> None:
        self.counters: list = []
        self.hists: list = []

    def counter(self, name, value=1, *, attrs=None, **kw):
        self.counters.append({"name": name, "attrs": dict(attrs or {})})

    def histogram(self, name, value, *, attrs=None, **kw):
        self.hists.append({"name": name, "value": value, "attrs": dict(attrs or {})})


@pytest.fixture
def rec():
    r = _Rec()
    with patch("kiro_crew.metrics.provider.get_recorder", return_value=r):
        yield r


@pytest.fixture
def clock():
    return Clock()


def _ladder(clock, **kw) -> lad.RecoveryLadder:
    return lad.RecoveryLadder(clock=clock, rng=random.Random(42), **kw)


# ── L1 detector ──────────────────────────────────────────────────────────────


class TestClassifyInfraError:
    def test_stub_capacity_error_object(self):
        err = {
            "code": -32001,
            "message": "gateway at capacity",
            "data": {"class": "capacity", "retry_after_secs": 7},
        }
        got = lad.classify_infra_error(err)
        assert got is not None
        assert got.error_class == lad.CLASS_CAPACITY
        assert got.retry_after_secs == 7.0
        assert got.code == -32001

    def test_jsonrpc_envelope_with_error_member(self):
        got = lad.classify_infra_error(
            {"jsonrpc": "2.0", "id": 4, "error": {"code": -32001, "data": {"class": "capacity"}}}
        )
        assert got is not None and got.error_class == lad.CLASS_CAPACITY
        assert got.retry_after_secs is None

    def test_serialised_error_text(self):
        text = '{"code": -32001, "message": "capacity", "data": {"class": "capacity", "retry_after_secs": 12}}'
        got = lad.classify_infra_error(text)
        assert got is not None and got.retry_after_secs == 12.0

    def test_kiro_cli_prose_carrying_the_code(self):
        got = lad.classify_infra_error(
            "MCP error -32001: gateway at capacity (class=capacity, retry_after_secs=5)"
        )
        assert got is not None
        assert got.error_class == lad.CLASS_CAPACITY
        assert got.retry_after_secs == 5.0

    def test_gateway_recoverable_infra_markers(self):
        for text in (
            "BackendGone: pooled backend exited before the reply",
            "spawn queue timed out after 600s",
            "reconnect budget exhausted; gateway daemon unavailable",
        ):
            got = lad.classify_infra_error(text)
            assert got is not None, text
            assert got.error_class == lad.CLASS_RECOVERABLE_INFRA

    def test_ordinary_failures_are_not_infra(self):
        for text in (
            "Permission denied: /home/x/.aws/credentials",
            "invalid arguments: expected string",
            "I cannot help with that request.",
            "Traceback (most recent call last): ValueError",
            "",
        ):
            assert lad.classify_infra_error(text) is None, text
        assert lad.classify_infra_error(None) is None

    def test_a_document_quoting_a_marker_is_not_an_error(self):
        doc = "log line about a BackendGone incident\n" * 200
        assert len(doc) > lad._INFRA_TEXT_MAX_CHARS
        assert lad.classify_infra_error(doc) is None

    def test_a_number_that_merely_contains_the_code_does_not_match(self):
        assert lad.classify_infra_error("balance: -320011.5 units") is None
        assert lad.classify_infra_error("id 132001 processed") is None

    def test_exceptions_are_classified_by_their_text(self):
        assert lad.classify_infra_error(RuntimeError("SpawnGateTimeout after 600s")) is not None


# ── L1 retry ────────────────────────────────────────────────────────────────


class TestL1:
    def test_retry_honours_retry_after(self, clock, rec):
        ladder = _ladder(clock)
        d = ladder.observe_failure(lad.L1_TOOL_CALL, "chat-1", retry_after_secs=30)
        assert d.action == lad.ACTION_RETRY
        assert d.retry is True
        assert d.attempt == 1
        assert d.delay_secs == 30.0  # floor: the hint is larger than the jittered 2s

    def test_retry_delay_is_jittered_and_capped_without_a_hint(self, clock, rec):
        ladder = _ladder(clock)
        d1 = ladder.observe_failure(lad.L1_TOOL_CALL, "chat-1")
        d2 = ladder.observe_failure(lad.L1_TOOL_CALL, "chat-1")
        assert 1.0 <= d1.delay_secs <= 2.0
        assert 2.0 <= d2.delay_secs <= 4.0
        assert d2.attempt == 2

    def test_third_failure_escalates_to_l2(self, clock, rec):
        ladder = _ladder(clock)
        ladder.observe_failure(lad.L1_TOOL_CALL, "chat-1")
        ladder.observe_failure(lad.L1_TOOL_CALL, "chat-1")
        d = ladder.observe_failure(lad.L1_TOOL_CALL, "chat-1")
        assert d.action == lad.ACTION_ESCALATE
        assert d.next_layer == lad.L2_BACKEND
        assert d.retry is False
        esc = [c for c in rec.counters if c["name"] == ev.RECOVERY_ESCALATIONS]
        assert esc[-1]["attrs"] == {"from_layer": lad.L1_TOOL_CALL, "to_layer": lad.L2_BACKEND}

    def test_units_do_not_share_attempts(self, clock, rec):
        ladder = _ladder(clock)
        ladder.observe_failure(lad.L1_TOOL_CALL, "a")
        ladder.observe_failure(lad.L1_TOOL_CALL, "a")
        assert ladder.observe_failure(lad.L1_TOOL_CALL, "b").retry is True

    def test_success_resets_and_measures_the_outage(self, clock, rec):
        ladder = _ladder(clock)
        ladder.observe_failure(lad.L1_TOOL_CALL, "a")
        clock.t += 45.0
        duration = ladder.observe_success(lad.L1_TOOL_CALL, "a")
        assert duration == 45.0
        assert ladder.attempts(lad.L1_TOOL_CALL, "a") == 0
        h = [x for x in rec.hists if x["name"] == ev.RECOVERY_DURATION_SECS]
        assert h[-1]["value"] == 45.0 and h[-1]["attrs"] == {"layer": lad.L1_TOOL_CALL}

    def test_success_without_an_open_run_measures_nothing(self, clock, rec):
        ladder = _ladder(clock)
        assert ladder.observe_success(lad.L1_TOOL_CALL, "never-failed") is None
        assert not rec.hists

    def test_cooldown_starts_the_count_over(self, clock, rec):
        ladder = _ladder(clock)
        ladder.observe_failure(lad.L1_TOOL_CALL, "a")
        ladder.observe_failure(lad.L1_TOOL_CALL, "a")
        clock.t += ladder.layer_policy(lad.L1_TOOL_CALL).cooldown_secs + 1
        assert ladder.observe_failure(lad.L1_TOOL_CALL, "a").attempt == 1

    def test_every_decision_is_counted_with_closed_attrs(self, clock, rec):
        ladder = _ladder(clock)
        ladder.observe_failure(lad.L1_TOOL_CALL, "a")
        c = [x for x in rec.counters if x["name"] == ev.RECOVERY_ATTEMPTS][-1]
        assert c["attrs"] == {"layer": lad.L1_TOOL_CALL, "action": lad.ACTION_RETRY}


# ── L2 -> L3 -> L4 -> L5 ──────────────────────────────────────────────────────


class TestEscalationChain:
    def test_l2_exhaustion_escalates_to_l3(self, clock, rec):
        ladder = _ladder(clock)
        assert ladder.observe_failure(lad.L2_BACKEND, "core").retry is True
        d = ladder.observe_failure(lad.L2_BACKEND, "core")
        assert (d.action, d.next_layer) == (lad.ACTION_ESCALATE, lad.L3_ACP_RUNTIME)

    def test_l3_exhaustion_escalates_to_l4(self, clock, rec):
        ladder = _ladder(clock)
        ladder.observe_failure(lad.L3_ACP_RUNTIME, "rt-1")
        d = ladder.observe_failure(lad.L3_ACP_RUNTIME, "rt-1")
        assert (d.action, d.next_layer) == (lad.ACTION_ESCALATE, lad.L4_GATEWAYD)

    def test_l4_second_respawn_in_window_notifies_l5_once(self, clock, rec):
        notes: list[tuple[str, str]] = []
        ladder = _ladder(clock, notifier=lambda layer, msg: notes.append((layer, msg)))
        first = ladder.observe_failure(lad.L4_GATEWAYD, "gatewayd", reason="daemon exited rc=1")
        assert first.action == lad.ACTION_RETRY  # one respawn per window is allowed
        clock.t += 60.0
        second = ladder.observe_failure(lad.L4_GATEWAYD, "gatewayd", reason="daemon exited rc=1")
        assert second.action == lad.ACTION_NOTIFY
        assert second.next_layer == lad.L5_GATEWAY
        assert notes and notes[0][0] == lad.L5_GATEWAY
        # A third failure in the same run does not spam the notifier.
        ladder.observe_failure(lad.L4_GATEWAYD, "gatewayd")
        assert len(notes) == 1
        # Recovery re-arms the notice for the next incident.
        ladder.observe_success(lad.L4_GATEWAYD, "gatewayd")
        clock.t += 1.0
        ladder.observe_failure(lad.L4_GATEWAYD, "gatewayd")
        ladder.observe_failure(lad.L4_GATEWAYD, "gatewayd")
        assert len(notes) == 2

    def test_l4_respawn_after_the_window_is_allowed_again(self, clock, rec):
        ladder = _ladder(clock)
        ladder.observe_failure(lad.L4_GATEWAYD, "gatewayd")
        clock.t += lad.GATEWAYD_RESPAWN_COOLDOWN_SECS + 1
        assert ladder.observe_failure(lad.L4_GATEWAYD, "gatewayd").retry is True

    def test_l5_is_never_automatic(self, clock, rec):
        notes: list = []
        ladder = _ladder(clock, notifier=lambda layer, msg: notes.append(layer))
        d = ladder.observe_failure(lad.L5_GATEWAY, "gateway")
        assert d.action == lad.ACTION_GIVE_UP
        assert d.retry is False
        assert d.delay_secs == 0.0
        assert notes == [lad.L5_GATEWAY]
        assert ladder.layer_policy(lad.L5_GATEWAY).automatic is False

    def test_a_raising_notifier_never_breaks_recovery(self, clock, rec):
        def boom(layer, msg):
            raise RuntimeError("no channel")

        ladder = _ladder(clock, notifier=boom)
        assert ladder.observe_failure(lad.L5_GATEWAY, "gateway").action == lad.ACTION_GIVE_UP


# ── event sink / restarts / table ────────────────────────────────────────────


class TestSinkAndMetrics:
    def test_event_sink_receives_one_recover_row_per_decision(self, clock, rec):
        rows: list = []
        ladder = _ladder(clock, event_sink=lambda tid, data: rows.append((tid, data)))
        ladder.observe_failure(lad.L1_TOOL_CALL, "a", task_id="task-1", reason="capacity")
        assert rows == [
            (
                "task-1",
                {
                    "layer": lad.L1_TOOL_CALL,
                    "attempt": 1,
                    "action": lad.ACTION_RETRY,
                    "delay_secs": rows[0][1]["delay_secs"],
                    "next_layer": None,
                    "reason": "capacity",
                },
            )
        ]

    def test_sink_is_skipped_without_a_task_id(self, clock, rec):
        rows: list = []
        ladder = _ladder(clock, event_sink=lambda tid, data: rows.append(tid))
        ladder.observe_failure(lad.L1_TOOL_CALL, "a")
        assert rows == []

    def test_record_restart_counts_per_layer(self, rec):
        lad.record_restart(lad.L4_GATEWAYD)
        c = [x for x in rec.counters if x["name"] == ev.RESTARTS_TOTAL][-1]
        assert c["attrs"] == {"layer": lad.L4_GATEWAYD}
        with pytest.raises(KeyError):
            lad.default_ladder().record_restart("L9")

    def test_table_rows_carry_every_column_the_rfc_names(self, clock):
        rows = _ladder(clock).table()
        assert [r["layer"] for r in rows] == list(lad.LAYERS)
        for r in rows:
            assert set(r) >= {
                "trigger",
                "cleanup_deadline_secs",
                "backoff_base_secs",
                "backoff_max_secs",
                "attempts_before_escalation",
                "escalates_to",
                "automatic",
            }
        by = {r["layer"]: r for r in rows}
        assert by[lad.L1_TOOL_CALL]["cleanup_deadline_secs"] is None
        assert by[lad.L2_BACKEND]["cleanup_deadline_secs"] == lad.POOL_SHUTDOWN_SECS
        assert by[lad.L4_GATEWAYD]["cleanup_deadline_secs"] == lad.TOTAL_SHUTDOWN_BUDGET_SECS
        assert by[lad.L5_GATEWAY]["automatic"] is False


# ── the layers actually read the shared schedule ─────────────────────────────


class TestLayersShareTheSchedule:
    def test_gatewayd_supervisor_reads_l4(self):
        from kiro_crew.mcp_gateway import manager

        l4 = lad.LADDER.layer(lad.L4_GATEWAYD)
        assert manager._RESPAWN_BACKOFF_START_SECS == l4.base_secs
        assert manager._RESPAWN_BACKOFF_MAX_SECS == l4.max_secs

    def test_supervisor_backoff_is_jittered_and_bounded(self, monkeypatch):
        from kiro_crew.mcp_gateway import manager

        floor, cap = manager._RESPAWN_BACKOFF_START_SECS, manager._RESPAWN_BACKOFF_MAX_SECS
        seen = set()
        cur = floor
        for _ in range(40):
            nxt = manager.GatewayManager._next_respawn_backoff(cur)
            assert floor <= nxt <= cap
            seen.add(round(nxt, 6))
            cur = nxt
        assert cur == cap or cap - cur < cap / 2  # converges toward the cap
        assert len(seen) > 5
        # A test that pins the floor to 0 gets a 0 delay, not a jittered one.
        monkeypatch.setattr(manager, "_RESPAWN_BACKOFF_START_SECS", 0.0)
        assert manager.GatewayManager._next_respawn_backoff(0.0) >= 0.0

    def test_acp_client_reads_l3(self):
        from kiro_crew.acp import client

        assert client._ACP_RESPAWN_BACKOFF_S == lad.LADDER.layer(lad.L3_ACP_RUNTIME).base_secs

    def test_task_store_reads_the_shared_schedule(self):
        from kiro_crew.taskq import model

        assert model.RECOVERY_BACKOFF_BASE_SECS == lad.LADDER.base_secs
        assert model.RECOVERY_BACKOFF_MAX_SECS == lad.LADDER.max_secs
        assert model.recovery_backoff_secs(0) == lad.LADDER.base_secs

    def test_no_layer_keeps_a_private_backoff_literal(self):
        """The literals the ladder replaced must not come back."""
        import inspect

        from kiro_crew.acp import client
        from kiro_crew.mcp_gateway import manager
        from kiro_crew.taskq import model

        assert "_RESPAWN_BACKOFF_START_SECS = 1.0" not in inspect.getsource(manager)
        assert "_RESPAWN_BACKOFF_MAX_SECS = 60.0" not in inspect.getsource(manager)
        assert "_ACP_RESPAWN_BACKOFF_S = 2.0" not in inspect.getsource(client)
        assert "RECOVERY_BACKOFF_BASE_SECS = 2.0" not in inspect.getsource(model)


def test_manager_records_an_l4_restart_and_escalation(monkeypatch):
    """The supervisor's respawn path is what feeds L4 (source-level pin)."""
    import inspect

    from kiro_crew.mcp_gateway import manager

    src = inspect.getsource(manager.GatewayManager._run_watchdog)
    assert "record_restart(L4_GATEWAYD)" in src
    assert "observe_failure(" in src and "L4_GATEWAYD" in src
    assert "observe_success(L4_GATEWAYD" in src
