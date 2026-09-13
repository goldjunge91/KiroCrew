"""Acceptance map for the overload-resilience PR (SPEC-ADDENDUM §10 + SPEC-INTERACTIVE §9).

One test per owner scenario. Each docstring names the OWNER tests that carry
the full behavioural proof (they are not imported: the map is documentation a
reader can grep, the assertion here is a smoke check through the public API of
the component the scenario lands on). A few scenarios run the fake-harness
experiment driver at a tiny N -- real ``SubagentManager`` admission, real
``taskq`` store, real coordinator, virtual clock, no kiro-cli, no sockets.

The two defect tests (``D1``, ``D2``) were strict ``xfail`` while the defects
stood; both are fixed and the tests must PASS. ``D1``/``D2`` are described in
``docs/reports/overload-resilience-experiment.md`` §4.
"""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]
_DRIVER = _REPO_ROOT / "scripts" / "experiments" / "overload_2000.py"

pytestmark = pytest.mark.usefixtures("healthy_host_memory")


def _driver():
    spec = importlib.util.spec_from_file_location("overload_2000", _DRIVER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules.setdefault("overload_2000", module)
    spec.loader.exec_module(module)
    return module


async def _run(scenario: str, *, n: int, user_max: int | None = None, faults=None):
    drv = _driver()
    if faults is None:
        faults = drv.faults_for(scenario, n)
    if user_max is None:
        user_max = 2 if scenario == "tree" else 10
    h = drv.Harness(scenario=scenario, n=n, faults=faults, user_max=user_max, seed=3)
    try:
        await h.drive(max_virtual_secs=2 * 3600.0)
    finally:
        h.close()
    return h.metrics


# ── SPEC-ADDENDUM §10 ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_a01_three_level_tree_leaf_waits_unrelated_tasks_complete():
    """Owners: test_taskq_nested_propagation.py::
    test_three_level_tree_leaf_waits_and_unrelated_work_completes,
    test_fairness_lanes.py::test_child_reserve_three_level_tree_at_cap_2.
    Smoke: the driver's tree scenario (S -> A -> B, cap 2, child_reserve 1)
    finishes every root and every child with zero lost or duplicated work."""
    m = await _run("tree", n=8)
    assert m.tree["max_depth"] == 2
    assert m.tree["roots_done"] == m.tree["roots"]
    assert m.tree["children_done"] == m.tree["children"]
    assert m.tree["waiting_children_entered"] >= m.tree["roots"]
    assert m.lost == 0 and m.duplicate_dispatches == 0 and m.remaining == 0
    assert m.max_in_flight <= 2


def test_a02_all_slots_held_by_waiting_parents_still_advances():
    """Owners: test_taskq_nested_propagation.py::
    test_all_slots_held_by_waiting_parents_still_progresses,
    test_fairness_lanes.py::test_child_reserve_three_level_tree_at_cap_2.
    Smoke: the dispatcher's ``CapacityView`` -- two parents holding a cap of 2
    admit nothing; once they yield their lane slots a nested start fits while a
    ROOT still may not take the reserve."""
    from kiro_crew.subagent_manager.admission import CapacityView

    held = CapacityView(
        cap_total=2, running=2, child_reserve=1, reserve_active=True, waiting_parents=0
    )
    assert not held.any_slot and not held.root_slot
    yielded = CapacityView(
        cap_total=2, running=1, child_reserve=1, reserve_active=True, waiting_parents=2
    )
    assert yielded.any_slot and not yielded.root_slot  # only the child reserve is left
    assert yielded.roots_cap == 1


def test_a03_many_sessions_same_throttle_one_coordinated_schedule():
    """Owners: test_dependency_coordinator.py::
    test_five_sessions_one_scope_one_schedule_one_probe,
    test_probe_failing_costs_the_scope_one_attempt_not_five,
    test_runloop_integration.py::test_throttle_parks_two_runs_on_one_scope_and_wakes_by_capacity.
    Smoke: three tasks from three sessions report a 429 on ONE scope -> one
    schedule, one retry_at, one probe on tick."""
    from kiro_crew.taskq.dependency import (
        KIND_RATE_LIMITED,
        DependencyCoordinator,
        DependencySignal,
    )

    now = [1000.0]
    coord = DependencyCoordinator(None, clock=lambda: now[0], capacity=lambda: 4)
    for tid in ("s1-t", "s2-t", "s3-t"):
        v = coord.report(tid, DependencySignal(KIND_RATE_LIMITED, "provider:x", "acp"))
        assert v.outcome == "wait"
    assert coord.scopes() == ["provider:x"]
    assert coord.schedule("provider:x").attempts == 1
    now[0] = coord.next_deadline() + 0.01
    assert len(coord.tick()) == 1  # the probe, never all three


def test_a04_fault_isolation_between_dependencies():
    """Owners: test_dependency_coordinator.py::
    test_throttled_scope_does_not_block_another_scope,
    test_failing_one_scope_fails_only_its_waiters.
    Smoke: a scope parked far in the future never delays a due scope."""
    from kiro_crew.taskq.dependency import (
        KIND_DEPENDENCY_UNAVAILABLE,
        KIND_RATE_LIMITED,
        DependencyCoordinator,
        DependencySignal,
    )

    now = [1000.0]
    coord = DependencyCoordinator(None, clock=lambda: now[0], capacity=lambda: 4)
    coord.report("slow", DependencySignal(KIND_RATE_LIMITED, "github:api", "gh", retry_at=5000.0))
    coord.report("quick", DependencySignal(KIND_DEPENDENCY_UNAVAILABLE, "db:main", "sql"))
    now[0] = coord.schedule("db:main").retry_at + 0.01
    woken = coord.tick()
    assert woken == ["quick"]
    assert coord.waiters("github:api") == ["slow"]


def test_a05_bounded_procs_fds_memory_while_waiting():
    """Owners: test_mcp_gateway_host_budget.py, test_taskq_waits.py::
    test_park_ends_residency_into_retry_wait, test_record_refuses_model_text_alone_and_kind_mismatch.
    Smoke: the budget refuses past its ceiling before anything is spawned, and a
    live wait record cannot claim its residency is free."""
    from kiro_crew.mcp_gateway.host_budget import HostBudget, HostBudgetExhausted, HostBudgetLimits
    from kiro_crew.taskq import WaitRecord

    budget = HostBudget(HostBudgetLimits(max_procs=2))
    a = budget.reserve(label="a")
    budget.reserve(label="b")
    with pytest.raises(HostBudgetExhausted):
        budget.reserve(label="c")
    a.release()
    a.release()  # idempotent
    assert budget.procs_in_use == 1
    with pytest.raises(ValueError):
        WaitRecord.dependency("x", since=1.0, retry_at=2.0).__class__(
            **{
                **WaitRecord.dependency("x", since=1.0, retry_at=2.0).__dict__,
                "residency_charged": False,
            }
        )


def test_a06_gradual_wake_on_dependency_recovery():
    """Owners: test_dependency_coordinator.py::
    test_probe_then_capacity_sized_batches_with_spacing,
    test_recovered_signal_wakes_now_but_still_staged.
    Smoke: six waiters, capacity 2 -> probe of 1, then batches of 2 spaced by
    ``wake_spacing_secs``; never six at once."""
    from kiro_crew.taskq.dependency import (
        KIND_DEPENDENCY_UNAVAILABLE,
        DependencyCoordinator,
        DependencySignal,
    )

    now = [0.0]
    coord = DependencyCoordinator(
        None, clock=lambda: now[0], capacity=lambda: 2, wake_spacing_secs=1.0
    )
    for i in range(6):
        coord.report(f"t{i}", DependencySignal(KIND_DEPENDENCY_UNAVAILABLE, "svc", "adapter"))
    assert coord.recovered("svc")
    batches = []
    for _ in range(6):
        if not coord.waiters("svc"):
            break
        now[0] = max(now[0], coord.next_deadline())
        batches.append(len(coord.tick()))
    assert batches[0] == 1
    assert all(b <= 2 for b in batches)
    assert sum(batches) == 6


def test_a07_unexpected_interactive_command_recovers_and_real_input_wait_is_shown():
    """Owners: test_interactive_command_detection.py::
    test_stuck_input_yields_waiting_input_status_before_the_stall_terminal,
    test_no_auto_answer_on_the_cancel_policy_either, test_session_health.py.
    Smoke: a pager-shaped command is classified before dispatch; a real input
    wait is a ``waiting_input`` record with ``cancel_call`` semantics."""
    from kiro_crew.acp.liveness import INTERACTIVE_PAGER, classify_interactive_command
    from kiro_crew.taskq import WAITING_INPUT, WaitRecord
    from kiro_crew.taskq.waits import CANCEL_CALL

    assert classify_interactive_command("git log").risk == INTERACTIVE_PAGER
    rec = WaitRecord.input("tool-call-7", since=10.0)
    assert rec.state == WAITING_INPUT and rec.cancel_semantics == CANCEL_CALL
    assert WaitRecord.from_dict(rec.to_dict()) == rec


def test_a08_long_silent_task_is_not_killed():
    """Owners: test_acp_liveness.py, test_acp_liveness_darwin.py::
    test_matched_live_child_is_working_and_tracked,
    test_interactive_command_detection.py::
    test_platform_limited_flat_on_a_plain_command_keeps_the_build_scale_window.
    Smoke: a plain build command carries no interactive risk, so silence alone
    never narrows its window; a clean completion is a success to the controller."""
    from kiro_crew.acp.liveness import NOT_INTERACTIVE, classify_interactive_command
    from kiro_crew.adaptive.controller import OUTCOME_SUCCESS, classify_run_outcome

    assert classify_interactive_command("make -j8 all") == NOT_INTERACTIVE

    class _Run:
        error = ""

    assert classify_run_outcome(_Run()) == OUTCOME_SUCCESS


def test_a09_black_box_wedged_tool_is_bounded_and_partial_kept():
    """Owners: test_subagent_stop_reason_consistency.py::
    test_tool_stall_is_never_recorded_as_success (partial preserved),
    test_interactive_command_detection.py::test_platform_limited_flat_on_a_plain_command_is_still_bounded.
    Smoke: the stall class is recoverable, bounded by the shared ladder constant."""
    from kiro_crew.acp.types import (
        STOP_CLASS_STALLED,
        STOP_RECOVERY_MAX_RETRIES,
        classify_stop_reason,
    )
    from kiro_crew.recovery.ladder import SESSION_RECOVERY_MAX_ATTEMPTS

    cls = classify_stop_reason("error: tool stall")
    assert cls.name == STOP_CLASS_STALLED and cls.recoverable and not cls.is_success
    assert STOP_RECOVERY_MAX_RETRIES == SESSION_RECOVERY_MAX_ATTEMPTS == 3


def test_a10_event_complete_with_tool_stall_is_not_success():
    """Owners: test_subagent_stop_reason_consistency.py::
    test_tool_stall_is_never_recorded_as_success, test_no_stop_reason_is_ever_an_unknown_success,
    test_chat_runner_has_no_private_stop_reason_mapping.
    Smoke: the one classifier maps every non-end_turn reason away from success."""
    from kiro_crew.acp.types import classify_stop_reason

    assert classify_stop_reason("end_turn").is_success
    for reason in (
        "error: tool stall",
        "stale_recover",
        "cancelled",
        "error: process exited (exit 137)",
    ):
        assert not classify_stop_reason(reason).is_success, reason
    unknown = classify_stop_reason("something new")
    assert not unknown.is_success and not unknown.known


def test_a11_blocking_wait_timeout_loses_nothing_duplicates_nothing():
    """Owners: test_mcp_core_spawn_sub_agents.py::
    test_wait_expiry_reports_still_running_never_failed,
    test_subagent_stop_reason_consistency.py::test_blocking_wait_expiry_does_not_fail_the_child.
    Smoke: the blocking tool's expiry path names ``still_running`` and never
    cancels a child (source pin)."""
    src = (_REPO_ROOT / "src" / "kiro_crew" / "mcp_tools" / "spawn.py").read_text()
    assert '"still_running"' in src
    assert "task_ids" in src


@pytest.mark.asyncio
async def test_a12_gateway_restart_during_a_wait_rebuilds_identity_and_links():
    """Owners: test_taskq_nested_propagation.py::test_restart_rebuilds_tree_from_the_store,
    test_dependency_coordinator.py::test_rebuild_restores_scopes_waiters_and_backoff,
    test_session_start_gate.py::test_timeout_then_late_response_is_adopted.
    Smoke: the driver's restart scenario -- store closed mid-run, fresh manager
    reconciles; queued rows survive under their ids, no terminal regresses, the
    real ``StartCollector`` adopts a late ``session/new`` instead of requeuing."""
    m = await _run("restart", n=40)
    assert m.lost == 0 and m.remaining == 0 and m.duplicate_dispatches == 0
    assert m.restart["queued_survived"] is True
    assert m.restart["terminal_regressed"] is False
    assert m.restarts == 1
    assert m.start_collector.get("outcome") == "adopted"
    assert m.start_collector.get("gate_releases") == 1


def test_a13_cancel_restart_late_result_lease_expiry_no_resurrection(tmp_path):
    """Owners: test_taskq_claim_fencing.py::test_cancel_during_run_beats_late_completion,
    test_stale_generation_result_is_rejected_and_logged, test_terminal_never_regresses,
    test_recovering_row_with_expired_lease_is_reclaimable.
    Smoke: cancel bumps the generation; the old worker's late ``done`` is fenced."""
    from kiro_crew.taskq import CANCELLED, DONE, KIND_SUBAGENT, TaskRecord, TaskStore

    store = TaskStore(tmp_path / "tasks.db").open()
    try:
        store.accept_one(TaskRecord(id="t1", kind=KIND_SUBAGENT, session_key="dash:1", params={}))
        claimed = store.claim("t1")
        assert claimed is not None
        assert store.cancel("t1", reason="user_stop") is not None
        assert store.state_of("t1") == CANCELLED
        assert not store.finish("t1", DONE, generation=claimed.generation)
        assert store.state_of("t1") == CANCELLED
        assert any(e.kind == "stale_result" for e in store.events("t1"))
        assert store.claim("t1") is None
    finally:
        store.close()


def test_a14_native_subagent_declared_boundary_holds():
    """Owners: test_native_subagent_boundary.py (22 tests), incl.
    test_native_children_take_no_host_budget_slot, test_parent_cancel_is_the_only_lever_and_covers_every_child.
    Smoke: native children are OBSERVED on the budget, never charged."""
    from kiro_crew.mcp_gateway.host_budget import HostBudget, HostBudgetLimits

    budget = HostBudget(HostBudgetLimits(max_procs=1))
    budget.reserve(label="parent")
    budget.report_uncharged("native_children", 200, label="parent")
    assert budget.procs_in_use == 1
    assert budget.uncharged("native_children") == 200
    assert budget.snapshot()["uncharged"] == {"native_children": 200}


# ── SPEC-INTERACTIVE §9 ──────────────────────────────────────────────────────


def test_i05_nested_question_surfaces_once_and_binds_to_task_attempt():
    """Owners: test_taskq_runner_adapter.py::test_waiting_input_resumes_with_the_answer,
    test_acp_structured_status.py (the per-handle question binding of the
    controlled terminal ships in a follow-up PR). Smoke: the question shape is
    a ``kirocrew/status`` v1 ``waiting_input`` frame."""
    from kiro_crew.acp.types import STATUS_EXTENSION_VERSION, WAIT_REASON_INPUT

    assert STATUS_EXTENSION_VERSION == 1
    assert WAIT_REASON_INPUT == "waiting_input"


def test_i08_cancel_or_replacement_cleans_up_and_rejects_late_answers():
    """Owners: test_taskq_claim_fencing.py::test_cancel_during_run_beats_late_completion,
    test_taskq_runner_adapter.py::test_waiting_input_cancelled_returns_none."""
    from kiro_crew.taskq import CANCELLED, TRANSITIONS, is_terminal

    assert is_terminal(CANCELLED)
    assert not TRANSITIONS.get(CANCELLED)  # nothing leaves a terminal state


def test_i09_parent_filling_the_cap_never_starves_children():
    """Owners: test_fairness_lanes.py::test_child_reserve_three_level_tree_at_cap_2,
    test_taskq_nested_propagation.py::test_all_slots_held_by_waiting_parents_still_progresses.
    Smoke: with the reserve active, roots_cap is cap - reserve."""
    from kiro_crew.subagent_manager.admission import CapacityView, FairnessSettings

    assert FairnessSettings().child_reserve == 1
    view = CapacityView(
        cap_total=4, running=3, child_reserve=1, reserve_active=True, waiting_parents=1
    )
    assert view.any_slot and not view.root_slot


def test_i10_tool_stall_or_error_completion_is_not_success_and_keeps_partial():
    """Owners: test_subagent_stop_reason_consistency.py::test_tool_stall_is_never_recorded_as_success,
    test_taskrunner_persistent_tool_stall_step_is_failed."""
    from kiro_crew.acp.types import STOP_CLASS_FAILED, classify_stop_reason

    assert classify_stop_reason("error: compaction failed").name == STOP_CLASS_FAILED
    assert classify_stop_reason("error: compaction failed", compaction_transient=True).recoverable


def test_i11_worker_or_gateway_restart_separates_recoverable_from_dead_handles():
    """Owners: test_taskq_reconcile.py,
    test_session_start_gate.py::test_timeout_then_late_response_is_adopted.
    """
    from kiro_crew.taskq import CLAIMABLE, RECOVERING, UNKNOWN_SIDE_EFFECT, is_terminal

    assert RECOVERING in CLAIMABLE  # a lost owner with a safe class is re-claimed
    assert is_terminal(UNKNOWN_SIDE_EFFECT)  # an unknown class is never silently re-run


@pytest.mark.asyncio
async def test_i12_many_waiting_tasks_and_normal_tasks_coexist_bounded():
    """Owners: test_taskq_waits.py,
    test_runloop_integration.py::test_capacity_refused_tool_call_is_retried_in_place_via_ladder_and_scope.
    Smoke: the driver's outage scenario -- the gate is unreachable for a virtual
    20 min; runs park on ONE scope with their residency charged, nothing is
    severed, everything completes after the daemon returns."""
    drv = _driver()
    m = await _run("outage", n=30, faults=drv.Faults(outage=(2.0, 20 * 60.0)))
    assert m.failed == 0 and m.lost == 0 and m.remaining == 0 and m.duplicate_dispatches == 0
    assert m.completed == 30
    assert m.coordinator_scopes_seen == 1
    assert m.host_budget_peaks["procs"] <= m.user_max
    assert m.outage_recovery_secs is not None


# ── defects found by the experiment ──────────────────────────────────────────


def test_d1_two_lifetime_gate_failures_must_not_pin_the_cap_forever():
    """D1 (fixed): ``classify`` reads ``Sample.gate_failures_in_window``, a
    delta the policy derives from the daemon's LIFETIME ``outcomes.failure``
    counter over ``gate_failure_window_secs``; two failures that happened before
    the window cannot pin the cap."""
    from kiro_crew.adaptive.policy import ACTION_INCREASE, AdaptivePolicy, PolicyParams
    from kiro_crew.adaptive.signals import Sample, SpawnGateStats

    policy = AdaptivePolicy(PolicyParams(exec_ceiling=10, exec_initial=4))
    t = 0.0
    actions = set()
    # Two failures happened long ago; every later window is clean, demand is
    # at the cap and completions pile up -- an increase must be earned.
    for step in range(1, 60):
        t += 5.0
        sample = Sample(
            t=t,
            free_mem_mb=8192.0,
            completions=step * 5,
            running=4,
            queued=50,
            spawn_gate=SpawnGateStats(
                capacity=4, in_flight=4, queued=2, successes=step * 5, failures=2
            ),
        )
        actions.add(policy.observe(sample).action)
    assert ACTION_INCREASE in actions


@pytest.mark.asyncio
async def test_spawn_gate_windowed_failures_let_the_cap_recover():
    """The counterpart of D1: with the failure counter presented as a 60 s
    window (what the fix must do), the driver's controller scenario decreases
    under injected start timeouts and earns its way back up afterwards."""
    m = await _run("controller", n=200)
    caps = [row["exec_cap"] for row in m.effective_cap_timeline]
    assert caps[0] == 10
    assert min(caps) < 10
    assert m.controller_counts.get("decrease", 0) >= 1
    assert m.controller_counts.get("increase", 0) >= 1
    assert caps[-1] > min(caps)
    assert m.lost == 0 and m.duplicate_dispatches == 0 and m.remaining == 0


def test_experiment_driver_refuses_the_default_data_home(monkeypatch, tmp_path):
    """RFC §11: the driver never writes into the real ``~/.kiro/crew``."""
    drv = _driver()
    monkeypatch.setenv("KIROCREW_SCRATCH", str(tmp_path))
    monkeypatch.setenv("KIROCREW_HOME", str(Path.home() / ".kiro" / "crew"))
    home = drv.ensure_experiment_home()
    assert home.resolve() != (Path.home() / ".kiro" / "crew").resolve()
    assert str(home).startswith(str(tmp_path))
    assert os.environ["KIROCREW_HOME"] == str(home)


@pytest.mark.asyncio
async def test_d2_gatewayd_outage_longer_than_a_minute_must_not_fail_the_scope():
    """D2 (fixed): an ``mcp_gateway:*`` scope's budget is the wait DEADLINE, not
    ``dependency_max_attempts`` probes, so even with a ~2 s ladder delay fed as
    ``retry_at`` (the pre-fix run.py shape this fault knob reproduces) a 20-minute
    outage fails no waiter; run.py itself passes only a server-stated delay."""
    drv = _driver()
    m = await _run(
        "outage",
        n=30,
        faults=drv.Faults(outage=(2.0, 20 * 60.0), infra_retry_at_from_ladder=True),
    )
    assert m.failed == 0 and m.completed == 30
