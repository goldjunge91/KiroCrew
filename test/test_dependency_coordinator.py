"""DependencyCoordinator: one schedule per scope, staged wake by capacity, bounded waits."""

from __future__ import annotations

import random
from pathlib import Path

import pytest
from overload_fakes import Clock, FixedRng, backoff, open_task_store

from kiro_crew.taskq import model
from kiro_crew.taskq.dependency import (
    EVENT_FAILED,
    EVENT_WAIT,
    EVENT_WAKE,
    KIND_AUTH_FAILED,
    KIND_PERMANENT_PARAM_ERROR,
    KIND_QUOTA_EXHAUSTED,
    KIND_RATE_LIMITED,
    PHASE_PROBE,
    PHASE_RAMP,
    PHASE_WAITING,
    DependencyCoordinator,
    DependencySignal,
    coordinator_from_config,
)
from kiro_crew.taskq.store import TaskStore
from kiro_crew.taskq.waits import WaitRecord

T0 = 1_000_000.0
GH = "github:api"
BEDROCK = "provider:model-x"


def _signal(
    kind: str = KIND_RATE_LIMITED, scope: str = GH, retry_at: float | None = None
) -> DependencySignal:
    return DependencySignal(kind=kind, dependency_scope=scope, source="test", retry_at=retry_at)


@pytest.fixture
def clock() -> Clock:
    return Clock(T0)


@pytest.fixture
def store(tmp_path: Path, clock: Clock) -> TaskStore:
    yield from open_task_store(tmp_path, clock, name="tasks/tasks.db", window=64)


def _running(store: TaskStore, task_id: str, *, session: str = "s") -> int:
    """Accept, claim and start *task_id* so it is a live ``running`` row; returns its generation."""
    rec = model.TaskRecord(
        id=task_id, kind=model.KIND_SUBAGENT, session_key=session, params={"t": task_id}
    )
    store.accept_one(rec)
    claim = store.claim(task_id, owner="w")
    assert claim is not None
    store.transition(task_id, model.STARTING, generation=claim.generation)
    store.transition(task_id, model.RUNNING, generation=claim.generation)
    return claim.generation


def _starting(store: TaskStore, task_id: str) -> int:
    rec = model.TaskRecord(
        id=task_id, kind=model.KIND_SUBAGENT, session_key="s", params={"t": task_id}
    )
    store.accept_one(rec)
    claim = store.claim(task_id, owner="w")
    assert claim is not None
    store.transition(task_id, model.STARTING, generation=claim.generation)
    return claim.generation


def _coord(store: TaskStore | None, clock: Clock, **kw) -> DependencyCoordinator:
    kw.setdefault("rng", FixedRng())
    kw.setdefault("backoff", backoff(2.0, 900.0))
    kw.setdefault("wake_spacing_secs", 5.0)
    return DependencyCoordinator(store, clock=clock, **kw)


def _events(store: TaskStore, task_id: str, kind: str) -> list[dict]:
    return [e.data for e in store.events(task_id) if e.kind == kind]


# ── one schedule per scope ────────────────────────────────────────────────────


class TestOneSchedulePerScope:
    def test_five_sessions_one_scope_one_schedule_one_probe(
        self, store: TaskStore, clock: Clock
    ) -> None:
        woken: list[str] = []
        coord = _coord(store, clock, capacity=lambda: 8, on_wake=woken.append)
        gens = {f"t{i}": _running(store, f"t{i}", session=f"session-{i}") for i in range(5)}
        verdicts = [coord.report(tid, _signal(), generation=gen) for tid, gen in gens.items()]

        assert coord.scopes() == [GH]
        sched = coord.schedule(GH)
        assert sched is not None
        assert sched.attempts == 1
        assert len(sched.waiters) == 5
        # Every waiter shares the ONE retry instant: base 2s (FixedRng = ceiling).
        assert {v.retry_at for v in verdicts} == {T0 + 2.0}
        assert all(v.outcome == "wait" and v.state == model.WAITING_DEPENDENCY for v in verdicts)
        for tid in gens:
            row = store.get(tid)
            assert row is not None and row.state == model.WAITING_DEPENDENCY
            record = WaitRecord.from_dict(row.wait)
            assert record is not None and record.dependency_scope == GH
            assert record.resume_condition.at == T0 + 2.0

        # Nothing wakes before retry_at.
        clock.advance(1.0)
        assert coord.tick() == []
        # At retry_at exactly ONE probe is released, not five.
        clock.advance(1.0)
        assert coord.tick() == ["t0"]
        assert woken == ["t0"]
        # A woken PARKED row (no live run to hand the slot to) is claimable
        # ``retry_wait`` -- the dispatcher writes ``running`` on claim.
        assert store.state_of("t0") == model.RETRY_WAIT
        assert all(store.state_of(f"t{i}") == model.WAITING_DEPENDENCY for i in range(1, 5))
        sched = coord.schedule(GH)
        assert sched is not None and sched.phase == PHASE_PROBE and sched.in_flight == {"t0"}

    def test_probe_failing_costs_the_scope_one_attempt_not_five(
        self, store: TaskStore, clock: Clock
    ) -> None:
        coord = _coord(store, clock, capacity=lambda: 8)
        gens = {f"t{i}": _running(store, f"t{i}") for i in range(5)}
        for tid, gen in gens.items():
            coord.report(tid, _signal(), generation=gen)
        clock.advance(2.0)
        assert coord.tick() == ["t0"]
        # The probe hits the wall again (new generation after the wake).
        new_gen = store.get("t0").generation  # type: ignore[union-attr]
        verdict = coord.report("t0", _signal(), generation=new_gen)
        sched = coord.schedule(GH)
        assert sched is not None
        assert sched.attempts == 2
        assert sched.phase == PHASE_WAITING
        assert verdict.retry_at == T0 + 2.0 + 4.0  # base·2^(2-1)
        assert len(sched.waiters) == 5 and not sched.in_flight
        # Nobody else was woken by the failed probe.
        assert len(_events(store, "t1", EVENT_WAKE)) == 0

    def test_later_joiner_does_not_start_a_second_timer(
        self, store: TaskStore, clock: Clock
    ) -> None:
        coord = _coord(store, clock)
        g1 = _running(store, "a")
        coord.report("a", _signal(), generation=g1)
        clock.advance(1.0)
        g2 = _running(store, "b")
        v = coord.report("b", _signal(), generation=g2)
        sched = coord.schedule(GH)
        assert sched is not None and sched.attempts == 1
        assert v.retry_at == T0 + 2.0  # the existing instant, not now + backoff

    def test_server_retry_at_is_honoured_exactly_and_only_extends(
        self, store: TaskStore, clock: Clock
    ) -> None:
        coord = _coord(store, clock, backoff=backoff(2.0, 10.0))
        g1 = _running(store, "a")
        v1 = coord.report("a", _signal(retry_at=T0 + 600.0), generation=g1)
        assert v1.retry_at == T0 + 600.0  # not clamped by the 10s backoff cap
        g2 = _running(store, "b")
        v2 = coord.report("b", _signal(retry_at=T0 + 300.0), generation=g2)
        assert v2.retry_at == T0 + 600.0  # an earlier reset never shortens
        g3 = _running(store, "c")
        v3 = coord.report("c", _signal(retry_at=T0 + 900.0), generation=g3)
        assert v3.retry_at == T0 + 900.0  # a later reset extends
        clock.advance(899.0)
        assert coord.tick() == []
        clock.advance(1.0)
        assert coord.tick() == ["a"]


# ── fault isolation ───────────────────────────────────────────────────────────


class TestFaultIsolation:
    def test_throttled_scope_does_not_block_another_scope(
        self, store: TaskStore, clock: Clock
    ) -> None:
        coord = _coord(store, clock, capacity=lambda: 4)
        g_gh = _running(store, "gh-task")
        g_br = _running(store, "bedrock-task")
        coord.report("gh-task", _signal(retry_at=T0 + 3600.0), generation=g_gh)
        coord.report("bedrock-task", _signal(scope=BEDROCK), generation=g_br)
        assert coord.scopes() == [GH, BEDROCK]
        clock.advance(2.0)
        assert coord.tick() == ["bedrock-task"]
        assert store.state_of("bedrock-task") == model.RETRY_WAIT  # claimable, re-dispatched
        assert store.state_of("gh-task") == model.WAITING_DEPENDENCY
        # The bedrock task completes: its scope is gone, GitHub's schedule untouched.
        coord.forget("bedrock-task")
        assert coord.scopes() == [GH]
        gh = coord.schedule(GH)
        assert gh is not None and gh.retry_at == T0 + 3600.0

    def test_failing_one_scope_fails_only_its_waiters(self, store: TaskStore, clock: Clock) -> None:
        coord = _coord(store, clock, wait_deadline_secs=100.0)
        g_gh = _running(store, "gh-task")
        g_br = _running(store, "bedrock-task")
        coord.report("gh-task", _signal(retry_at=T0 + 50.0), generation=g_gh)
        coord.report("bedrock-task", _signal(scope=BEDROCK, retry_at=T0 + 500.0), generation=g_br)
        # GitHub's probe keeps failing until its deadline passes.
        clock.advance(50.0)
        assert coord.tick() == ["gh-task"]
        clock.advance(51.0)
        gen = store.get("gh-task").generation  # type: ignore[union-attr]
        v = coord.report("gh-task", _signal(), generation=gen)
        assert v.outcome == "deadline"
        assert store.state_of("gh-task") == model.FAILED
        assert store.state_of("bedrock-task") == model.WAITING_DEPENDENCY
        assert coord.scopes() == [BEDROCK]


# ── staged wake ───────────────────────────────────────────────────────────────


class TestStagedWake:
    def test_probe_then_capacity_sized_batches_with_spacing(
        self, store: TaskStore, clock: Clock
    ) -> None:
        woken: list[str] = []
        coord = _coord(
            store, clock, capacity=lambda: 3, wake_spacing_secs=5.0, on_wake=woken.append
        )
        gens = {f"t{i}": _running(store, f"t{i}") for i in range(8)}
        for tid, gen in gens.items():
            coord.report(tid, _signal(retry_at=T0 + 10.0), generation=gen)
        clock.advance(10.0)
        assert coord.tick() == ["t0"]  # probe
        assert coord.tick() == []  # spacing not elapsed
        clock.advance(4.0)
        assert coord.tick() == []
        clock.advance(1.0)
        assert coord.tick() == ["t1", "t2", "t3"]  # ramp: capacity 3
        sched = coord.schedule(GH)
        assert sched is not None and sched.phase == PHASE_RAMP
        clock.advance(5.0)
        assert coord.tick() == ["t4", "t5", "t6"]
        clock.advance(5.0)
        assert coord.tick() == ["t7"]
        assert woken == [f"t{i}" for i in range(8)]
        # Every woken row is claimable again (``retry_wait``, due now) under a
        # NEW generation; ``running`` is written by whoever claims it.
        for tid, gen in gens.items():
            row = store.get(tid)
            assert row is not None and row.state == model.RETRY_WAIT and row.generation == gen + 1
            assert row.next_run_at is not None and row.next_run_at <= clock.t
            assert row.lease_owner is None
            assert len(_events(store, tid, EVENT_WAKE)) == 1

    def test_probe_completion_starts_the_ramp_early(self, store: TaskStore, clock: Clock) -> None:
        coord = _coord(store, clock, capacity=lambda: 2, wake_spacing_secs=60.0)
        gens = {f"t{i}": _running(store, f"t{i}") for i in range(4)}
        for tid, gen in gens.items():
            coord.report(tid, _signal(retry_at=T0 + 1.0), generation=gen)
        clock.advance(1.0)
        assert coord.tick() == ["t0"]
        coord.forget("t0")  # the probe finished its work: the dependency is back
        assert coord.tick() == ["t1", "t2"]
        clock.advance(60.0)
        assert coord.tick() == ["t3"]
        # Nothing left to wake; the scope lingers only to count a late probe
        # failure and is dropped once every woken task has finished.
        assert coord.waiters(GH) == []
        for tid in ("t1", "t2", "t3"):
            coord.forget(tid)
        assert coord.scopes() == []

    def test_scope_with_only_in_flight_tasks_ages_out(self, store: TaskStore, clock: Clock) -> None:
        coord = _coord(store, clock, backoff=backoff(2.0, 100.0))
        gen = _running(store, "a")
        coord.report("a", _signal(retry_at=T0 + 1.0), generation=gen)
        clock.advance(1.0)
        assert coord.tick() == ["a"]
        assert coord.scopes() == [GH]  # a silent finisher is kept until the cap passes
        clock.advance(100.0)
        coord.tick()
        assert coord.scopes() == [GH]
        clock.advance(1.0)
        coord.tick()
        assert coord.scopes() == []

    def test_wake_per_tick_overrides_capacity(self, store: TaskStore, clock: Clock) -> None:
        coord = _coord(store, clock, capacity=lambda: 100, wake_per_tick=2, wake_spacing_secs=1.0)
        gens = {f"t{i}": _running(store, f"t{i}") for i in range(5)}
        for tid, gen in gens.items():
            coord.report(tid, _signal(retry_at=T0 + 1.0), generation=gen)
        clock.advance(1.0)
        assert coord.tick() == ["t0"]
        clock.advance(1.0)
        assert coord.tick() == ["t1", "t2"]
        clock.advance(1.0)
        assert coord.tick() == ["t3", "t4"]

    def test_recovered_signal_wakes_now_but_still_staged(
        self, store: TaskStore, clock: Clock
    ) -> None:
        coord = _coord(store, clock, capacity=lambda: 2)
        gens = {f"t{i}": _running(store, f"t{i}") for i in range(3)}
        for tid, gen in gens.items():
            coord.report(tid, _signal(retry_at=T0 + 3600.0), generation=gen)
        assert coord.recovered(GH)
        assert not coord.recovered("nope")
        assert coord.tick() == ["t0"]
        assert coord.next_deadline() == T0 + 5.0

    def test_reports_during_ramp_reset_the_scope(self, store: TaskStore, clock: Clock) -> None:
        coord = _coord(store, clock, capacity=lambda: 2, wake_spacing_secs=1.0)
        gens = {f"t{i}": _running(store, f"t{i}") for i in range(4)}
        for tid, gen in gens.items():
            coord.report(tid, _signal(retry_at=T0 + 1.0), generation=gen)
        clock.advance(1.0)
        coord.tick()
        clock.advance(1.0)
        assert coord.tick() == ["t1", "t2"]
        gen = store.get("t2").generation  # type: ignore[union-attr]
        # t2 was re-dispatched (claimed -> running) before its probe failed again.
        reclaim = store.claim("t2", owner="w")
        assert reclaim is not None
        store.transition("t2", model.STARTING, generation=reclaim.generation)
        store.transition("t2", model.RUNNING, generation=reclaim.generation)
        gen = reclaim.generation
        coord.report("t2", _signal(), generation=gen)
        sched = coord.schedule(GH)
        assert sched is not None
        assert sched.phase == PHASE_WAITING and sched.attempts == 2
        assert set(sched.waiters) == {"t2", "t3"}
        assert store.state_of("t2") == model.WAITING_DEPENDENCY


# ── backoff and bounds ────────────────────────────────────────────────────────


class TestBackoffAndBounds:
    def test_jitter_is_bounded_by_the_doubling_ceiling_and_the_cap(self, clock: Clock) -> None:
        """The schedule is the recovery ladder's: equal jitter in ``[ceiling/2, ceiling]``,
        never a near-zero draw that would hot-loop a scope still down."""
        coord = DependencyCoordinator(
            None, clock=clock, rng=random.Random(7), backoff=backoff(2.0, 100.0)
        )
        for attempts in range(1, 12):
            ceiling = min(100.0, 2.0 * 2 ** (attempts - 1))
            assert coord.backoff_ceiling(attempts) == ceiling
            for _ in range(50):
                delay = coord.backoff_delay(attempts)
                assert ceiling / 2 <= delay <= ceiling
        assert coord.backoff_ceiling(1_000) == 100.0

    def test_default_schedule_is_the_recovery_ladders(self, clock: Clock) -> None:
        from kiro_crew.recovery.policy import DEFAULT_BACKOFF_BASE_SECS, DEFAULT_BACKOFF_MAX_SECS

        coord = DependencyCoordinator(None, clock=clock)
        assert coord.backoff_ceiling(1) == DEFAULT_BACKOFF_BASE_SECS
        assert coord.backoff_max_secs == DEFAULT_BACKOFF_MAX_SECS

    def test_attempts_cap_fails_every_waiter_with_the_reason(
        self, store: TaskStore, clock: Clock
    ) -> None:
        coord = _coord(store, clock, max_attempts=2, wake_spacing_secs=0.0)
        gens = {f"t{i}": _running(store, f"t{i}") for i in range(3)}
        for tid, gen in gens.items():
            coord.report(tid, _signal(), generation=gen)
        for _ in range(2):  # two failed probes exhaust max_attempts=2
            sched = coord.schedule(GH)
            assert sched is not None
            clock.t = sched.retry_at
            probe = coord.tick()
            assert len(probe) == 1
            gen = store.get(probe[0]).generation  # type: ignore[union-attr]
            v = coord.report(probe[0], _signal(), generation=gen)
        assert v.outcome == "deadline" and "attempts" in v.reason
        for tid in gens:
            assert store.state_of(tid) == model.FAILED
            failed = _events(store, tid, EVENT_FAILED)
            assert failed and failed[-1]["reason"] == v.reason
        assert coord.scopes() == []

    def test_wall_clock_deadline_fails_waiters_on_tick(
        self, store: TaskStore, clock: Clock
    ) -> None:
        coord = _coord(store, clock, wait_deadline_secs=30.0)
        gens = {f"t{i}": _running(store, f"t{i}") for i in range(2)}
        for tid, gen in gens.items():
            coord.report(tid, _signal(retry_at=T0 + 60.0), generation=gen)
        # The WaitRecord carries the same deadline the coordinator enforces.
        record = WaitRecord.from_dict(store.get("t0").wait)  # type: ignore[union-attr]
        assert record is not None and record.deadline_at == T0 + 30.0
        clock.advance(60.0)
        assert coord.tick() == []
        for tid in gens:
            assert store.state_of(tid) == model.FAILED
        assert coord.scopes() == []

    def test_deadline_zero_means_attempts_cap_only(self, store: TaskStore, clock: Clock) -> None:
        coord = _coord(store, clock, wait_deadline_secs=0.0)
        gen = _running(store, "a")
        coord.report("a", _signal(retry_at=T0 + 10.0), generation=gen)
        record = WaitRecord.from_dict(store.get("a").wait)  # type: ignore[union-attr]
        assert record is not None and record.deadline_at is None
        clock.advance(1_000_000.0)
        assert coord.tick() == ["a"]


# ── terminal signals ──────────────────────────────────────────────────────────


class TestTerminalSignals:
    def test_auth_failed_goes_to_waiting_input_and_is_never_scheduled(
        self, store: TaskStore, clock: Clock
    ) -> None:
        coord = _coord(store, clock)
        gen = _running(store, "a")
        v = coord.report("a", _signal(KIND_AUTH_FAILED), generation=gen)
        assert v.outcome == "terminal" and v.state == model.WAITING_INPUT
        row = store.get("a")
        assert row is not None and row.state == model.WAITING_INPUT
        record = WaitRecord.from_dict(row.wait)
        assert record is not None and record.resume_condition.key == f"auth:{GH}"
        assert coord.scopes() == []
        assert _events(store, "a", EVENT_FAILED)
        clock.advance(10_000.0)
        assert coord.tick() == []

    def test_auth_failed_on_a_non_running_row_fails(self, store: TaskStore, clock: Clock) -> None:
        coord = _coord(store, clock)
        gen = _starting(store, "a")
        v = coord.report("a", _signal(KIND_AUTH_FAILED), generation=gen)
        assert v.state == model.FAILED and store.state_of("a") == model.FAILED

    def test_permanent_param_error_and_quota_fail_at_once(
        self, store: TaskStore, clock: Clock
    ) -> None:
        coord = _coord(store, clock)
        for tid, kind in (("p", KIND_PERMANENT_PARAM_ERROR), ("q", KIND_QUOTA_EXHAUSTED)):
            gen = _running(store, tid)
            v = coord.report(tid, _signal(kind), generation=gen)
            assert v.outcome == "terminal" and v.state == model.FAILED
            assert store.state_of(tid) == model.FAILED
        assert coord.scopes() == []

    def test_quota_with_reset_is_scheduled_at_the_reset(
        self, store: TaskStore, clock: Clock
    ) -> None:
        coord = _coord(store, clock)
        gen = _running(store, "q")
        v = coord.report("q", _signal(KIND_QUOTA_EXHAUSTED, retry_at=T0 + 86_400.0), generation=gen)
        assert v.outcome == "wait" and v.retry_at == T0 + 86_400.0


# ── parking a row that is not running ────────────────────────────────────────


class TestParkedRows:
    def test_starting_row_parks_in_retry_wait_until_the_deadline(
        self, store: TaskStore, clock: Clock
    ) -> None:
        coord = _coord(store, clock, wait_deadline_secs=600.0)
        gen = _starting(store, "a")
        v = coord.report("a", _signal(retry_at=T0 + 30.0), generation=gen)
        assert v.outcome == "wait"
        row = store.get("a")
        assert row is not None and row.state == model.RETRY_WAIT
        # Eligible only at the scope DEADLINE: the dispatcher cannot pre-empt the staged wake.
        assert row.next_run_at == T0 + 600.0
        assert store.fetch_dispatchable(model.KIND_SUBAGENT, limit=10) == []
        clock.advance(30.0)
        assert coord.tick() == ["a"]
        row = store.get("a")
        assert row is not None and row.state == model.QUEUED and row.next_run_at == T0 + 30.0
        assert [r.id for r in store.fetch_dispatchable(model.KIND_SUBAGENT, limit=10)] == ["a"]
        wake = _events(store, "a", EVENT_WAKE)
        assert wake and wake[-1]["to"] == model.QUEUED


# ── restart ───────────────────────────────────────────────────────────────────


class TestRestart:
    def test_rebuild_restores_scopes_waiters_and_backoff(
        self, store: TaskStore, clock: Clock
    ) -> None:
        first = _coord(store, clock, capacity=lambda: 2)
        gens = {f"t{i}": _running(store, f"t{i}") for i in range(3)}
        for tid, gen in gens.items():
            first.report(tid, _signal(retry_at=T0 + 120.0), generation=gen)
        g_br = _running(store, "b")
        first.report("b", _signal(scope=BEDROCK), generation=g_br)
        g_p = _starting(store, "parked")
        first.report("parked", _signal(retry_at=T0 + 120.0), generation=g_p)
        # A second failed probe on bedrock raises its attempts before the crash.
        clock.advance(2.0)
        assert first.tick() == ["b"]
        first.report("b", _signal(scope=BEDROCK), generation=store.get("b").generation)  # type: ignore[union-attr]
        assert first.schedule(BEDROCK).attempts == 2  # type: ignore[union-attr]

        # "Restart": a fresh coordinator over the same store.
        second = _coord(store, clock, capacity=lambda: 2)
        assert second.scopes() == []
        assert second.rebuild() == 5
        assert second.scopes() == [GH, BEDROCK]
        gh = second.schedule(GH)
        assert gh is not None
        assert set(gh.waiters) == {"t0", "t1", "t2", "parked"}
        assert gh.retry_at == T0 + 120.0 and gh.since == T0 and gh.attempts == 1
        br = second.schedule(BEDROCK)
        assert br is not None and br.attempts == 2 and set(br.waiters) == {"b"}
        # The rebuilt schedule resumes the backoff rather than retrying at once.
        assert second.tick() == []
        clock.t = T0 + 120.0
        # Both scopes are due: each releases its own single probe.
        assert second.tick() == ["t0", "b"]
        assert store.state_of("t0") == model.RETRY_WAIT  # claimable; no live run owns it
        # A rebuild after the wake does not re-add the woken rows.
        third = _coord(store, clock)
        assert third.rebuild() == 3
        assert "t0" not in third.waiters(GH) and third.waiters(BEDROCK) == []

    def test_rebuild_ignores_rows_that_were_woken_or_failed(
        self, store: TaskStore, clock: Clock
    ) -> None:
        coord = _coord(store, clock, max_attempts=1)
        gen = _running(store, "a")
        coord.report("a", _signal(), generation=gen)
        clock.advance(2.0)
        assert coord.tick() == ["a"]
        coord.report("a", _signal(), generation=store.get("a").generation)  # type: ignore[union-attr]
        assert store.state_of("a") == model.FAILED
        fresh = _coord(store, clock)
        assert fresh.rebuild() == 0


# ── misc ──────────────────────────────────────────────────────────────────────


class TestMisc:
    def test_store_less_coordinator_keeps_a_schedule(self, clock: Clock) -> None:
        coord = DependencyCoordinator(
            None, clock=clock, rng=FixedRng(), backoff=backoff(1.0, 900.0)
        )
        v = coord.report("x", _signal())
        assert v.outcome == "wait" and v.retry_at == T0 + 1.0
        clock.advance(1.0)
        assert coord.tick() == ["x"]

    def test_events_carry_the_signal_and_schedule(self, store: TaskStore, clock: Clock) -> None:
        coord = _coord(store, clock)
        gen = _running(store, "a")
        coord.report("a", _signal(retry_at=T0 + 9.0), generation=gen)
        wait = _events(store, "a", EVENT_WAIT)
        assert wait and wait[-1]["kind"] == KIND_RATE_LIMITED
        assert wait[-1]["dependency_scope"] == GH and wait[-1]["retry_at"] == T0 + 9.0
        assert wait[-1]["attempts"] == 1 and wait[-1]["state"] == model.WAITING_DEPENDENCY
        assert coord.public()[0]["waiters"] == 1

    def test_coordinator_from_config_reads_agent_keys(self, store: TaskStore, clock: Clock) -> None:
        class Agent:
            recovery_backoff_base_secs = 3.0
            recovery_backoff_max_secs = 30
            dependency_max_attempts = 4
            dependency_wait_deadline_secs = 0
            dependency_wake_per_tick = 7
            dependency_wake_spacing_secs = 0.5

        coord = coordinator_from_config(store, Agent(), clock=clock)
        assert coord.backoff_ceiling(1) == 3.0
        assert coord.backoff_ceiling(100) == 30.0
        assert coord._wake_batch() == 7

    def test_default_agent_config_has_the_keys(self) -> None:
        from kiro_crew.config.sections import AgentConfig

        cfg = AgentConfig()
        # The dependency schedule is the recovery ladder's: no keys of its own.
        assert not hasattr(cfg, "dependency_backoff_base_secs")
        assert not hasattr(cfg, "dependency_backoff_max_secs")
        assert cfg.dependency_max_attempts == 20
        assert cfg.dependency_wait_deadline_secs == 3600
        assert cfg.dependency_wake_per_tick == 0
        assert cfg.dependency_wake_spacing_secs == 1.0


# ── D2 (overload experiment): infrastructure scopes are deadline-bounded ─────


def test_infra_scope_survives_more_probes_than_max_attempts() -> None:
    """An ``mcp_gateway:*`` scope keeps its waiters past ``max_attempts`` probes:
    its budget is the wall-clock deadline (a 20-minute gatewayd outage must be
    survived), one probe per backoff step, whole-scope. A provider scope with
    the same probe count still fails at the cap."""
    from kiro_crew.taskq import dependency as dep

    clock = [1000.0]
    coordinator = dep.DependencyCoordinator(
        None,
        clock=lambda: clock[0],
        max_attempts=3,
        wait_deadline_secs=3600.0,
        backoff=backoff(2.0, 2.0),
    )

    def _probe_cycle(scope: str, task_id: str) -> str:
        # Every report carries a ~2 s "server" retry_at, the pre-fix run.py shape.
        verdict = coordinator.report(
            task_id,
            dep.DependencySignal(
                kind=dep.KIND_DEPENDENCY_UNAVAILABLE,
                dependency_scope=scope,
                source="mcp_gateway",
                retry_at=clock[0] + 2.0,
            ),
        )
        clock[0] += 2.5
        coordinator.tick()  # wakes the probe (in_flight); the next report fails it
        return verdict.outcome

    assert dep.is_infra_scope("mcp_gateway:capacity")
    assert not dep.is_infra_scope("provider:acp")
    outcomes = [_probe_cycle("mcp_gateway:capacity", "run-1") for _ in range(12)]
    assert set(outcomes) == {"wait"}
    sched = coordinator.schedule("mcp_gateway:capacity")
    assert sched is not None and sched.attempts > 3
    # The deadline still ends it: same scope, 3600 s later.
    clock[0] += 3600.0
    late = coordinator.report(
        "run-1",
        dep.DependencySignal(
            kind=dep.KIND_DEPENDENCY_UNAVAILABLE,
            dependency_scope="mcp_gateway:capacity",
            source="mcp_gateway",
        ),
    )
    assert late.outcome == "deadline"

    provider_outcomes = [_probe_cycle("provider:acp", "run-2") for _ in range(6)]
    assert provider_outcomes[:3] == ["wait", "wait", "wait"]
    assert "deadline" in provider_outcomes
