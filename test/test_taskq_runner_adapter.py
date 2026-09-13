"""``taskq.adapters.runner``: TaskRunner steps and workflow agent calls as rows.

Fake clock, fake sleep, a real SQLite store in ``tmp_path``. No kiro-cli, no
real waits: every ``await`` here resolves on the loop's own tick.
"""

from __future__ import annotations

import asyncio
import random
from pathlib import Path
from types import SimpleNamespace

import pytest
from overload_fakes import Clock, open_task_store

from kiro_crew.recovery.ladder import RecoveryLadder
from kiro_crew.taskq import model as m
from kiro_crew.taskq.adapters import runner as r
from kiro_crew.taskq.dependency import (
    KIND_AUTH_FAILED,
    KIND_RATE_LIMITED,
    DependencyCoordinator,
    DependencySignal,
)
from kiro_crew.taskq.store import TaskStore, TaskStoreUnavailable


class _Sleeps:
    """Records requested sleeps; advances the fake clock instead of waiting."""

    def __init__(self, clock: Clock) -> None:
        self.clock = clock
        self.calls: list[float] = []

    async def __call__(self, secs: float) -> None:
        self.calls.append(secs)
        self.clock.advance(secs)
        await asyncio.sleep(0)


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def store(tmp_path: Path, clock: Clock) -> TaskStore:
    yield from open_task_store(tmp_path, clock)


def _admission(store: TaskStore | None, clock: Clock, **kw) -> tuple[r.RunnerAdmission, _Sleeps]:
    sleeps = _Sleeps(clock)
    lane = kw.pop("lane", None) or r.RunnerLane(kw.pop("cap", 1), mode=kw.pop("mode", r.MODE_AIMD))
    adm = r.RunnerAdmission(store, lane=lane, clock=clock, sleep=sleeps, **kw)
    return adm, sleeps


def _events(store: TaskStore, task_id: str) -> list[tuple[str, dict]]:
    return [(e.kind, e.data) for e in store.events(task_id)]


# ── lanes ────────────────────────────────────────────────────────────────────


def test_lane_for_system_and_session() -> None:
    assert r.lane_for("sess-1", "cron") == r.LANE_SYSTEM
    assert r.lane_for("sess-1", "hook") == r.LANE_SYSTEM
    assert r.lane_for("", "chat") == r.LANE_SYSTEM
    assert r.lane_for("sess-1", "chat") == "sess-1"
    assert r.lane_for("sess-1") == "sess-1"


@pytest.mark.asyncio
async def test_runner_lane_is_fifo_and_bounded_by_live_cap() -> None:
    lane = r.RunnerLane(1)
    order: list[str] = []

    async def worker(name: str) -> None:
        await lane.acquire(name)
        order.append(f"{name}:in")
        await asyncio.sleep(0)
        order.append(f"{name}:out")
        lane.release()

    await asyncio.gather(worker("a"), worker("b"), worker("c"))
    assert order == ["a:in", "a:out", "b:in", "b:out", "c:in", "c:out"]
    assert lane.running == 0 and lane.waiting == 0


@pytest.mark.asyncio
async def test_runner_lane_effective_cap_actuator_pauses_and_resumes() -> None:
    lane = r.RunnerLane(4)
    assert lane.effective == 4
    assert lane.set_effective_cap(0) == 0  # pause: nothing new is granted

    grants: list[str] = []

    async def waiter(name: str) -> None:
        await lane.acquire(name)
        grants.append(name)

    tasks = [asyncio.create_task(waiter("x")), asyncio.create_task(waiter("y"))]
    await asyncio.sleep(0)
    assert grants == [] and lane.waiting == 2
    assert lane.set_effective_cap(1) == 1
    await asyncio.sleep(0)
    assert grants == ["x"]  # exactly one slot, FIFO
    lane.set_effective_cap(None)
    await asyncio.sleep(0)
    assert grants == ["x", "y"]
    await asyncio.gather(*tasks)
    assert lane.effective == 4 and lane.stats()["granted"] == 2


def test_runner_lane_fixed_mode_pins_the_bound() -> None:
    ceiling = {"v": 6}
    lane = r.RunnerLane(lambda: ceiling["v"], mode=lambda: r.MODE_FIXED, pinned=3)
    assert lane.effective == 3
    lane.set_effective_cap(1)  # the controller's actuator is ignored under fixed
    assert lane.effective == 3
    ceiling["v"] = 2
    assert lane.effective == 3
    aimd = r.RunnerLane(lambda: ceiling["v"], mode=r.MODE_AIMD)
    assert aimd.effective == 2
    aimd.set_effective_cap(1)
    assert aimd.effective == 1


@pytest.mark.asyncio
async def test_runner_lane_cancelled_waiter_leaves_the_queue() -> None:
    lane = r.RunnerLane(1)
    await lane.acquire("holder")
    t = asyncio.create_task(lane.acquire("w"))
    await asyncio.sleep(0)
    assert lane.waiting == 1
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t
    assert lane.waiting == 0
    # A parked waiter never held a slot: the holder's slot is still counted.
    assert lane.running == 1
    lane.release()
    assert lane.running == 0


@pytest.mark.asyncio
async def test_runner_lane_cancelled_parked_waiter_keeps_the_bound() -> None:
    """Cancelling a parked waiter leaves ``running`` alone; the next pump
    grants exactly the bound, so the lane cannot over-admit afterwards."""
    lane = r.RunnerLane(2)
    await lane.acquire("a")
    await lane.acquire("b")
    parked = [asyncio.create_task(lane.acquire(f"w{i}")) for i in range(3)]
    await asyncio.sleep(0)
    assert lane.waiting == 3 and lane.running == 2
    parked[0].cancel()
    with pytest.raises(asyncio.CancelledError):
        await parked[0]
    assert lane.running == 2 and lane.waiting == 2
    lane.release()  # one slot back: exactly one of the two remaining waiters starts
    await asyncio.sleep(0)
    assert lane.running == 2 and lane.waiting == 1
    lane.release()
    await asyncio.sleep(0)
    assert lane.running == 2 and lane.waiting == 0
    await asyncio.gather(*parked[1:])
    lane.release()
    lane.release()
    assert lane.running == 0


@pytest.mark.asyncio
async def test_runner_lane_waiter_granted_then_cancelled_hands_the_slot_on() -> None:
    lane = r.RunnerLane(1)
    await lane.acquire("holder")
    first = asyncio.create_task(lane.acquire("first"))
    second = asyncio.create_task(lane.acquire("second"))
    await asyncio.sleep(0)
    assert lane.waiting == 2
    lane.release()  # grants ``first`` without letting it run yet
    assert lane.running == 1 and lane.waiting == 1
    first.cancel()  # granted and cancelled in the same tick
    with pytest.raises(asyncio.CancelledError):
        await first
    await second  # the slot went to the next waiter
    assert lane.running == 1 and lane.waiting == 0
    lane.release()
    assert lane.running == 0


# ── accept (write-before-ack) ────────────────────────────────────────────────


def test_accept_writes_the_row_before_returning(store: TaskStore, clock: Clock) -> None:
    adm, _ = _admission(store, clock)
    rec = adm.accept(
        kind=m.KIND_TASKRUNNER_STEP,
        task_id="taskrunner:run1:task1",
        session_key="sess",
        source="chat",
        params={"index": 1},
    )
    assert rec is not None
    row = store.get(rec.id)
    assert row is not None and row.state == m.QUEUED
    assert row.params[r.PARAM_LANE] == "sess"
    assert [k for k, _ in _events(store, rec.id)] == ["accepted"]


def test_accept_failure_is_a_refusal_not_an_id(store: TaskStore, clock: Clock, monkeypatch) -> None:
    adm, _ = _admission(store, clock)

    def _boom(_rec):
        raise TaskStoreUnavailable("disk full")

    monkeypatch.setattr(store, "accept_one", _boom)
    with pytest.raises(r.RunnerAdmissionRefused):
        adm.accept(kind=m.KIND_TASKRUNNER_STEP, task_id="taskrunner:r:task1")
    assert store.get("taskrunner:r:task1") is None


def test_accept_reuses_a_claimable_row_and_suffixes_a_terminal_one(
    store: TaskStore, clock: Clock
) -> None:
    adm, _ = _admission(store, clock)
    first = adm.accept(kind=m.KIND_TASKRUNNER_STEP, task_id="taskrunner:r:task1")
    assert first is not None
    # Still queued: the same id is handed back, not duplicated.
    again = adm.accept(kind=m.KIND_TASKRUNNER_STEP, task_id="taskrunner:r:task1")
    assert again is not None and again.id == first.id
    assert store.count(kind=m.KIND_TASKRUNNER_STEP) == 1
    store.claim(first.id)
    store.finish(first.id, m.FAILED, error="x")
    rerun = adm.accept(kind=m.KIND_TASKRUNNER_STEP, task_id="taskrunner:r:task1")
    assert rerun is not None and rerun.id == "taskrunner:r:task1~2"
    assert store.get("taskrunner:r:task1").state == m.FAILED


def test_accept_without_a_store_returns_none(clock: Clock) -> None:
    adm, _ = _admission(None, clock)
    assert adm.accept(kind=m.KIND_TASKRUNNER_STEP, task_id="x") is None


# ── admit ────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_two_steps_under_cap_one_run_in_order_and_are_claimed_once(
    store: TaskStore, clock: Clock
) -> None:
    adm, _ = _admission(store, clock, cap=1)
    ids = []
    for i in (1, 2):
        rec = adm.accept(kind=m.KIND_TASKRUNNER_STEP, task_id=f"taskrunner:r:task{i}")
        ids.append(rec.id)
    order: list[str] = []

    async def step(task_id: str) -> None:
        h = await adm.admit(task_id, lane="sess")
        order.append(f"{task_id}:start")
        assert h.running()
        await asyncio.sleep(0)
        order.append(f"{task_id}:end")
        assert h.done()

    await asyncio.gather(*(step(t) for t in ids))
    assert order == [f"{ids[0]}:start", f"{ids[0]}:end", f"{ids[1]}:start", f"{ids[1]}:end"]
    for task_id in ids:
        kinds = [k for k, _ in _events(store, task_id)]
        assert kinds.count("claimed") == 1, kinds
        rec = store.get(task_id)
        assert rec.state == m.DONE and rec.generation == 1 and rec.attempts == 1
    assert adm.lane.running == 0


@pytest.mark.asyncio
async def test_admit_defers_on_memory_pressure_instead_of_refusing(
    store: TaskStore, clock: Clock
) -> None:
    verdicts = iter(
        [
            SimpleNamespace(admitted=False, reason="memory critical"),
            SimpleNamespace(admitted=True, reason=""),
        ]
    )
    adm, sleeps = _admission(store, clock, pressure=lambda: next(verdicts), admit_wait_secs=5.0)
    rec = adm.accept(kind=m.KIND_TASKRUNNER_STEP, task_id="taskrunner:r:task1")
    h = await adm.admit(rec.id)
    assert h.generation == 1 and h.state == m.STARTING
    kinds = [k for k, _ in _events(store, rec.id)]
    assert kinds[:3] == ["accepted", "deferred", "claimed"]
    assert sleeps.calls == [5.0]
    assert adm.deferred_count == 1


@pytest.mark.asyncio
async def test_admit_raises_when_the_row_was_cancelled_while_waiting(
    store: TaskStore, clock: Clock
) -> None:
    adm, _ = _admission(store, clock, cap=1)
    a = adm.accept(kind=m.KIND_TASKRUNNER_STEP, task_id="taskrunner:r:task1")
    b = adm.accept(kind=m.KIND_TASKRUNNER_STEP, task_id="taskrunner:r:task2")
    ha = await adm.admit(a.id)
    waiter = asyncio.create_task(adm.admit(b.id))
    await asyncio.sleep(0)
    assert store.cancel(b.id, reason="user") == m.QUEUED
    ha.done()  # frees the slot; the waiter now claims -- and is refused
    with pytest.raises(r.RunnerTaskCancelled):
        await waiter
    assert adm.lane.running == 0


@pytest.mark.asyncio
async def test_admit_without_a_store_is_the_lane_alone(clock: Clock) -> None:
    adm, _ = _admission(None, clock, cap=1)
    h = await adm.admit("anything")
    assert h.generation == 0 and h.running() and adm.lane.running == 1
    assert h.done() and adm.lane.running == 0


# ── recovery (stop reason) ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_recovering_then_reclaim_is_a_new_generation_and_fences_the_old(
    store: TaskStore, clock: Clock
) -> None:
    ladder = RecoveryLadder(clock=clock, rng=random.Random(1))
    adm, sleeps = _admission(store, clock, ladder=ladder)
    rec = adm.accept(kind=m.KIND_TASKRUNNER_STEP, task_id="taskrunner:r:task1")
    h = await adm.admit(rec.id)
    h.running()
    old_gen = h.generation
    decision = adm.decide_recovery(h, unit="sess:task1", reason="stalled")
    assert decision is not None and decision.retry and decision.delay_secs > 0
    assert h.recovering(reason="stalled", delay_secs=decision.delay_secs)
    row = store.get(rec.id)
    assert row.state == m.RECOVERING and row.next_run_at == pytest.approx(
        clock.t + decision.delay_secs
    )
    assert adm.lane.running == 0  # the slot was released for the wait
    # Not eligible before next_run_at: admit waits it out on the fake clock.
    await h.reclaim()
    assert h.generation == old_gen + 1 and adm.lane.running == 1
    assert sleeps.calls and sum(sleeps.calls) == pytest.approx(decision.delay_secs)
    # The interrupted turn's late write is fenced out.
    assert store.transition(rec.id, m.DONE, generation=old_gen) is False
    assert (
        "stale_result",
        {"from_generation": old_gen, "current": h.generation, "wanted": m.DONE},
    ) in _events(store, rec.id)
    assert h.running() and h.done()


def test_l3_ladder_bounds_the_stall_recoveries(clock: Clock) -> None:
    ladder = RecoveryLadder(clock=clock, rng=random.Random(2))
    adm, _ = _admission(None, clock, ladder=ladder)
    h = r.Admitted(adm, "t", m.KIND_TASKRUNNER_STEP, "sess", 0, _slot_held=False)
    verdicts = [adm.decide_recovery(h, unit="u", reason="stall").retry for _ in range(3)]
    assert verdicts == [True, False, False]  # L3: 2 attempts, then escalate
    adm.recovered("u")
    assert adm.decide_recovery(h, unit="u", reason="stall").retry is True


# ── waits: dependency ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_rate_limit_parks_in_waiting_dependency_and_tick_wakes_it(
    store: TaskStore, clock: Clock
) -> None:
    adm, _ = _admission(store, clock, cap=1)
    rec = adm.accept(kind=m.KIND_TASKRUNNER_STEP, task_id="taskrunner:r:task1")
    h = await adm.admit(rec.id)
    h.running()
    gen = h.generation
    signal = DependencySignal(
        kind=KIND_RATE_LIMITED, dependency_scope="github:api", source="test", retry_at=clock.t + 30
    )
    waiting = asyncio.create_task(adm.yield_dependency(h, signal))
    await asyncio.sleep(0)
    row = store.get(rec.id)
    assert row.state == m.WAITING_DEPENDENCY
    assert row.wait["dependency_scope"] == "github:api"
    assert row.wait["resume_condition"]["at"] == pytest.approx(clock.t + 30)
    assert adm.lane.running == 0  # slot released; the runtime stays resident
    assert adm.tick() == []  # not due yet
    clock.advance(31)
    assert adm.tick() == [rec.id]
    assert await waiting is True
    # +1 at the wake (claimable ``retry_wait``), +1 at the re-admission's
    # claim: ``running`` was written only once the lane was granted again.
    assert h.generation == gen + 2 and h.state == m.RUNNING
    assert store.get(rec.id).state == m.RUNNING
    assert adm.lane.running == 1  # re-admitted through capacity
    assert h.done()


@pytest.mark.asyncio
async def test_dependency_wait_with_coordinator_shares_one_schedule(
    store: TaskStore, clock: Clock
) -> None:
    lane = r.RunnerLane(2)
    coord = DependencyCoordinator(
        store, clock=clock, rng=random.Random(3), capacity=lambda: lane.effective
    )
    adm, _ = _admission(store, clock, lane=lane, coordinator=coord)
    coord._on_wake = adm.on_wake
    handles = []
    for i in (1, 2):
        rec = adm.accept(kind=m.KIND_WORKFLOW_AGENT, task_id=f"workflow:w:agent{i}")
        h = await adm.admit(rec.id, kind=m.KIND_WORKFLOW_AGENT)
        h.running()
        handles.append(h)
    sig = DependencySignal(kind=KIND_RATE_LIMITED, dependency_scope="gh", source="t", retry_at=None)
    waits = [asyncio.create_task(adm.yield_dependency(h, sig)) for h in handles]
    await asyncio.sleep(0)
    assert coord.scopes() == ["gh"] and sorted(coord.waiters("gh")) == [h.task_id for h in handles]
    assert lane.running == 0
    clock.advance(coord.schedule("gh").retry_at - clock.t + 0.01)
    woken = adm.tick()  # probe: exactly one waiter
    assert len(woken) == 1
    clock.advance(2.0)
    woken += adm.tick()  # ramp: the rest
    assert sorted(woken) == [h.task_id for h in handles]
    assert await asyncio.gather(*waits) == [True, True]
    assert lane.running == 2
    for h in handles:
        assert h.done()


@pytest.mark.asyncio
async def test_terminal_dependency_signal_fails_the_row(store: TaskStore, clock: Clock) -> None:
    coord = DependencyCoordinator(store, clock=clock, rng=random.Random(4))
    adm, _ = _admission(store, clock, coordinator=coord)
    rec = adm.accept(kind=m.KIND_TASKRUNNER_STEP, task_id="taskrunner:r:task1")
    h = await adm.admit(rec.id)
    h.running()
    sig = DependencySignal(kind=KIND_AUTH_FAILED, dependency_scope="gh", source="t")
    assert await adm.yield_dependency(h, sig) is False
    # auth_failed becomes a sign-in wait (waiting_input), not a silent retry.
    assert store.get(rec.id).state == m.WAITING_INPUT
    assert adm.lane.running == 1  # the caller still owns its slot until it settles
    h.fail("auth")


@pytest.mark.asyncio
async def test_dependency_wait_cancelled_returns_false(store: TaskStore, clock: Clock) -> None:
    adm, _ = _admission(store, clock)
    rec = adm.accept(kind=m.KIND_TASKRUNNER_STEP, task_id="taskrunner:r:task1")
    h = await adm.admit(rec.id)
    h.running()
    sig = DependencySignal(kind=KIND_RATE_LIMITED, dependency_scope="gh", source="t", retry_at=None)
    waiting = asyncio.create_task(adm.yield_dependency(h, sig))
    await asyncio.sleep(0)
    assert store.get(rec.id).state == m.WAITING_DEPENDENCY
    assert adm.cancel_wait(rec.id, reason="user stop")
    assert await waiting is False
    assert store.get(rec.id).state == m.CANCELLED


# ── waits: input ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_waiting_input_resumes_with_the_answer(store: TaskStore, clock: Clock) -> None:
    adm, _ = _admission(store, clock, cap=1)
    rec = adm.accept(kind=m.KIND_TASKRUNNER_STEP, task_id="taskrunner:r:task1")
    h = await adm.admit(rec.id)
    h.running()
    gen = h.generation
    waiting = asyncio.create_task(adm.waiting_input(h, tool_call_id="q-1", reason="passphrase?"))
    await asyncio.sleep(0)
    row = store.get(rec.id)
    assert row.state == m.WAITING_INPUT and row.wait["tool_call_id"] == "q-1"
    assert adm.lane.running == 0
    assert adm.answer_input("nope", "x") is False  # unknown row: nothing woken
    assert adm.answer_input(rec.id, "yes") is True
    assert await waiting == "yes"
    assert h.generation == gen + 2 and adm.lane.running == 1  # wake +1, re-claim +1
    assert h.done()


@pytest.mark.asyncio
async def test_waiting_input_cancelled_returns_none(store: TaskStore, clock: Clock) -> None:
    adm, _ = _admission(store, clock)
    rec = adm.accept(kind=m.KIND_TASKRUNNER_STEP, task_id="taskrunner:r:task1")
    h = await adm.admit(rec.id)
    h.running()
    waiting = asyncio.create_task(adm.waiting_input(h, tool_call_id="q-2"))
    await asyncio.sleep(0)
    adm.cancel_wait(rec.id)
    assert await waiting is None
    assert store.get(rec.id).state == m.CANCELLED


# ── adopt (the recovery adapter) ─────────────────────────────────────────────


def _crashed_row(store: TaskStore, task_id: str, *, safe: bool, parent: str | None = None) -> None:
    rec = m.TaskRecord(
        id=task_id,
        kind=m.KIND_TASKRUNNER_STEP,
        parent_id=parent,
        params={"task_id": "run1", r.PARAM_SAFE_RETRY: safe},
    )
    store.accept_one(rec)
    store.claim(task_id, owner="dead-incarnation")
    store.transition(task_id, m.STARTING)
    store.transition(task_id, m.RUNNING)


def test_adopt_resumes_safe_rows_and_parks_unsafe_ones(store: TaskStore, clock: Clock) -> None:
    _crashed_row(store, "taskrunner:run1", safe=True)
    _crashed_row(store, "taskrunner:run1:task2", safe=True, parent="taskrunner:run1")
    _crashed_row(store, "taskrunner:run2", safe=False)
    _crashed_row(store, "taskrunner:run2:task1", safe=False, parent="taskrunner:run2")
    resumed: list[str] = []

    report = r.adopt_orphaned_rows(store, resume=lambda rec: resumed.append(rec.id) or True)

    assert report.examined == 4
    assert resumed == ["taskrunner:run1"] and report.resumed == ["taskrunner:run1"]
    assert store.get("taskrunner:run1").state == m.RECOVERING  # claimable for the resume
    assert store.get("taskrunner:run1:task2").state == m.FAILED  # re-run from the checkpoint
    assert store.get("taskrunner:run2").state == m.UNKNOWN_SIDE_EFFECT
    assert store.get("taskrunner:run2:task1").state == m.UNKNOWN_SIDE_EFFECT
    assert sorted(report.unknown_side_effect) == ["taskrunner:run2", "taskrunner:run2:task1"]
    # Idempotent: nothing left to adopt.
    again = r.adopt_orphaned_rows(store, resume=lambda rec: True)
    assert again.examined == 1 and again.resumed == ["taskrunner:run1"]


def test_adopt_skips_rows_this_incarnation_still_owns(store: TaskStore, clock: Clock) -> None:
    rec = m.TaskRecord(
        id="taskrunner:live", kind=m.KIND_TASKRUNNER_STEP, params={r.PARAM_SAFE_RETRY: True}
    )
    store.accept_one(rec)
    store.claim(rec.id)  # this incarnation, fresh lease
    store.transition(rec.id, m.STARTING)
    store.transition(rec.id, m.RUNNING)
    report = r.adopt_orphaned_rows(store, resume=lambda _r: True)
    assert report.skipped == ["taskrunner:live"] and store.get(rec.id).state == m.RUNNING


def test_adopt_declined_resume_fails_the_row(store: TaskStore, clock: Clock) -> None:
    _crashed_row(store, "taskrunner:gone", safe=True)
    report = r.adopt_orphaned_rows(store, resume=lambda _r: False)
    assert report.failed == ["taskrunner:gone"]
    assert store.get("taskrunner:gone").state == m.FAILED


def test_legacy_import_row_is_adopted(store: TaskStore, clock: Clock) -> None:
    from kiro_crew.taskq.migrate import legacy_taskrunner_records

    runs = Path(store.path).parent / "runs.json"
    runs.write_text('[{"task_id": "old_1", "status": "paused", "name": "n", "spec_path": "s.md"}]')
    for rec in legacy_taskrunner_records(runs, now=clock.t):
        store.insert_if_absent(rec)
    row = store.get("taskrunner:old_1")
    assert row.state == m.RECOVERING and row.side_effect_class == m.SIDE_EFFECT_UNKNOWN
    # An import carries no safe_retry: it is NOT re-run blind.
    report = r.adopt_orphaned_rows(store, resume=lambda _r: True)
    assert report.unknown_side_effect == ["taskrunner:old_1"]


# ── factory ──────────────────────────────────────────────────────────────────


def test_runner_admission_for_follows_the_manager_cap(store: TaskStore) -> None:
    manager = SimpleNamespace(_taskq=store, max_concurrent=5)
    cfg = SimpleNamespace(
        agent=SimpleNamespace(adaptive_concurrency_mode="aimd", admit_wait_secs=7)
    )
    adm = r.runner_admission_for(manager, cfg=cfg, pressure=lambda: SimpleNamespace(admitted=True))
    assert adm.store is store and adm.lane.effective == 5
    manager.max_concurrent = 2  # the controller's set_effective_cap landed
    assert adm.lane.effective == 2
    cfg.agent.adaptive_concurrency_mode = "fixed"
    manager.max_concurrent = 1
    assert adm.lane.mode == r.MODE_FIXED and adm.lane.effective == 1
    assert adm._admit_wait == 7.0


# ── the durable state machine is the only path ───────────────────────────────


@pytest.mark.asyncio
async def test_required_store_missing_refuses_instead_of_lane_only(clock: Clock) -> None:
    """With ``agent.task_queue_enabled`` on, no store means REFUSE (typed):
    lane-only admission would hand out generation-0 handles a restart forgets."""
    adm, _ = _admission(
        None, clock, cap=1, require_store=True, store_error=lambda: "tasks.db: disk I/O error"
    )
    with pytest.raises(r.RunnerAdmissionRefused, match="disk I/O error"):
        adm.accept(kind=m.KIND_TASKRUNNER_STEP, task_id="taskrunner:r:task1")
    with pytest.raises(r.RunnerAdmissionRefused):
        await adm.admit("taskrunner:r:task1")
    assert adm.lane.running == 0
    # The legacy queue-off shape is unchanged: lane-only, generation 0.
    legacy, _ = _admission(None, clock, cap=1)
    h = await legacy.admit("legacy")
    assert h.generation == 0 and h.done()


@pytest.mark.asyncio
async def test_failed_claim_never_starts_generation_zero(
    store: TaskStore, clock: Clock, monkeypatch
) -> None:
    adm, _ = _admission(store, clock, cap=1)
    rec = adm.accept(kind=m.KIND_TASKRUNNER_STEP, task_id="taskrunner:r:task1")

    def _boom(_task_id):
        raise TaskStoreUnavailable("locked")

    monkeypatch.setattr(store, "claim", _boom)
    with pytest.raises(r.RunnerAdmissionRefused):
        await adm.admit(rec.id)
    assert adm.lane.running == 0
    assert store.state_of(rec.id) == m.QUEUED  # still dispatchable, never started


@pytest.mark.asyncio
async def test_settle_marks_settled_only_after_the_terminal_write_or_a_durable_retry(
    store: TaskStore, clock: Clock, monkeypatch
) -> None:
    adm, _ = _admission(store, clock, cap=1)
    rec = adm.accept(kind=m.KIND_TASKRUNNER_STEP, task_id="taskrunner:r:task1")
    h = await adm.admit(rec.id)
    h.running()
    real_finish = store.finish
    outage = {"on": True}

    def _flaky(*a, **kw):
        if outage["on"]:
            raise TaskStoreUnavailable("store down")
        return real_finish(*a, **kw)

    monkeypatch.setattr(store, "finish", _flaky)
    assert h.done() is False
    # The row is still live in the store, the write is owned by the retry,
    # the slot is released so the lane is not leaked -- and nothing forgot
    # the task while its terminal write is outstanding.
    assert store.state_of(rec.id) == m.RUNNING
    assert adm.stats()["pending_terminal_writes"] == 1
    assert adm.lane.running == 0
    assert h.done() is False  # idempotent: no second write attempt is queued
    outage["on"] = False
    assert adm.retry_terminal_writes() == 1
    assert store.state_of(rec.id) == m.DONE
    assert adm.stats()["pending_terminal_writes"] == 0
    # tick() replays too, and a fenced row (another owner ended it) is dropped.
    rec2 = adm.accept(kind=m.KIND_TASKRUNNER_STEP, task_id="taskrunner:r:task2")
    adm.defer_terminal_write(rec2.id, 99, m.DONE, result_ref=None, error=None)
    adm.tick()
    assert adm.stats()["pending_terminal_writes"] == 0


# ── a deferred terminal write is durable by reconcile ───────────────────────


@pytest.mark.asyncio
async def test_crash_during_a_deferred_terminal_write_reconciles_the_row(
    tmp_path: Path, clock: Clock, monkeypatch
) -> None:
    """The store refuses the terminal write; the process crashes before the
    retry lands. Nothing is lost: the row is still ``running`` under the dead
    incarnation's lease (the retry never releases it), so the next boot's
    reconcile settles it -- ``unknown_side_effect`` for the default class, a
    claimable ``recovering`` row for side-effect-free (retry-safe) work. The pending write is
    therefore durable BY RECONCILE, not by a second persisted record."""
    from kiro_crew.taskq.reconcile import reconcile_on_boot

    path = tmp_path / "tasks.db"
    store_a = TaskStore(path, window=8, clock=clock, network_fs=False).open()
    adm, _ = _admission(store_a, clock, cap=2)
    unknown = adm.accept(kind=m.KIND_TASKRUNNER_STEP, task_id="taskrunner:r:unknown")
    safe = adm.accept(
        kind=m.KIND_TASKRUNNER_STEP,
        task_id="taskrunner:r:safe",
        side_effect_class=m.SIDE_EFFECT_NONE,
    )
    h1 = await adm.admit(unknown.id)
    h2 = await adm.admit(safe.id)
    h1.running()
    h2.running()

    def _down(*a, **kw):
        raise TaskStoreUnavailable("store down")

    monkeypatch.setattr(store_a, "finish", _down)
    assert h1.done() is False and h2.done() is False
    assert adm.stats()["pending_terminal_writes"] == 2
    # The lease is still the dead owner's: a running row is not claimable, so
    # nothing in the live process could start a second copy meanwhile.
    for rec in (store_a.get(unknown.id), store_a.get(safe.id)):
        assert rec is not None and rec.state == m.RUNNING
        assert rec.lease_owner == store_a.incarnation
    # -- crash: the retry never runs; a new incarnation boots on the same file.
    store_a.close()
    store_b = TaskStore(path, window=8, clock=clock, network_fs=False).open()
    report = reconcile_on_boot(store_b, now=clock())
    assert report.examined == 2 and report.awaiting_adapter == 2
    # The boot reconciler drops the dead owner's lease and hands runner kinds
    # to the runner adapter, which settles them by class.
    resumed: list[str] = []
    adopted = r.adopt_orphaned_rows(store_b, resume=lambda rec: resumed.append(rec.id) or True)
    assert unknown.id in adopted.unknown_side_effect
    assert store_b.state_of(unknown.id) == m.UNKNOWN_SIDE_EFFECT
    assert resumed == [safe.id]
    assert store_b.state_of(safe.id) == m.RECOVERING  # claimable for the runner's own admit
    adm_b, _ = _admission(store_b, clock, cap=2)
    clock.advance(3600)
    again = await adm_b.admit(safe.id)
    assert again.generation > h2.generation
    store_b.close()


# ── an accepted answer survives a crash before the grant ─────────────────────


@pytest.mark.asyncio
async def test_answer_is_restored_from_the_wake_event_after_a_rebuild(
    tmp_path: Path, clock: Clock
) -> None:
    """answer -> crash before the lane grant -> rebuild: the resumed step reads
    the answer from the persisted wake event, not from RAM."""
    path = tmp_path / "tasks.db"
    store_a = TaskStore(path, window=8, clock=clock, network_fs=False).open()
    adm, _ = _admission(store_a, clock, cap=1)
    rec = adm.accept(kind=m.KIND_TASKRUNNER_STEP, task_id="taskrunner:r:ask")
    h = await adm.admit(rec.id)
    h.running()
    wait = asyncio.create_task(adm.waiting_input(h, tool_call_id="q1", reason="password?"))
    await asyncio.sleep(0)
    assert store_a.state_of(rec.id) == m.WAITING_INPUT
    assert adm.answer_input(rec.id, "hunter2") is True
    # -- crash before the coroutine is re-admitted: RAM is gone.
    wait.cancel()
    with pytest.raises(asyncio.CancelledError):
        await wait
    assert store_a.state_of(rec.id) == m.RETRY_WAIT  # claimable, no lease
    store_a.close()
    store_b = TaskStore(path, window=8, clock=clock, network_fs=False).open()
    from kiro_crew.taskq.reconcile import reconcile_on_boot

    reconcile_on_boot(store_b, now=clock())
    assert store_b.state_of(rec.id) == m.RETRY_WAIT  # boot leaves a claimable row alone
    adm_b, _ = _admission(store_b, clock, cap=1)
    assert adm_b.recorded_answer(rec.id) == "hunter2"
    h_b = await adm_b.admit(rec.id)
    h_b.running()
    # The resumed step takes the answer once; a later rebuild does not replay it.
    assert adm_b.recorded_answer(rec.id) == "hunter2"
    assert adm_b.consume_answer(rec.id, "q1") is True
    assert adm_b.recorded_answer(rec.id) is None
    store_b.close()


@pytest.mark.asyncio
async def test_waiting_input_falls_back_to_the_persisted_answer(
    store: TaskStore, clock: Clock
) -> None:
    """A wake that arrives without this process's RAM copy of the answer (the
    ledger woke the row on another instance's behalf) still returns it."""
    adm, _ = _admission(store, clock, cap=1)
    rec = adm.accept(kind=m.KIND_TASKRUNNER_STEP, task_id="taskrunner:r:ask2")
    h = await adm.admit(rec.id)
    h.running()
    wait = asyncio.create_task(adm.waiting_input(h, tool_call_id="q2"))
    await asyncio.sleep(0)
    from kiro_crew.taskq.waits import WaitLedger

    # Another instance records the answer on the wake event and wakes the row.
    assert WaitLedger(store, clock=clock).wake(
        rec.id, reason="input answered", detail={"answer": "yes"}
    )
    adm.on_wake(rec.id)
    assert await asyncio.wait_for(wait, 2) == "yes"


@pytest.mark.asyncio
async def test_terminal_decision_is_persisted_before_capacity_is_released(
    tmp_path: Path, clock: Clock, monkeypatch
) -> None:
    """``Admitted.settle`` writes the terminal state FIRST and releases the lane
    slot only afterwards; a crash between the committed write and the local
    ``_settled`` bookkeeping leaves a TERMINAL row, which neither the boot
    reconciler nor the runner adapter touches -- a cancelled retry-safe row is
    never re-run. Only an unreachable store (``TaskStoreUnavailable``) leaves
    the decision to reconcile."""
    from kiro_crew.taskq.reconcile import reconcile_on_boot

    path = tmp_path / "tasks.db"
    store_a = TaskStore(path, window=8, clock=clock, network_fs=False).open()
    adm, _ = _admission(store_a, clock, cap=1)
    rec = adm.accept(
        kind=m.KIND_TASKRUNNER_STEP,
        task_id="taskrunner:r:safe-cancel",
        side_effect_class=m.SIDE_EFFECT_NONE,
    )
    h = await adm.admit(rec.id)
    h.running()
    order: list[str] = []
    real_finish = store_a.finish

    def _finish(*a, **kw):
        order.append("finish")
        return real_finish(*a, **kw)

    def _release():
        order.append("release_slot")
        # -- crash here: the write is committed, the local bookkeeping is not.
        raise SystemExit("simulated crash after the terminal write")

    monkeypatch.setattr(store_a, "finish", _finish)
    monkeypatch.setattr(h, "release_slot", _release)
    with pytest.raises(SystemExit):
        h.cancel("operator stop")
    assert order == ["finish", "release_slot"], "the terminal write precedes the slot release"
    assert store_a.state_of(rec.id) == m.CANCELLED
    store_a.close()
    store_b = TaskStore(path, window=8, clock=clock, network_fs=False).open()
    report = reconcile_on_boot(store_b, now=clock())
    assert report.examined == 0  # a terminal row is not an active row
    resumed: list[str] = []
    adopted = r.adopt_orphaned_rows(store_b, resume=lambda rec: resumed.append(rec.id) or True)
    assert adopted.examined == 0 and resumed == []
    assert store_b.state_of(rec.id) == m.CANCELLED
    store_b.close()
