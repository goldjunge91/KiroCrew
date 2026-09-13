"""TaskRunner steps as taskq rows -- the runner adapter end to end.

Real ``TaskRunner`` + real ``execute_single_task`` + a real SQLite store in
``tmp_path``; only the ACP session provider is faked (the same shape
``test_subagent_stop_reason_consistency`` uses). Fake clock, fake sleeps,
seeded jitter: nothing here waits on wall-clock time.
"""

from __future__ import annotations

import asyncio
import random
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from overload_fakes import Clock, open_task_store
from test_taskrunner import _make_mock_sessions

from kiro_crew.acp.types import STOP_REASON_END_TURN, STOP_REASON_TOOL_STALL
from kiro_crew.providers.base import LLMEvent
from kiro_crew.recovery.ladder import RecoveryLadder
from kiro_crew.taskq import model as m
from kiro_crew.taskq.adapters import runner as r
from kiro_crew.taskq.dependency import KIND_RATE_LIMITED, SIGNAL_ATTR, DependencySignal
from kiro_crew.taskq.store import TaskStore
from kiro_crew.taskrunner import Step, StepStatus, TaskRun, TaskRunner

_STALL_EVIDENCE = "verdict=unknown; idle_secs=5400; tool=execute_bash; evidence=no result frame"


@pytest.fixture
def clock() -> Clock:
    return Clock(5_000.0)


@pytest.fixture
def store(tmp_path: Path, clock: Clock) -> TaskStore:
    yield from open_task_store(tmp_path, clock)


def _admission(store: TaskStore, clock: Clock, *, cap: int = 1) -> r.RunnerAdmission:
    async def _sleep(secs: float) -> None:
        clock.advance(secs)
        await asyncio.sleep(0)

    return r.RunnerAdmission(
        store,
        lane=r.RunnerLane(cap),
        clock=clock,
        sleep=_sleep,
        ladder=RecoveryLadder(clock=clock, rng=random.Random(7)),
    )


def _provider(script) -> MagicMock:
    """``script(call_no, message)`` yields the events of one turn (or raises)."""
    provider = MagicMock()
    calls: list[str] = []
    active = {"n": 0, "peak": 0}

    async def _stream(message: str):
        calls.append(message)
        active["n"] += 1
        active["peak"] = max(active["peak"], active["n"])
        try:
            await asyncio.sleep(0)
            for ev in script(len(calls), message):
                yield ev
        finally:
            active["n"] -= 1

    provider.stream = _stream
    provider.approve_tool = AsyncMock()
    provider.reject_tool = AsyncMock()
    provider.context_usage_pct = MagicMock(return_value=0.0)
    provider.calls = calls
    provider.active = active
    return provider


def _done(text: str = "whole") -> list[LLMEvent]:
    return [
        LLMEvent(kind="text_chunk", text=text),
        LLMEvent(kind="complete", stop_reason=STOP_REASON_END_TURN),
    ]


def _stalled(text: str = "half") -> list[LLMEvent]:
    return [
        LLMEvent(kind="text_chunk", text=text),
        LLMEvent(kind="complete", stop_reason=STOP_REASON_TOOL_STALL, text=_STALL_EVIDENCE),
    ]


@pytest.fixture(autouse=True)
def _no_self_review(monkeypatch):
    # The post-step self-review is its own turn; these tests count STEP turns.
    monkeypatch.setattr("kiro_crew.task_executor.self_review", AsyncMock(return_value=True))


def _runner(tmp_path: Path, provider: MagicMock, admission: r.RunnerAdmission) -> TaskRunner:
    sessions = _make_mock_sessions()
    sessions.get_or_create = AsyncMock(return_value=(provider, True, False))
    runner = TaskRunner(sessions=sessions, auto_test=False, work_dir=tmp_path)
    runner.attach_task_admission(admission)
    return runner


def _run(tmp_path: Path, *steps: Step, task_id: str = "r1") -> TaskRun:
    run = TaskRun(spec_path=str(tmp_path / "t.md"), spec_content="s", status="running")
    run.task_id = task_id
    run.work_dir = str(tmp_path)
    run.tasks = list(steps)
    return run


def _kinds(store: TaskStore, task_id: str) -> list[str]:
    return [e.kind for e in store.events(task_id)]


# ── 1. two steps through admission under cap 1 ──────────────────────────────


@pytest.mark.asyncio
async def test_two_steps_under_cap_one_serialize_and_are_claimed_once(
    tmp_path: Path, store: TaskStore, clock: Clock
) -> None:
    adm = _admission(store, clock, cap=1)
    provider = _provider(lambda n, msg: _done(f"out{n}"))
    runner = _runner(tmp_path, provider, adm)
    runner._run_session_keys["r1"] = "chat:alice"
    s1, s2 = Step(index=1, title="one", description="d1"), Step(
        index=2, title="two", description="d2"
    )
    run = _run(tmp_path, s1, s2)
    runner._taskq_begin_run(run)

    results = await asyncio.gather(
        runner._execute_single_task(run, s1), runner._execute_single_task(run, s2)
    )

    assert results == [True, True]
    assert provider.active["peak"] == 1  # never two turns at once under cap 1
    assert [s.status for s in (s1, s2)] == [StepStatus.PASSED, StepStatus.PASSED]
    run_row = store.get("taskrunner:r1")
    assert run_row is not None and run_row.state == m.RUNNING
    assert run_row.params[r.PARAM_LANE] == "chat:alice"
    for idx in (1, 2):
        row = store.get(f"taskrunner:r1:task{idx}")
        assert row.state == m.DONE and row.parent_id == "taskrunner:r1"
        assert row.root_id == "taskrunner:r1" and row.params[r.PARAM_LANE] == "chat:alice"
        assert _kinds(store, row.id).count("claimed") == 1
    assert adm.lane.running == 0
    run.status = "completed"
    runner._taskq_end_run(run)
    assert store.get("taskrunner:r1").state == m.DONE


@pytest.mark.asyncio
async def test_cron_launched_run_queues_in_the_system_lane(
    tmp_path: Path, store: TaskStore, clock: Clock
) -> None:
    adm = _admission(store, clock, cap=2)
    provider = _provider(lambda n, msg: _done())
    runner = _runner(tmp_path, provider, adm)
    s1 = Step(index=1, title="one", description="d1")
    run = _run(tmp_path, s1)
    run.source = "cron"
    runner._run_session_keys["r1"] = "chat:alice"  # a session exists, but cron wins
    runner._taskq_begin_run(run)
    assert await runner._execute_single_task(run, s1) is True
    assert store.get("taskrunner:r1").params[r.PARAM_LANE] == r.LANE_SYSTEM
    assert store.get("taskrunner:r1:task1").params[r.PARAM_LANE] == r.LANE_SYSTEM


# ── 2. restart mid-step -> resume from the checkpoint, not a re-run ─────────


@pytest.mark.asyncio
async def test_restart_mid_step_resumes_from_checkpoint(
    tmp_path: Path, store: TaskStore, clock: Clock, monkeypatch
) -> None:
    # A previous incarnation: run row + step rows, PASSED step 1, step 2 cut off.
    dead = "dead-incarnation"
    for row_id, parent in (("taskrunner:r1", None), ("taskrunner:r1:task2", "taskrunner:r1")):
        store.accept_one(
            m.TaskRecord(
                id=row_id,
                kind=m.KIND_TASKRUNNER_STEP,
                parent_id=parent,
                params={"task_id": "r1", r.PARAM_SAFE_RETRY: True},
            )
        )
        store.claim(row_id, owner=dead)
        store.transition(row_id, m.STARTING)
        store.transition(row_id, m.RUNNING)
    store.accept_one(
        m.TaskRecord(
            id="taskrunner:r1:task1", kind=m.KIND_TASKRUNNER_STEP, parent_id="taskrunner:r1"
        )
    )
    store.claim("taskrunner:r1:task1", owner=dead)
    store.finish("taskrunner:r1:task1", m.DONE)

    adm = _admission(store, clock, cap=1)
    provider = _provider(lambda n, msg: _done("resumed"))
    runner = _runner(tmp_path, provider, adm)
    s1 = Step(index=1, title="one", description="d1", status=StepStatus.PASSED, result="first")
    s2 = Step(index=2, title="two", description="d2")
    run = _run(tmp_path, s1, s2)
    run.status = "paused"  # what _load_runs assigns to a run cut off by a crash
    run.branch_name = "kirocrew/r1"  # git-coordinated: the checkpoint is real
    runner._runs["r1"] = run
    monkeypatch.setattr(runner, "_ensure_resumable_workspace", AsyncMock(return_value=True))
    monkeypatch.setattr("kiro_crew.taskrunner.git_coord.finalize", AsyncMock())
    monkeypatch.setattr(runner, "_apersist_runs", AsyncMock())

    report = await runner.adopt_task_rows()  # joins the sweep attach started

    assert report is not None and report.resumed == ["taskrunner:r1"]
    assert report.failed == ["taskrunner:r1:task2"]  # the cut-off step, re-run below
    await runner._tasks["r1"]
    assert run.status == "completed"
    assert s1.status == StepStatus.PASSED and s1.result == "first"  # the checkpoint held
    assert provider.calls and len(provider.calls) == 1  # only step 2 ran
    assert "two" in provider.calls[0]
    assert store.get("taskrunner:r1").state == m.DONE
    assert store.get("taskrunner:r1:task2").state == m.FAILED  # the interrupted attempt
    rerun = store.get("taskrunner:r1:task2~2")
    assert rerun is not None and rerun.state == m.DONE and rerun.parent_id == "taskrunner:r1"


@pytest.mark.asyncio
async def test_restart_without_a_checkpoint_is_not_re_run_blind(
    tmp_path: Path, store: TaskStore, clock: Clock
) -> None:
    store.accept_one(
        m.TaskRecord(
            id="taskrunner:r9",
            kind=m.KIND_TASKRUNNER_STEP,
            params={"task_id": "r9", r.PARAM_SAFE_RETRY: False},
        )
    )
    store.claim("taskrunner:r9", owner="dead")
    store.transition("taskrunner:r9", m.STARTING)
    store.transition("taskrunner:r9", m.RUNNING)
    adm = _admission(store, clock)
    notices: list[str] = []

    async def _notify(title, body, *args, **kw):
        notices.append(title)

    runner = _runner(tmp_path, _provider(lambda n, msg: _done()), adm)
    runner._on_notify = _notify
    run = _run(tmp_path, Step(index=1, title="x", description="d"), task_id="r9")
    run.status = "paused"
    runner._runs["r9"] = run

    report = await runner.adopt_task_rows()

    assert report.unknown_side_effect == ["taskrunner:r9"]
    assert run.status == "paused" and "r9" not in runner._tasks
    assert any("not auto-resumed" in n for n in notices)


# ── 3. stall -> recovering -> bounded failed with the partial kept ──────────


@pytest.mark.asyncio
async def test_persistent_stall_recovers_through_the_ladder_then_fails_with_partial(
    tmp_path: Path, store: TaskStore, clock: Clock, monkeypatch
) -> None:
    delays: list[float] = []

    async def _fake_delay(secs: float) -> None:
        delays.append(secs)
        clock.advance(secs)
        await asyncio.sleep(0)

    monkeypatch.setattr("kiro_crew.task_executor._recovery_delay", _fake_delay)
    adm = _admission(store, clock)
    provider = _provider(lambda n, msg: _stalled())
    runner = _runner(tmp_path, provider, adm)
    step = Step(index=1, title="Test step", description="desc")
    run = _run(tmp_path, step)

    success = await runner._execute_single_task(run, step)

    assert success is False and step.status == StepStatus.FAILED
    assert step.result == "half"  # the partial is kept
    assert "partial result preserved" in step.error and "stalled" in step.error
    # L3 allows one re-run, then the layer is exhausted: two turns, one delay.
    assert len(provider.calls) == 2 and len(delays) == 1 and delays[0] > 0
    assert "retry attempt" in provider.calls[1] and STOP_REASON_TOOL_STALL in provider.calls[1]
    row = store.get("taskrunner:r1:task1")
    assert row.state == m.FAILED
    transitions = [
        (e.data.get("from"), e.data.get("to"))
        for e in store.events(row.id)
        if e.kind == "transition"
    ]
    assert (m.RUNNING, m.RECOVERING) in transitions  # the wait between turns
    assert transitions[-1][1] == m.FAILED
    assert row.generation == 2  # the re-run was a fresh claim
    assert adm.lane.running == 0


@pytest.mark.asyncio
async def test_stall_then_success_passes_after_one_recovery(
    tmp_path: Path, store: TaskStore, clock: Clock, monkeypatch
) -> None:
    monkeypatch.setattr("kiro_crew.task_executor._recovery_delay", AsyncMock())
    adm = _admission(store, clock)
    provider = _provider(lambda n, msg: _stalled() if n == 1 else _done())
    runner = _runner(tmp_path, provider, adm)
    step = Step(index=1, title="Test step", description="desc")
    run = _run(tmp_path, step)
    assert await runner._execute_single_task(run, step) is True
    assert step.status == StepStatus.PASSED and step.result == "whole"
    assert store.get("taskrunner:r1:task1").state == m.DONE


# ── 4. 429 -> waiting_dependency -> wake -> PASSED ──────────────────────────


@pytest.mark.asyncio
async def test_rate_limit_parks_the_step_and_the_wake_resumes_it(
    tmp_path: Path, store: TaskStore, clock: Clock
) -> None:
    adm = _admission(store, clock)

    def _script(n: int, msg: str):
        if n == 1:
            exc = RuntimeError("HTTP 429 Too Many Requests")
            setattr(
                exc,
                SIGNAL_ATTR,
                DependencySignal(
                    kind=KIND_RATE_LIMITED,
                    dependency_scope="github:api",
                    source="test",
                    retry_at=clock.t + 30,
                ),
            )
            raise exc
        return _done()

    provider = _provider(_script)
    runner = _runner(tmp_path, provider, adm)
    step = Step(index=1, title="Test step", description="desc")
    run = _run(tmp_path, step)

    running = asyncio.create_task(runner._execute_single_task(run, step))
    for _ in range(20):
        await asyncio.sleep(0)
        if store.get("taskrunner:r1:task1").state == m.WAITING_DEPENDENCY:
            break
    row = store.get("taskrunner:r1:task1")
    assert row.state == m.WAITING_DEPENDENCY and row.wait["dependency_scope"] == "github:api"
    assert adm.lane.running == 0  # the slot is released for the wait
    assert step.attempts == 1 and not running.done()
    clock.advance(31)
    assert adm.tick() == ["taskrunner:r1:task1"]

    assert await running is True
    assert step.status == StepStatus.PASSED and len(provider.calls) == 2
    assert step.attempts == 1  # the wait did not burn an attempt
    assert store.get("taskrunner:r1:task1").state == m.DONE
    assert adm.lane.running == 0


# ── the run cap does not refuse once the lane meters the steps ──────────────


@pytest.mark.asyncio
async def test_run_cap_defers_to_the_lane_when_admission_is_attached(
    tmp_path: Path, store: TaskStore, clock: Clock
) -> None:
    adm = _admission(store, clock)
    runner = _runner(tmp_path, _provider(lambda n, msg: _done()), adm)
    never = asyncio.get_running_loop().create_future()
    for i in range(3):
        runner._tasks[f"busy{i}"] = asyncio.ensure_future(never)
    run = _run(tmp_path, Step(index=1, title="x", description="d"), task_id="r5")
    run.status = "planned"
    runner._runs["r5"] = run
    runner._apersist_runs = AsyncMock()  # type: ignore[method-assign]
    runner._workflow_rebind = AsyncMock()  # type: ignore[method-assign]
    try:
        # Legacy: refused. Attached: accepted (the steps queue in the lane).
        runner._task_admission = None
        with pytest.raises(ValueError, match="Too many concurrent tasks"):
            await runner.execute_plan("r5")
        runner._task_admission = adm
        assert await runner.execute_plan("r5") == "r5"
        runner._tasks["r5"].cancel()
        with pytest.raises(asyncio.CancelledError):
            await runner._tasks["r5"]
    finally:
        never.cancel()
        for i in range(3):
            runner._tasks.pop(f"busy{i}", None)
