"""SubagentManager admission on top of the durable task queue.

Real ``SubagentManager`` admission and drain; the run itself is a fake worker
that finishes at once. No kiro-cli, no sockets, tmp ``KIROCREW_HOME``.
"""

from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import kiro_crew.resource_status as resource_status
import kiro_crew.subagent as subagent_mod
from kiro_crew.subagent import SubagentInfo, SubagentManager
from kiro_crew.subagent_manager.admission import TASK_STORE_UNAVAILABLE_CODE
from kiro_crew.taskq import model
from kiro_crew.taskq.store import TaskStore, TaskStoreUnavailable

pytestmark = pytest.mark.usefixtures("healthy_host_memory")


def _sessions() -> MagicMock:
    sessions = MagicMock()
    sessions.get_pid = MagicMock(return_value=None)
    sessions.get_approval_policy = MagicMock(return_value="auto")
    sessions.get_agent = MagicMock(return_value="")
    sessions.has_session = MagicMock(return_value=True)
    sessions.release = MagicMock()
    sessions.reset = AsyncMock()
    return sessions


def _ctx() -> MagicMock:
    ctx = MagicMock()
    ctx.hooks.auto_approve_subagent_spawn = True
    return ctx


async def _manager(max_concurrent: int = 2, **kw) -> SubagentManager:
    mgr = SubagentManager(
        sessions=_sessions(), ctx_builder=_ctx(), max_concurrent=max_concurrent, **kw
    )
    await mgr.wait_taskq_ready()
    mgr._spawn_stagger_secs = 0.0
    mgr._last_spawn_ts = 0.0
    return mgr


def _store_path() -> Path:
    return Path(os.environ["KIROCREW_HOME"]) / "tasks" / "tasks.db"


async def _fake_run(mgr: SubagentManager, info: SubagentInfo, *, fail: bool = False) -> None:
    """The smallest honest worker: finish, report once, free the slot, pump."""
    await asyncio.sleep(0)
    info.done = True
    if fail:
        info.error = "boom"
    info.result = "ok"
    mgr._claim_finalize(info)
    if mgr._release_slot(info):
        mgr._running_count -= 1
        mgr._drain_queue()


@pytest.fixture
def quiet():
    with patch("kiro_crew.subagent.Stats"), patch("kiro_crew.subagent.sel"):
        yield


# ── write-before-ack ──────────────────────────────────────────────────────────


def test_manager_opens_store_under_home(quiet) -> None:
    mgr = SubagentManager(sessions=_sessions(), ctx_builder=_ctx())
    assert mgr._taskq is not None
    assert mgr._taskq.path == _store_path()
    assert _store_path().exists()


@pytest.mark.asyncio
async def test_spawn_persists_before_returning_id(quiet) -> None:
    mgr = await _manager(max_concurrent=1)
    with patch.object(SubagentManager, "_run", new=AsyncMock()):
        started = mgr.spawn("first", parent_session_key="dash:1")
        queued = mgr.spawn("second", parent_session_key="dash:1")
    assert started is not None and not started.queued
    assert queued is not None and queued.queued
    store: TaskStore = mgr._taskq
    assert store.state_of(started.id) == model.STARTING
    assert store.state_of(queued.id) == model.QUEUED
    row = store.get(queued.id)
    assert row.params["task"] == "second" and row.params["_preassigned_id"] == queued.id
    assert row.session_key == "dash:1" and row.kind == model.KIND_SUBAGENT
    assert [e.kind for e in store.events(started.id)][:3] == ["accepted", "claimed", "transition"]


@pytest.mark.asyncio
async def test_store_write_failure_refuses_with_typed_code_and_no_row(quiet) -> None:
    mgr = await _manager()

    def boom(*a, **k):
        raise TaskStoreUnavailable("disk full")

    with (
        patch.object(mgr._taskq, "accept_one", side_effect=boom),
        patch.object(SubagentManager, "_run", new=AsyncMock()),
    ):
        info = mgr.spawn("x", parent_session_key="dash:1")
    assert info is not None and info.done and info.error
    assert info.error_code == TASK_STORE_UNAVAILABLE_CODE
    assert "task store unavailable" in info.error
    assert info.id not in mgr._agents
    assert mgr._taskq.count() == 0
    assert mgr._running_count == 0


@pytest.mark.asyncio
async def test_policy_refusals_leave_no_row(quiet) -> None:
    mgr = await _manager()
    with patch.object(SubagentManager, "_run", new=AsyncMock()):
        empty = mgr.spawn("   ", parent_session_key="dash:1")
        bad_cwd = mgr.spawn("t", parent_session_key="dash:1", cwd="/definitely/not/allowed")
    assert empty.done and bad_cwd.done and bad_cwd.error
    assert mgr._taskq.count() == 0


# ── memory pressure defers ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_memory_pressure_defers_instead_of_refusing(
    quiet, monkeypatch: pytest.MonkeyPatch
) -> None:
    mgr = await _manager()
    monkeypatch.setattr(
        subagent_mod,
        "cached_admission_check",
        lambda: resource_status.AdmissionDecision(
            admitted=False,
            posture=resource_status.POSTURE_CRITICAL,
            available_gb=0.5,
            reason="host memory critically low",
        ),
    )
    with patch.object(SubagentManager, "_run", new=AsyncMock()):
        info = mgr.spawn("later", parent_session_key="dash:1")
    assert info is not None
    assert info.queued is True and info.done is False and not info.error
    store: TaskStore = mgr._taskq
    row = store.get(info.id)
    assert row.state == model.QUEUED
    assert row.next_run_at is not None and row.next_run_at > store.now()
    assert [e.kind for e in store.events(info.id)] == ["accepted", "deferred"]
    assert info.id not in mgr._agents and mgr._running_count == 0
    # not in the window either: it is not eligible yet
    assert mgr._queue == []
    assert mgr.queued_count_for("dash:1") == 1
    # pressure lifts and the clock passes: the pump starts it
    monkeypatch.undo()
    store._clock = lambda: time.time() + 3600
    with patch.object(SubagentManager, "_run", new=AsyncMock()):
        mgr._drain_queue()
    assert store.state_of(info.id) == model.STARTING
    assert info.id in mgr._agents


@pytest.mark.asyncio
async def test_low_memory_floor_defers_too(quiet, monkeypatch: pytest.MonkeyPatch) -> None:
    mgr = await _manager()
    monkeypatch.setattr(
        subagent_mod, "check_memory_available", lambda min_gb=None, path=None: (False, 0.2)
    )
    with patch.object(SubagentManager, "_run", new=AsyncMock()):
        info = mgr.spawn("later", parent_session_key="dash:1")
    assert info.queued and not info.done
    assert mgr._taskq.state_of(info.id) == model.QUEUED


@pytest.mark.asyncio
async def test_without_store_pressure_still_refuses(quiet, monkeypatch: pytest.MonkeyPatch) -> None:
    mgr = await _manager()
    mgr._taskq = None
    monkeypatch.setattr(
        subagent_mod, "check_memory_available", lambda min_gb=None, path=None: (False, 0.2)
    )
    with patch.object(SubagentManager, "_run", new=AsyncMock()):
        info = mgr.spawn("now", parent_session_key="dash:1")
    assert info.done and "refused" in info.error


# ── bounded window / drain from store ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_window_bounded_and_fifo_across_boundary(quiet) -> None:
    mgr = await _manager(max_concurrent=1)
    mgr._taskq._window = 3
    ids = []
    with patch.object(SubagentManager, "_run", new=AsyncMock()):
        for i in range(8):
            ids.append(mgr.spawn(f"t{i}", parent_session_key="dash:1").id)
    # one started, 7 queued: 3 in the window, 4 store-only
    assert len(mgr._queue) == 3
    assert [p["_preassigned_id"] for p in mgr._queue] == ids[1:4]
    assert mgr._admission.taskq_overflow() == 4
    assert mgr.queued_count_for("dash:1") == 7
    assert mgr._taskq.count(state=model.QUEUED) == 7
    # a completion frees the slot: the drain takes the OLDEST row and refills
    order: list[str] = []
    with patch.object(SubagentManager, "_run", new=AsyncMock()):
        mgr._running_count = 0
        mgr._drain_queue()
    started = [i for i in ids[1:] if mgr._taskq.state_of(i) == model.STARTING]
    assert started == [ids[1]]
    assert len(mgr._queue) == 3
    assert [p["_preassigned_id"] for p in mgr._queue] == ids[2:5]
    del order


@pytest.mark.asyncio
async def test_2000_submissions_all_complete_window_never_exceeds_64(quiet) -> None:
    mgr = await _manager(max_concurrent=8)
    store: TaskStore = mgr._taskq
    assert store.window == 64
    peak_window = {"n": 0}

    async def run(self, info):
        peak_window["n"] = max(peak_window["n"], len(mgr._queue))
        await _fake_run(mgr, info)

    with patch.object(SubagentManager, "_run", new=run):
        ids = [mgr.spawn(f"task {i}", parent_session_key="dash:1").id for i in range(2000)]
        assert len(set(ids)) == 2000
        assert store.count() == 2000
        assert len(mgr._queue) <= 64
        deadline = time.monotonic() + 60
        while store.count(state=model.DONE) < 2000 and time.monotonic() < deadline:
            await asyncio.sleep(0.01)
            mgr._drain_queue()
    by_state = store.count_by_state()
    assert by_state == {model.DONE: 2000}, by_state
    assert peak_window["n"] <= 64
    assert len(mgr._queue) == 0
    assert mgr._running_count == 0
    assert mgr.queued_count_for("dash:1") == 0
    # every id is done exactly once: one terminal transition event each
    sample = store.events(ids[1234])
    assert [e.kind for e in sample].count("transition") == 2  # starting, done (fake run)
    assert sample[-1].data["to"] == model.DONE


# ── terminal writes ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_completion_and_failure_settle_store_rows(quiet) -> None:
    mgr = await _manager(max_concurrent=4)
    outcomes = {"a": False, "b": True}

    async def run(self, info):
        await _fake_run(mgr, info, fail=outcomes[info.task])

    with patch.object(SubagentManager, "_run", new=run):
        a = mgr.spawn("a", parent_session_key="dash:1")
        b = mgr.spawn("b", parent_session_key="dash:1")
        await asyncio.gather(mgr._tasks[a.id], mgr._tasks[b.id])
    assert mgr._taskq.state_of(a.id) == model.DONE
    assert mgr._taskq.state_of(b.id) == model.FAILED
    ev = mgr._taskq.events(b.id)[-1]
    assert ev.data["error"] == "boom"
    assert mgr._taskq.get(a.id).result_ref.endswith("result.txt")


@pytest.mark.asyncio
async def test_user_stop_settles_as_cancelled(quiet) -> None:
    mgr = await _manager()
    with patch.object(SubagentManager, "_run", new=AsyncMock()):
        a = mgr.spawn("a", parent_session_key="dash:1")
    info = mgr._agents[a.id]
    info.user_stopped = True
    info.done = True
    assert mgr._claim_finalize(info) is True
    assert mgr._taskq.state_of(a.id) == model.CANCELLED
    # a second reporter cannot flip it
    info.user_stopped = False
    info.error = "late"
    assert mgr._claim_finalize(info) is False
    assert mgr._taskq.state_of(a.id) == model.CANCELLED


@pytest.mark.asyncio
async def test_stale_generation_from_superseded_dispatch_is_ignored(quiet) -> None:
    mgr = await _manager()
    with patch.object(SubagentManager, "_run", new=AsyncMock()):
        a = mgr.spawn("a", parent_session_key="dash:1")
    store: TaskStore = mgr._taskq
    old_info = mgr._agents[a.id]
    old_gen = old_info._taskq_generation
    # the runtime is lost and the row is re-dispatched under a new generation
    assert store.transition(a.id, model.RECOVERING, generation=old_gen)
    mgr._agents.pop(a.id)
    mgr._running_count = 0
    with patch.object(SubagentManager, "_run", new=AsyncMock()):
        mgr._drain_queue()
    new_info = mgr._agents[a.id]
    assert new_info._taskq_generation == old_gen + 1
    assert store.state_of(a.id) == model.STARTING
    # the OLD worker reports failure: fenced
    old_info.done = True
    old_info.error = "late failure"
    mgr._admission.taskq_settle(old_info)
    assert store.state_of(a.id) == model.STARTING
    assert any(e.kind == "stale_result" for e in store.events(a.id))


# ── cancel vs drain ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_cancel_store_only_queued_row_never_starts(quiet) -> None:
    mgr = await _manager(max_concurrent=1)
    mgr._taskq._window = 1
    with patch.object(SubagentManager, "_run", new=AsyncMock()):
        first = mgr.spawn("run", parent_session_key="dash:1")
        in_window = mgr.spawn("w", parent_session_key="dash:1")
        outside = mgr.spawn("o", parent_session_key="dash:1")
    assert [p["_preassigned_id"] for p in mgr._queue] == [in_window.id]
    assert mgr._taskq.state_of(outside.id) == model.QUEUED
    reported: list[SubagentInfo] = []
    mgr._report_queued_stop = lambda params: reported.append(params)  # type: ignore[method-assign]
    assert await mgr.cancel(outside.id) is True
    assert mgr._taskq.state_of(outside.id) == model.CANCELLED
    assert reported and reported[0]["_preassigned_id"] == outside.id
    # drain everything: the cancelled row is never started
    with patch.object(SubagentManager, "_run", new=AsyncMock()):
        mgr._running_count = 0
        mgr._drain_queue()
        mgr._running_count = 0
        mgr._drain_queue()
    assert mgr._taskq.state_of(in_window.id) == model.STARTING
    assert mgr._taskq.state_of(outside.id) == model.CANCELLED
    assert outside.id not in mgr._agents
    del first


@pytest.mark.asyncio
async def test_cancel_in_window_marks_store_and_unqueues(quiet) -> None:
    mgr = await _manager(max_concurrent=1)
    with patch.object(SubagentManager, "_run", new=AsyncMock()):
        mgr.spawn("run", parent_session_key="dash:1")
        waiting = mgr.spawn("w", parent_session_key="dash:1")
    mgr._report_queued_stop = MagicMock()  # type: ignore[method-assign]
    assert await mgr.cancel(waiting.id) is True
    assert mgr._queue == []
    assert mgr._taskq.state_of(waiting.id) == model.CANCELLED


@pytest.mark.asyncio
async def test_cancel_landing_between_claim_and_start_stops_the_spawn(quiet) -> None:
    """A row cancelled in the store after the window popped it: the drain's
    fenced claim fails and nothing registers."""
    mgr = await _manager(max_concurrent=1)
    with patch.object(SubagentManager, "_run", new=AsyncMock()):
        mgr.spawn("run", parent_session_key="dash:1")
        waiting = mgr.spawn("w", parent_session_key="dash:1")
    mgr._taskq.cancel(waiting.id, reason="user_stop")  # e.g. an external cancel
    with patch.object(SubagentManager, "_run", new=AsyncMock()):
        mgr._running_count = 0
        mgr._drain_queue()
    assert waiting.id not in mgr._agents
    assert mgr._running_count == 0
    assert mgr._taskq.state_of(waiting.id) == model.CANCELLED


@pytest.mark.asyncio
async def test_cancel_for_parent_reaches_store_only_rows(quiet) -> None:
    mgr = await _manager(max_concurrent=1)
    mgr._taskq._window = 1
    with patch.object(SubagentManager, "_run", new=AsyncMock()):
        mgr.spawn("run", parent_session_key="dash:1")
        w = mgr.spawn("w", parent_session_key="dash:1")
        o1 = mgr.spawn("o1", parent_session_key="dash:1")
        o2 = mgr.spawn("o2", parent_session_key="dash:2")  # other parent
    mgr._report_queued_stop = MagicMock()  # type: ignore[method-assign]
    mgr._force_reap = AsyncMock()  # type: ignore[method-assign]
    running, queued = await mgr.cancel_for_parent("dash:1")
    assert queued == 2
    assert mgr._taskq.state_of(w.id) == model.CANCELLED
    assert mgr._taskq.state_of(o1.id) == model.CANCELLED
    assert mgr._taskq.state_of(o2.id) == model.QUEUED
    del running


# ── restart survival ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_queued_rows_survive_restart_and_redispatch(quiet) -> None:
    first = await _manager(max_concurrent=1)
    with patch.object(SubagentManager, "_run", new=AsyncMock()):
        running = first.spawn("running", parent_session_key="dash:1")
        q1 = first.spawn("q1", parent_session_key="dash:1")
        q2 = first.spawn("q2", parent_session_key="dash:1")
    first._taskq.close()  # crash: no terminal writes, in-memory queue gone
    del first
    second = await _manager(max_concurrent=2)
    store: TaskStore = second._taskq
    # reconcile settled the lost run (subagent default class: unknown)
    assert store.state_of(running.id) == model.UNKNOWN_SIDE_EFFECT
    assert store.state_of(q1.id) == model.QUEUED and store.state_of(q2.id) == model.QUEUED
    assert second._queue == []
    with patch.object(SubagentManager, "_run", new=AsyncMock()):
        second._drain_queue()
        second._drain_queue()
    assert store.state_of(q1.id) == model.STARTING
    assert store.state_of(q2.id) == model.STARTING
    assert set(second._agents) == {q1.id, q2.id}
    assert second._agents[q1.id].task == "q1"


@pytest.mark.asyncio
async def test_batch_pending_sees_store_only_members(quiet) -> None:
    mgr = await _manager(max_concurrent=1)
    mgr._taskq._window = 1
    with patch.object(SubagentManager, "_run", new=AsyncMock()):
        a = mgr.spawn("a", batch_id="wv", batch_total=3, parent_session_key="dash:1")
        mgr.spawn("b", batch_id="wv", batch_total=3, parent_session_key="dash:1")
        c = mgr.spawn("c", batch_id="wv", batch_total=3, parent_session_key="dash:1")
    assert mgr.batch_members_pending("wv") is True
    mgr._agents[a.id].done = True
    mgr._queue.clear()  # window member gone; store-only member c still holds the wave
    assert mgr._taskq.state_of(c.id) == model.QUEUED
    assert mgr.batch_members_pending("wv") is True
    mgr._taskq.cancel(c.id)
    (
        mgr._taskq.cancel([r.id for r in mgr._taskq.list_pending(model.KIND_SUBAGENT)][0])
        if mgr._taskq.list_pending(model.KIND_SUBAGENT)
        else None
    )
    assert mgr.batch_members_pending("wv") is False


@pytest.mark.asyncio
async def test_task_queue_disabled_keeps_legacy_queue(
    quiet, monkeypatch: pytest.MonkeyPatch
) -> None:
    from kiro_crew.config.loader import KiroCrewConfig

    real_load = KiroCrewConfig.load

    def load_disabled(*a, **k):
        cfg = real_load(*a, **k)
        cfg.agent.task_queue_enabled = False
        return cfg

    monkeypatch.setattr(KiroCrewConfig, "load", staticmethod(load_disabled))
    mgr = await _manager(max_concurrent=1)
    assert mgr._taskq is None
    with patch.object(SubagentManager, "_run", new=AsyncMock()):
        mgr.spawn("a", parent_session_key="dash:1")
        q = mgr.spawn("b", parent_session_key="dash:1")
    assert q.queued and len(mgr._queue) == 1
    assert not (Path(os.environ["KIROCREW_HOME"]) / "tasks").exists()
