"""Shared fakes for the overload-resilience tests (taskq, admission, fairness, adaptive).

A plain module, imported explicitly (``from overload_fakes import Clock``) like
``terminal_fakes``: no autouse magic, every test file names what it takes.

* :class:`Clock` -- a callable virtual clock (``clock()`` returns ``t``); the
  starting instant is a constructor argument so each suite keeps its own epoch.
* :class:`FixedRng` -- every draw answers the top of its range so jittered instants are exact.
* :func:`backoff` -- the coordinator's recovery-ladder schedule with a test-sized base and cap.
* :func:`task_record` -- a minimal ``TaskRecord`` for one id.
* :func:`open_task_store` -- the store a fixture yields, closed on teardown.
* :class:`ManagerHarness` -- a real ``SubagentManager`` whose runs finish when
  the test says so, with the mocks it needs (:func:`mock_sessions`, :func:`mock_ctx`).
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from kiro_crew.recovery.policy import LayerPolicy, RecoveryPolicy
from kiro_crew.subagent import SubagentInfo, SubagentManager
from kiro_crew.subagent_manager.admission import FairnessSettings
from kiro_crew.taskq import model
from kiro_crew.taskq.dependency import dependency_backoff
from kiro_crew.taskq.store import TaskStore
from kiro_crew.taskq.waits import WaitLedger

#: The tool a parent blocks in while it waits for its children.
BLOCKING_TOOL = "@kirocrew-core/spawn_sub_agents"


class Clock:
    """Callable virtual clock: ``clock()`` is ``t``; tests move it explicitly."""

    def __init__(self, t: float = 1000.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, secs: float) -> float:
        self.t += secs
        return self.t


class FixedRng(random.Random):
    """Every draw answers the top of its range so retry instants are exact."""

    def uniform(self, a: float, b: float) -> float:  # type: ignore[override]
        return b

    def random(self) -> float:
        return 1.0


def backoff(base_secs: float, max_secs: float) -> LayerPolicy:
    """The dependency coordinator's schedule with a test-sized base and cap."""
    return dependency_backoff(RecoveryPolicy(base_secs=base_secs, max_secs=max_secs))


def task_record(task_id: str, **kw: Any) -> model.TaskRecord:
    kw.setdefault("kind", model.KIND_SUBAGENT)
    kw.setdefault("params", {"task": task_id})
    return model.TaskRecord(id=task_id, **kw)


def open_task_store(
    tmp_path: Path, clock: Any, *, name: str = "tasks.db", window: int = 8, **kw: Any
) -> Iterator[TaskStore]:
    """Yield an opened store under *tmp_path* and close it after the test.

    A fixture body: ``yield from open_task_store(tmp_path, clock)``. The
    defaults are the ones most suites use; a suite with its own window or file
    name passes them through.
    """
    s = TaskStore(tmp_path / name, window=window, clock=clock, network_fs=False, **kw).open()
    try:
        yield s
    finally:
        s.close()


def mock_sessions() -> MagicMock:
    sessions = MagicMock()
    sessions.get_pid = MagicMock(return_value=None)
    sessions.get_approval_policy = MagicMock(return_value="auto")
    sessions.get_agent = MagicMock(return_value="")
    sessions.has_session = MagicMock(return_value=True)
    sessions.release = MagicMock()
    sessions.reset = AsyncMock()
    return sessions


def mock_ctx() -> MagicMock:
    ctx = MagicMock()
    ctx.hooks.auto_approve_subagent_spawn = True
    return ctx


class ManagerHarness:
    """A real manager whose runs finish when the test says so.

    ``end(info, outcome)`` resolves the run: ``"ok"`` (default) completes it,
    ``"fail"`` sets ``error``, ``"cancel"`` sets ``user_stopped``. The manager's
    store is opened by its constructor; a loop caller awaits
    ``mgr.wait_taskq_ready()`` before spawning.
    """

    def __init__(self, max_concurrent: int, settings: FairnessSettings | None = None) -> None:
        self.mgr = SubagentManager(
            sessions=mock_sessions(), ctx_builder=mock_ctx(), max_concurrent=max_concurrent
        )
        self.mgr._spawn_stagger_secs = 0.0
        self.mgr._last_spawn_ts = 0.0
        if settings is not None:
            self.mgr._admission.set_fairness_settings(settings)
        self.finish: dict[str, asyncio.Future] = {}
        self.started: list[str] = []
        harness = self

        async def _run(_self, info: SubagentInfo) -> None:
            harness.started.append(info.id)
            _self._admission.taskq_mark(info, "running")
            fut = harness.finish.setdefault(info.id, asyncio.get_event_loop().create_future())
            outcome = await fut
            info.done = True
            if outcome == "fail":
                info.error = "boom"
            elif outcome == "cancel":
                info.user_stopped = True
            info.result = "ok"
            _self._claim_finalize(info)
            if _self._release_slot(info):
                _self._running_count -= 1
                _self._drain_queue()

        self._patch = patch.object(SubagentManager, "_run", new=_run)
        self._patch.start()

    def close(self) -> None:
        self._patch.stop()

    @property
    def store(self) -> TaskStore:
        return self.mgr._taskq

    @property
    def ledger(self) -> WaitLedger:
        return WaitLedger(self.store)

    def spawn(self, task: str, parent: str = "dash:1", **kw: Any) -> SubagentInfo:
        info = self.mgr.spawn(task, parent_session_key=parent, **kw)
        assert info is not None
        return info

    def block_in_spawn_sub_agents(self, info: SubagentInfo, call_id: str = "call") -> None:
        self.mgr._agents[info.id]._inflight_tool = SimpleNamespace(
            tool_name=BLOCKING_TOOL, title=call_id
        )

    def child_of(self, parent: SubagentInfo, task: str, **kw: Any) -> SubagentInfo:
        return self.spawn(task, parent=f"subagent:{parent.id}", **kw)

    async def end(self, info: SubagentInfo, outcome: str = "ok") -> None:
        fut = self.finish.setdefault(info.id, asyncio.get_event_loop().create_future())
        if not fut.done():
            fut.set_result(outcome)
        await self.settle()

    async def settle(self, rounds: int = 25) -> None:
        for _ in range(rounds):
            await asyncio.sleep(0)

    def state(self, info: SubagentInfo) -> str | None:
        return self.store.state_of(info.id)

    def live(self, info: SubagentInfo) -> SubagentInfo:
        return self.mgr._agents[info.id]
