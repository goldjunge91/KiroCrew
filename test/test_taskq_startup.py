"""Task-store startup stays off-loop; requests fail closed until attachment."""

import asyncio
import threading
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from kiro_crew.subagent import SubagentManager
from kiro_crew.subagent_manager.admission import SpawnAdmissionCoordinator
from kiro_crew.taskq.store import TaskStore, TaskStoreUnavailable


@pytest.mark.asyncio
async def test_manager_open_is_off_loop_and_pending_spawn_is_refused(monkeypatch):
    entered = asyncio.Event()
    release = threading.Event()
    loop = asyncio.get_running_loop()
    original = TaskStore.open
    observed = []

    def parked_open(store):
        observed.append(TaskStore._on_running_loop_thread())
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(5)
        return original(store)

    monkeypatch.setattr(SpawnAdmissionCoordinator, "pump_off_loop", True)
    monkeypatch.setattr(SpawnAdmissionCoordinator, "open_store_off_loop", True)
    monkeypatch.setattr(TaskStore, "strict_loop_guard", True)
    monkeypatch.setattr(TaskStore, "open", parked_open)
    manager = SubagentManager(sessions=MagicMock(), ctx_builder=MagicMock())
    try:
        await asyncio.wait_for(entered.wait(), 3)
        assert manager._taskq is None
        for spawn in (manager.spawn, manager.spawn_async):
            result = spawn("before attachment", parent_session_key="web-1")
            info = await result if asyncio.iscoroutine(result) else result
            assert info is not None and info.done
            assert info.error_code == "task_store_unavailable"
        assert manager._running_count == 0
        assert not manager._agents
        release.set()
        await manager.wait_taskq_ready()
        assert manager._taskq is not None
        assert manager._taskq_unavailable is None
        assert manager._taskq.loop_thread_calls == 0
        assert observed == [False]
        record = manager.prepare_spawn("after attachment", parent_session_key="web-1")
        assert record is not None and hasattr(record, "record")
    finally:
        release.set()
        await manager.cancel_all()
        if manager._taskq is not None:
            await asyncio.to_thread(manager._taskq.close)


@pytest.mark.asyncio
async def test_failed_open_keeps_typed_refusal_after_startup(monkeypatch):
    def fail_open(store):
        raise TaskStoreUnavailable("database is locked")

    monkeypatch.setattr(TaskStore, "open", fail_open)
    monkeypatch.setattr(SpawnAdmissionCoordinator, "open_store_off_loop", True)
    manager = SubagentManager(sessions=MagicMock(), ctx_builder=MagicMock())
    await manager.wait_taskq_ready()
    info = await manager.spawn_async("not accepted")
    assert manager._taskq is None
    assert info.error_code == "task_store_unavailable"
    assert "locked" in info.error
    await manager.cancel_all()


def test_manager_without_a_loop_opens_synchronously():
    manager = SubagentManager(sessions=MagicMock(), ctx_builder=MagicMock())
    assert manager._taskq_init_task is None
    assert manager._taskq is not None
    assert manager._taskq.loop_thread_calls == 0
    manager._taskq.close()


def test_feature_map_covers_all_overload_routes():
    root = Path(__file__).resolve().parents[1]
    feature_map = (root / "docs/feature-map/README.md").read_text(encoding="utf-8")
    for route in (
        "GET /api/tasks",
        "GET /api/tasks/summary",
        "GET /api/tasks/{task_id}",
        "POST /api/tasks/{task_id}",
        "POST /api/tasks/{task_id}/cancel",
        "GET /api/spawn/lanes",
        "GET /api/spawn/{agent_id}/resume",
        "GET /api/sessions/health",
    ):
        assert f"`{route}`" in feature_map
    assert "`answer_input`" in feature_map and "`cancel_wait`" in feature_map
