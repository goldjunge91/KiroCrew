"""Remaining private workflow paths with real schedulers and owned stores.

Only model output and the unit fixture's OS availability are synthetic.
"""

import asyncio

import pytest
from test_workflows_private_execution import world as _world
from test_workflows_receipts import ReceiptModel

from kiro_crew.acp.types import EVENT_COMPLETE, EVENT_TEXT_CHUNK
from kiro_crew.config import KiroCrewConfig
from kiro_crew.context import store_of_session
from kiro_crew.member_memory_auth import private_memory_store_for_session
from kiro_crew.providers.base import LLMEvent
from kiro_crew.session import SessionManager
from kiro_crew.taskrunner import TaskRunner
from kiro_crew.workflow_memory import WorkflowScope
from kiro_crew.workflows.service import WorkflowService

world = _world


@pytest.mark.asyncio
@pytest.mark.parametrize("author_only", [False, True])
async def test_saved_task_plan_executes_on_real_private_taskrunner(
    world, tmp_path_factory, author_only, caplog
):
    import logging

    caplog.set_level(logging.INFO)
    providers = []

    class TaskModel(ReceiptModel):
        def __init__(self, key):
            super().__init__()
            self.key = key
            self._private_memory = bool(private_memory_store_for_session(key))
            self.alive = True

        def is_process_alive(self):
            return self.alive

        def has_active_turn(self):
            return False

        async def shutdown(self):
            self.alive = False

        async def raw(self, message):
            self.sent.append(message)
            yield LLMEvent(kind=EVENT_TEXT_CHUNK, text='{"ok": true}')
            yield LLMEvent(kind=EVENT_COMPLETE, stop_reason="end_turn")

    def factory(key, **kwargs):
        model = TaskModel(key)
        providers.append(model)
        return model

    from kiro_crew.config.sections import WorkspaceConfig

    project_dir = tmp_path_factory.mktemp("workflow-project")
    config = KiroCrewConfig.load()
    config.workspaces["workflow-project"] = WorkspaceConfig(dir=str(project_dir))
    config.save()
    sessions = SessionManager(config, provider_factory=factory)
    service = WorkflowService(sessions=sessions, context_builder=world.builder, persist=False)
    runner = TaskRunner(
        sessions=sessions,
        context_builder=world.builder,
        conversation_log=world.log,
        auto_test=False,
        auto_commit=False,
        work_dir=project_dir,
        max_parallel_steps=1,
        workflow_service=service,
    )
    service.attach_task_runner(runner)
    try:
        saved = service.save_definition(
            "agents:\n  PRIVATE_DIAGNOSTIC_TITLE:\n    prompt: Return a confirmation without editing files\n",
            source_format="task-plan",
        )
        started = await service.start_definition(
            saved["definition"]["id"],
            author="dashboard:alice",
            session_key="" if author_only else "dashboard:alice",
        )
        assert "task_id" in started, started
        await asyncio.wait_for(runner._tasks[started["task_id"]], 20)
        run = runner._runs[started["task_id"]]
        assert run.status == "completed", run.error
        assert providers and any(provider.sent for provider in providers)
        assert all(
            store_of_session(world.log, provider.key) == world.stores["alice"]
            for provider in providers
        )
        scope = await WorkflowScope.restore(started["run_id"])
        assert scope.store == world.stores["alice"]
        assert "PRIVATE_DIAGNOSTIC_TITLE" not in caplog.text
        assert "Private task" in caplog.text
    finally:
        for task in list(runner._tasks.values()):
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await sessions.close_all()


@pytest.mark.asyncio
async def test_private_pool_overflow_and_replacement_keep_scope(world, monkeypatch):
    from kiro_crew.workflows.agent_pool import build_pooled_agent_fn

    scope = await WorkflowScope.admit("wf_overflow", world.builder, "dashboard:alice")
    execute, pool = build_pooled_agent_fn(
        world.sessions,
        run_id=scope.run_id,
        memory_scope=scope,
        context_builder=world.builder,
        max_workers=1,
        max_identities=1,
    )
    try:
        assert await execute("first", {"model": "model-one"}) == scope.store
        first = world.models[-1]
        first.alive = False
        assert await execute("replacement", {"model": "model-one"}) == scope.store
        replacement = world.models[-1]
        assert replacement is not first
        assert await execute("overflow", {"model": "model-two"}) == scope.store
        assert any(model.key.startswith("wf-unpooled:") for model in world.models)

        async def failed_reset():
            raise RuntimeError("deterministic model reset failure")

        monkeypatch.setattr(replacement, "new_conversation", failed_reset)
        assert await execute("hard reset", {"model": "model-one"}) == scope.store
        assert world.models[-1] is not replacement
        assert all(store_of_session(world.log, model.key) == scope.store for model in world.models)
    finally:
        await pool.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["archived", "database-deleted"])
@pytest.mark.parametrize("mid_run", [False, True])
@pytest.mark.parametrize("pooled", [False, True])
async def test_retired_or_deleted_memory_never_allocates_global(
    world, monkeypatch, damage, mid_run, pooled
):
    from test_workflows_private_execution import SCRIPT

    from kiro_crew.memory_stores import archive_member_memory_store, resolve_store_path
    from kiro_crew.workflows import agent_exec, agent_pool

    def invalidate():
        if damage == "archived":
            archive_member_memory_store(world.stores["alice"], expected_owner="alice")
        else:
            from kiro_crew.context import release_cached_memory_store

            path = resolve_store_path(world.stores["alice"])
            # Windows cannot unlink a live SQLite file. Retire the fixture's
            # actual handles before simulating loss of the database itself.
            release_cached_memory_store(world.stores["alice"])
            path.unlink()
            assert not path.exists()

    svc = WorkflowService(
        sessions=world.sessions,
        context_builder=world.builder,
        pool_agents=pooled,
        persist=False,
    )
    if not mid_run:
        invalidate()
        rejected = await svc.start(SCRIPT, session_key="dashboard:alice")
        assert rejected["code"] == "workflow_memory_unavailable"
        assert not world.models
        assert not svc.list_runs()
        return
    base_stream = agent_pool.stream_and_collect
    invalidated = False

    async def complete_then_invalidate(provider, prompt, **kwargs):
        nonlocal invalidated
        result = await base_stream(provider, prompt, **kwargs)
        if not invalidated:
            invalidated = True
            invalidate()
        return result

    for module in (agent_exec, agent_pool):
        monkeypatch.setattr(module, "stream_and_collect", complete_then_invalidate)
    started = await svc.start(SCRIPT, session_key="dashboard:alice")
    run = svc.registry.get(started["run_id"])
    await asyncio.wait_for(run.task, 10)
    assert run.status == "failed", run.result
    assert world.models and all(model._private_memory for model in world.models)


@pytest.mark.asyncio
async def test_http_private_run_read_list_cancel_rerun_scope(world):
    import os
    from types import SimpleNamespace

    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer
    from test_workflows_private_execution import SCRIPT, finished

    from kiro_crew.dashboard.handlers import workflows
    from kiro_crew.dashboard.server import _MIXED_INTERNAL_API_PATHS, _STRICT_INTERNAL_API_PATHS
    from kiro_crew.dashboard.token_auth import token_auth_middleware
    from kiro_crew.member_memory_auth import (
        PROOF_HEADER,
        issue_member_session_proof,
        publish_member_session_pid,
    )

    service = WorkflowService(sessions=world.sessions, context_builder=world.builder, persist=False)
    run = await finished(service, await service.start(SCRIPT, session_key="dashboard:alice"))
    state = SimpleNamespace(
        workflow_service=service,
        context_builder=world.builder,
        conversation_log=world.log,
        owner_id="owner",
    )
    app = web.Application(
        middlewares=[
            token_auth_middleware(
                internal_paths=_STRICT_INTERNAL_API_PATHS,
                mixed_internal_paths=_MIXED_INTERNAL_API_PATHS,
                internal_secret="workflow-test-secret",
            )
        ]
    )
    app["state"] = state
    app.router.add_get("/api/workflows/runs", workflows.api_workflow_runs)
    app.router.add_get("/api/workflows/runs/{run_id}", workflows.api_workflow_run_get)
    app.router.add_post("/api/workflows/runs/{run_id}/cancel", workflows.api_workflow_run_cancel)
    app.router.add_post("/api/workflows/runs/{run_id}/rerun", workflows.api_workflow_run_rerun)
    async with TestClient(TestServer(app, host="127.0.0.1")) as client:
        for member in ("bob", "global", "alice"):
            store = world.stores.get(member, "")
            key = f"dashboard:{member}"
            publish_member_session_pid(os.getpid(), key, memory_store=store)
            headers = {"X-Internal-Secret": "workflow-test-secret", "X-Session-Key": key}
            if store:
                proof = issue_member_session_proof(key, os.getpid())
                assert proof
                headers[PROOF_HEADER] = proof
            allowed = member == "alice"
            detail = await client.get(f"/api/workflows/runs/{run.run_id}", headers=headers)
            assert detail.status == (200 if allowed else 403), (member, await detail.text())
            listing = await client.get("/api/workflows/runs", headers=headers)
            assert listing.status == 200, (member, await listing.text())
            assert bool((await listing.json())["runs"]) == allowed
            for operation in ("cancel", "rerun"):
                response = await client.post(
                    f"/api/workflows/runs/{run.run_id}/{operation}", json={}, headers=headers
                )
                assert response.status == (200 if allowed else 403), await response.text()
                if allowed and operation == "rerun":
                    await finished(service, await response.json())
