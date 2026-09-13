"""The scheduler consumes authoritative turn results and preserves retry state."""

import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from kiro_crew.monitoring.models import MonitorActionDisposition
from kiro_crew.organization import DEFAULT_STAFFING, OWNER, OrganizationStore
from kiro_crew.organization_runtime import OrganizationRunner


@pytest.mark.asyncio
@pytest.mark.parametrize("completion", [True, False])
async def test_delivery_uses_private_thread_and_requires_authoritative_completion(
    tmp_path, monkeypatch, completion
):
    store = OrganizationStore(tmp_path / "organization.sqlite3")
    member = store.enroll(
        OWNER, name="Conductor", memory_store="private-root", role="conductor", manager_id=None
    )
    store.configure(
        OWNER,
        revision=store.snapshot()["settings"]["revision"],
        concurrency=1,
        enabled=True,
        staffing=DEFAULT_STAFFING,
    )
    store.assign(OWNER, member, title="Work", acceptance="Evidence")
    run = store.claim_run()
    messages = []
    slot = SimpleNamespace(
        agent="Conductor",
        memory_store="private-root",
        running=False,
        _lock=asyncio.Lock(),
        append=lambda *args, **kwargs: messages.append((args, kwargs)),
    )
    state = SimpleNamespace(_slots={"member-root": slot}, _background_tasks=set())
    monkeypatch.setattr("kiro_crew.organization_runtime.verify_member", lambda _: None)
    opener = AsyncMock(return_value=web.json_response({"slot_key": "member-root"}))
    monkeypatch.setattr("kiro_crew.dashboard.handlers.members.ensure_member_thread", opener)

    async def turn(_state, _slot, notice, *, monitor_completion, _synthetic_payload):
        assert _synthetic_payload
        assert await monitor_completion.authorize()
        assert "org_inbox" in notice
        if completion:
            await monitor_completion.complete(MonitorActionDisposition.SUCCESS)

    monkeypatch.setattr("kiro_crew.dashboard.chat_runner._run_chat", turn)
    await OrganizationRunner(state, store)._deliver(run)
    result = store.snapshot()["runs"][0]
    assert result["state"] == ("completed" if completion else "failed")
    assert messages[0][0][0] == "inject"
    # Provider completion cannot accept the business assignment.
    assert store.snapshot()["tasks"][0]["state"] == "queued"


@pytest.mark.asyncio
async def test_busy_member_thread_returns_the_claim_without_losing_work(tmp_path, monkeypatch):
    store = OrganizationStore(tmp_path / "organization.sqlite3")
    member = store.enroll(
        OWNER, name="Conductor", memory_store="private-root", role="conductor", manager_id=None
    )
    store.configure(
        OWNER,
        revision=store.snapshot()["settings"]["revision"],
        concurrency=1,
        enabled=True,
        staffing=DEFAULT_STAFFING,
    )
    store.message(OWNER, member, "A material update")
    run = store.claim_run()
    monkeypatch.setattr("kiro_crew.organization_runtime.verify_member", lambda _: None)
    monkeypatch.setattr(
        "kiro_crew.dashboard.handlers.members.ensure_member_thread",
        AsyncMock(return_value=web.json_response({"code": "member_slot_conflict"}, status=409)),
    )
    await OrganizationRunner(SimpleNamespace(), store)._deliver(run)
    assert store.snapshot()["runs"][0]["state"] == "queued"
    assert store.claim_run()["id"] == run["id"]


@pytest.mark.asyncio
@pytest.mark.parametrize("corrupt", [False, True])
async def test_optional_recovery_starts_after_bind_and_cannot_break_dashboard(
    tmp_path, monkeypatch, corrupt
):
    from kiro_crew import organization_runtime
    from kiro_crew.dashboard import server

    path = tmp_path / "organization.sqlite3"
    if corrupt:
        path.write_bytes(b"not a SQLite database")
    else:
        await asyncio.to_thread(OrganizationStore(path).snapshot)
    monkeypatch.setattr(organization_runtime, "organization_path", lambda: path)
    recovered = []
    original = OrganizationStore.recover_runs

    def recover(store):
        recovered.append(True)
        return original(store)

    monkeypatch.setattr(OrganizationStore, "recover_runs", recover)
    app = web.Application()
    app["state"] = SimpleNamespace(_slots={}, _background_tasks=set())
    app.cleanup_ctx.append(server._organization_lifecycle)

    async def healthy(_request):
        return web.json_response({"ready": True})

    app.router.add_get("/health", healthy)
    async with TestClient(TestServer(app)) as client:
        assert recovered == [], "runner.setup must not touch organization storage"
        assert (await client.get("/health")).status == 200
        server._kick_organization(app)
        await asyncio.wait_for(app["organization_startup_task"], timeout=5)
        assert recovered == [True]
        assert ("organization_runner" in app) is not corrupt
        assert (await client.get("/health")).status == 200


@pytest.mark.asyncio
async def test_shutdown_drains_recovery_before_releasing_app_state(tmp_path, monkeypatch):
    from kiro_crew import organization_runtime
    from kiro_crew.dashboard import server

    path = tmp_path / "organization.sqlite3"
    await asyncio.to_thread(OrganizationStore(path).snapshot)
    monkeypatch.setattr(organization_runtime, "organization_path", lambda: path)
    entered = asyncio.Event()
    release = threading.Event()
    finished = threading.Event()
    loop = asyncio.get_running_loop()
    original = OrganizationStore.recover_runs

    def recover(store):
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(5), "test did not release the recovery worker"
        try:
            return original(store)
        finally:
            finished.set()

    monkeypatch.setattr(OrganizationStore, "recover_runs", recover)
    app = web.Application()
    app["state"] = SimpleNamespace(_slots={}, _background_tasks=set())
    app.cleanup_ctx.append(server._organization_lifecycle)
    runner = web.AppRunner(app)
    await runner.setup()
    server._kick_organization(app)
    cleanup = None
    try:
        await asyncio.wait_for(entered.wait(), timeout=5)
        cleanup = asyncio.create_task(runner.cleanup())
        await asyncio.sleep(0)  # let cleanup deliver cancellation to initialization
        assert not cleanup.done()
        assert not finished.is_set()
    finally:
        release.set()
        if cleanup is not None:
            await asyncio.wait_for(cleanup, timeout=5)
        else:
            await runner.cleanup()
    assert finished.is_set()
    assert app["organization_startup_task"].cancelled()
    assert "organization_runner" not in app
