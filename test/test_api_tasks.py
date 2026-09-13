"""``/api/tasks*``: rows, detail, cancel-through-the-cascade, summary, auth.

A real ``TaskStore`` on a temp file with an injected clock, a fake dashboard
state whose ``subagents`` carries the store and a recording ``cancel``, and the
REAL token middleware for the auth case. No manager, no sockets, no sleeps.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
from aiohttp import web
from aiohttp.test_utils import make_mocked_request
from overload_fakes import Clock, open_task_store

from kiro_crew.dashboard import session_health
from kiro_crew.dashboard.handlers import tasks as tasks_mod
from kiro_crew.taskq import model
from kiro_crew.taskq.store import TaskStore
from kiro_crew.taskq.waits import WaitRecord


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def store(tmp_path: Path, clock: Clock):
    yield from open_task_store(tmp_path, clock)


@pytest.fixture(autouse=True)
def _fresh_monitor(monkeypatch: pytest.MonkeyPatch):
    """A private health monitor: the default one carries progress memos and
    registered cap sources from other tests in the same process."""
    mon = session_health.SessionHealthMonitor(include_log_scan=False)
    monkeypatch.setattr(session_health, "_default_monitor", mon)
    yield mon


class FakeManager:
    """Just enough of ``SubagentManager`` for the handlers: the store and ``cancel``.

    ``cancel`` records the call and, like the real cascade, ends the row
    through the STORE's own cancel (the manager owns that write); the handler
    under test must never touch the store directly.
    """

    def __init__(self, store: TaskStore, *, honours: bool = True) -> None:
        self._taskq = store
        self.cancel_calls: list[str] = []
        self._honours = honours
        self.max_concurrent = 4
        self.running_count = 0
        self._queue: list[dict[str, Any]] = []
        self.running: list[Any] = []

    async def cancel(self, agent_id: str) -> bool:
        self.cancel_calls.append(agent_id)
        if not self._honours:
            return False
        return self._taskq.cancel(agent_id, reason="user_stop") is not None


def _state(store: TaskStore, **kw: Any) -> SimpleNamespace:
    return SimpleNamespace(subagents=FakeManager(store, **kw), _slots={})


def _req(method: str, path: str, state: Any, *, query: str = "", match: dict | None = None):
    # A REAL Application: make_mocked_request's default app is a Mock whose
    # ``["state"]`` answers a fresh MagicMock, which reads as a phantom store.
    app = web.Application()
    app["state"] = state
    return make_mocked_request(
        method, path + (f"?{query}" if query else ""), match_info=match or {}, app=app
    )


def _body(resp: web.Response) -> dict[str, Any]:
    assert resp.text is not None
    return json.loads(resp.text)


def _seed(store: TaskStore, clock: Clock) -> dict[str, str]:
    """A queue with one row per interesting state; returns ids by role."""
    rows = [
        model.TaskRecord(
            id="q-1", kind=model.KIND_SUBAGENT, session_key="web-a", params={"task": "a"}
        ),
        model.TaskRecord(
            id="q-2", kind=model.KIND_SUBAGENT, session_key="web-b", params={"task": "b"}
        ),
        model.TaskRecord(id="sys-1", kind=model.KIND_CRON, session_key="", params={"job": "j"}),
        model.TaskRecord(id="run-1", kind=model.KIND_SUBAGENT, session_key="web-a", params={}),
        model.TaskRecord(id="dep-1", kind=model.KIND_SUBAGENT, session_key="web-a", params={}),
        model.TaskRecord(id="inp-1", kind=model.KIND_SUBAGENT, session_key="web-b", params={}),
        model.TaskRecord(id="rec-1", kind=model.KIND_SUBAGENT, session_key="web-b", params={}),
        model.TaskRecord(id="done-1", kind=model.KIND_SUBAGENT, session_key="web-a", params={}),
    ]
    store.accept(rows)
    clock.t += 30.0
    # run-1: claimed and running.
    assert store.claim("run-1") is not None
    assert store.transition("run-1", model.STARTING)
    assert store.transition("run-1", model.RUNNING)
    # dep-1: running → waiting_dependency (github:api, retry in 90s).
    assert store.claim("dep-1") is not None
    assert store.transition("dep-1", model.STARTING)
    assert store.transition("dep-1", model.RUNNING)
    dep = WaitRecord.dependency("github:api", since=clock.t, retry_at=clock.t + 90.0)
    assert store.enter_wait("dep-1", dep.to_dict())
    # inp-1: running → waiting_input.
    assert store.claim("inp-1") is not None
    assert store.transition("inp-1", model.STARTING)
    assert store.transition("inp-1", model.RUNNING)
    inp = WaitRecord.input("call-7", since=clock.t, reason="sudo wants a password")
    assert store.enter_wait("inp-1", inp.to_dict())
    # rec-1: dispatched once, now recovering with a backoff.
    assert store.claim("rec-1") is not None
    assert store.transition("rec-1", model.STARTING)
    assert store.transition("rec-1", model.RECOVERING, next_run_at=clock.t + 4.0)
    # q-2: a transient in-run failure parked it in retry_wait (no runtime held).
    assert store.claim("q-2") is not None
    assert store.transition("q-2", model.STARTING)
    assert store.transition("q-2", model.RETRY_WAIT, next_run_at=clock.t + 9.0)
    # done-1: finished.
    assert store.claim("done-1") is not None
    assert store.transition("done-1", model.STARTING)
    assert store.transition("done-1", model.RUNNING)
    assert store.transition("done-1", model.DONE)
    clock.t += 60.0
    return {r.id: r.id for r in rows}


# ── GET /api/tasks ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_list_returns_rows_with_lane_and_wait_fields(store: TaskStore, clock: Clock) -> None:
    _seed(store, clock)
    resp = await tasks_mod.api_tasks_list(_req("GET", "/api/tasks", _state(store)))
    assert resp.status == 200
    body = _body(resp)
    assert body["available"] is True
    assert body["count"] == 8
    by_id = {t["id"]: t for t in body["tasks"]}
    assert by_id["sys-1"]["lane"] == "system"
    assert by_id["q-1"]["lane"] == "web-a"
    dep = by_id["dep-1"]
    assert dep["state"] == "waiting_dependency"
    assert dep["wait"]["resume_kind"] == "at_time"
    assert dep["wait"]["dependency_scope"] == "github:api"
    assert dep["wait_since"] == pytest.approx(1_030.0)
    assert dep["wait_reason"]
    assert by_id["inp-1"]["wait_reason"] == "sudo wants a password"
    assert by_id["rec-1"]["attempts"] == 1
    assert by_id["rec-1"]["next_run_at"] == pytest.approx(1_034.0)
    assert by_id["run-1"]["lease_owner"] == store.incarnation
    assert by_id["done-1"]["terminal"] is True
    assert by_id["q-1"]["parent_id"] is None and by_id["q-1"]["root_id"] == "q-1"


@pytest.mark.asyncio
async def test_list_filters_by_state_lane_and_limit(store: TaskStore, clock: Clock) -> None:
    _seed(store, clock)
    st = _state(store)
    body = _body(
        await tasks_mod.api_tasks_list(_req("GET", "/api/tasks", st, query="state=queued"))
    )
    assert sorted(t["id"] for t in body["tasks"]) == ["q-1", "sys-1"]
    body = _body(await tasks_mod.api_tasks_list(_req("GET", "/api/tasks", st, query="lane=system")))
    assert [t["id"] for t in body["tasks"]] == ["sys-1"]
    body = _body(await tasks_mod.api_tasks_list(_req("GET", "/api/tasks", st, query="lane=web-b")))
    assert sorted(t["id"] for t in body["tasks"]) == ["inp-1", "q-2", "rec-1"]
    body = _body(
        await tasks_mod.api_tasks_list(
            _req("GET", "/api/tasks", st, query="lane=web-b&state=queued")
        )
    )
    assert [t["id"] for t in body["tasks"]] == []
    body = _body(
        await tasks_mod.api_tasks_list(
            _req("GET", "/api/tasks", st, query="lane=web-b&state=retry_wait")
        )
    )
    assert [t["id"] for t in body["tasks"]] == ["q-2"]
    body = _body(await tasks_mod.api_tasks_list(_req("GET", "/api/tasks", st, query="limit=2")))
    assert body["count"] == 2 and body["limit"] == 2
    # Ceiling, not an error, for an oversized limit.
    body = _body(
        await tasks_mod.api_tasks_list(_req("GET", "/api/tasks", st, query="limit=999999"))
    )
    assert body["limit"] == tasks_mod._MAX_LIMIT


@pytest.mark.asyncio
async def test_nested_row_reports_its_roots_lane(store: TaskStore, clock: Clock) -> None:
    """The stored ``lane`` column (resolved at accept) wins over derivation."""
    _seed(store, clock)
    store.accept(
        [
            model.TaskRecord(
                id="child-1",
                kind=model.KIND_SUBAGENT,
                session_key="subagent:run-1",
                parent_id="run-1",
                params={},
            )
        ]
    )
    body = _body(
        await tasks_mod.api_tasks_list(_req("GET", "/api/tasks", _state(store), query="lane=web-a"))
    )
    by_id = {t["id"]: t for t in body["tasks"]}
    assert by_id["child-1"]["lane"] == "web-a"
    assert by_id["child-1"]["parent_id"] == "run-1" and by_id["child-1"]["root_id"] == "run-1"


@pytest.mark.asyncio
@pytest.mark.parametrize("query", ["state=bogus", "limit=0", "limit=x", "limit=-3"])
async def test_list_rejects_bad_filters(store: TaskStore, query: str) -> None:
    resp = await tasks_mod.api_tasks_list(_req("GET", "/api/tasks", _state(store), query=query))
    assert resp.status == 400
    assert _body(resp)["code"] in ("bad_state", "bad_limit")


@pytest.mark.asyncio
async def test_list_without_a_store_reports_unavailable() -> None:
    state = SimpleNamespace(subagents=SimpleNamespace(_taskq=None), _slots={})
    body = _body(await tasks_mod.api_tasks_list(_req("GET", "/api/tasks", state)))
    assert body == {"available": False, "tasks": [], "count": 0}


# ── GET /api/tasks/{id} ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_detail_returns_row_and_events_tail(store: TaskStore, clock: Clock) -> None:
    _seed(store, clock)
    resp = await tasks_mod.api_task_detail(
        _req("GET", "/api/tasks/dep-1", _state(store), match={"task_id": "dep-1"})
    )
    assert resp.status == 200
    body = _body(resp)
    assert body["task"]["id"] == "dep-1"
    kinds = [e["kind"] for e in body["events"]]
    assert kinds[0] == "accepted"
    assert "claimed" in kinds
    assert kinds[-1] == "transition"
    assert body["events"][-1]["data"].get("wait") is True
    # Oldest first, seq ascending.
    assert [e["seq"] for e in body["events"]] == sorted(e["seq"] for e in body["events"])


@pytest.mark.asyncio
async def test_detail_404_for_unknown_id(store: TaskStore) -> None:
    resp = await tasks_mod.api_task_detail(
        _req("GET", "/api/tasks/nope", _state(store), match={"task_id": "nope"})
    )
    assert resp.status == 404
    assert _body(resp)["code"] == "not_found"


# ── POST /api/tasks/{id}/cancel ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_cancel_goes_through_the_manager_not_the_store(
    store: TaskStore, clock: Clock, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed(store, clock)
    st = _state(store)
    # The handler must not call the store's own writers: guard every one.
    for name in ("cancel", "transition", "finish", "defer", "wake_wait", "enter_wait"):
        real = getattr(store, name)

        def _trip(*a: Any, _n: str = name, _real: Any = real, **kw: Any) -> Any:
            if st.subagents.cancel_calls and _n == "cancel":
                # The MANAGER's cascade is allowed to write (it did the call).
                return _real(*a, **kw)
            raise AssertionError(f"handler wrote the store directly via {_n}")

        monkeypatch.setattr(store, name, _trip)
    resp = await tasks_mod.api_task_cancel(
        _req("POST", "/api/tasks/q-1/cancel", st, match={"task_id": "q-1"})
    )
    assert resp.status == 200, resp.text
    body = _body(resp)
    assert body["ok"] is True and body["cancelled"] is True
    assert body["task"]["state"] == "cancelled"
    assert st.subagents.cancel_calls == ["q-1"]
    assert store.state_of("q-1") == "cancelled"


@pytest.mark.asyncio
async def test_cancel_of_a_live_wait_also_routes_through_the_manager(
    store: TaskStore, clock: Clock
) -> None:
    _seed(store, clock)
    st = _state(store)
    resp = await tasks_mod.api_task_cancel(
        _req("POST", "/api/tasks/inp-1/cancel", st, match={"task_id": "inp-1"})
    )
    assert resp.status == 200
    assert st.subagents.cancel_calls == ["inp-1"]
    assert store.state_of("inp-1") == "cancelled"


@pytest.mark.asyncio
async def test_cancel_terminal_row_is_409_and_never_calls_the_manager(
    store: TaskStore, clock: Clock
) -> None:
    _seed(store, clock)
    st = _state(store)
    resp = await tasks_mod.api_task_cancel(
        _req("POST", "/api/tasks/done-1/cancel", st, match={"task_id": "done-1"})
    )
    assert resp.status == 409
    assert _body(resp)["code"] == "terminal"
    assert st.subagents.cancel_calls == []
    assert store.state_of("done-1") == "done"


@pytest.mark.asyncio
async def test_cancel_without_an_adapter_is_409_and_leaves_the_row(
    store: TaskStore, clock: Clock
) -> None:
    _seed(store, clock)
    st = _state(store, honours=False)
    resp = await tasks_mod.api_task_cancel(
        _req("POST", "/api/tasks/run-1/cancel", st, match={"task_id": "run-1"})
    )
    assert resp.status == 409
    body = _body(resp)
    assert body["code"] == "no_cancel_adapter"
    assert body["task"]["state"] == "running"
    assert store.state_of("run-1") == "running"


@pytest.mark.asyncio
async def test_cancel_unknown_id_404(store: TaskStore) -> None:
    st = _state(store)
    resp = await tasks_mod.api_task_cancel(
        _req("POST", "/api/tasks/nope/cancel", st, match={"task_id": "nope"})
    )
    assert resp.status == 404
    assert st.subagents.cancel_calls == []


# ── GET /api/tasks/summary ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_summary_shape_depth_oldest_wait_and_waits(
    store: TaskStore, clock: Clock, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed(store, clock)
    monkeypatch.setattr(tasks_mod.resource_status, "adaptive_state", lambda: None)
    resp = await tasks_mod.api_tasks_summary(_req("GET", "/api/tasks/summary", _state(store)))
    assert resp.status == 200
    body = _body(resp)
    assert body["available"] is True
    assert set(body) >= {
        "generated_at",
        "depth",
        "oldest_wait_secs",
        "lanes",
        "degrade_reason",
        "adaptive",
        "slots",
        "waiting",
        "recovering",
        "stalled",
        "counts",
    }
    depth = body["depth"]
    assert depth["by_state"] == {
        "queued": 2,
        "retry_wait": 1,
        "running": 1,
        "waiting_dependency": 1,
        "waiting_input": 1,
        "recovering": 1,
        "done": 1,
    }
    assert depth["queued"] == 3  # queued + admitted + retry_wait + waiting_infra
    assert depth["waiting"] == 2
    # recovering + retry_wait: both are rows being retried (retry_wait also
    # counts as queued above -- it waits for capacity too).
    assert depth["recovering"] == 2
    assert depth["running"] == 1
    assert depth["total"] == 8
    # Oldest claimable row was accepted at t=1000 and the clock is at 1090.
    assert body["oldest_wait_secs"] == pytest.approx(90.0)
    waits = {w["id"]: w for w in body["waiting"]}
    assert (
        waits["dep-1"]["wait_reason"] and waits["dep-1"]["wait"]["dependency_scope"] == "github:api"
    )
    assert waits["inp-1"]["state"] == "waiting_input"
    assert waits["inp-1"]["age_secs"] == pytest.approx(60.0)
    rec = body["recovering"]
    assert sorted(r["id"] for r in rec["tasks"]) == ["q-2", "rec-1"]
    assert rec["task_attempts"] == 2
    assert rec["slots"] == []
    assert {row["layer"] for row in rec["ladder"]} >= {"L1_tool_call", "L4_gatewayd"}
    # The manager's live cap lands as the subagents lane even without a controller.
    assert body["lanes"]["subagents"]["effective"] == 4
    assert body["degrade_reason"] is None
    assert body["adaptive"] is None


@pytest.mark.asyncio
async def test_summary_merges_adaptive_caps_and_degrade_reason(
    store: TaskStore, clock: Clock, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed(store, clock)
    fake_adaptive = {
        "enabled": True,
        "mode": "aimd",
        "effective_exec_cap": 2,
        "exec_ceiling": 8,
        "spawn_gate_capacity": 3,
        "gate_ceiling": 8,
        "paused": False,
        "probing": False,
        "last": {"action": "decrease", "reason": "loop_lag=410ms,timeouts=0.4"},
    }
    monkeypatch.setattr(tasks_mod.resource_status, "adaptive_state", lambda: fake_adaptive)
    body = _body(
        await tasks_mod.api_tasks_summary(_req("GET", "/api/tasks/summary", _state(store)))
    )
    lanes = body["lanes"]
    # The health monitor's own reading (the live manager cap) wins for `effective`;
    # the controller supplies the ceiling and the gate lane.
    assert lanes["subagents"]["effective"] == 4
    assert lanes["subagents"]["user_max"] == 8
    assert lanes["spawn_gate"] == {"effective": 3, "user_max": 8}
    assert body["degrade_reason"] == "loop_lag=410ms,timeouts=0.4"
    assert body["adaptive"]["effective_exec_cap"] == 2


@pytest.mark.asyncio
async def test_summary_prefers_a_registered_pressure_source(
    store: TaskStore, clock: Clock, monkeypatch: pytest.MonkeyPatch, _fresh_monitor
) -> None:
    _seed(store, clock)
    _fresh_monitor.register_pressure_source(lambda: "memory_critical")
    _fresh_monitor.register_cap_source("spawn_gate", lambda: {"effective": 1, "user_max": 8})
    monkeypatch.setattr(
        tasks_mod.resource_status, "adaptive_state", lambda: {"paused": True, "last": None}
    )
    body = _body(
        await tasks_mod.api_tasks_summary(_req("GET", "/api/tasks/summary", _state(store)))
    )
    assert body["degrade_reason"] == "memory_critical"
    assert body["lanes"]["spawn_gate"]["effective"] == 1


@pytest.mark.asyncio
async def test_summary_paused_controller_reads_as_degraded(
    store: TaskStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        tasks_mod.resource_status,
        "adaptive_state",
        lambda: {"paused": True, "last": {"action": "pause", "reason": "memory<2GB"}},
    )
    body = _body(
        await tasks_mod.api_tasks_summary(_req("GET", "/api/tasks/summary", _state(store)))
    )
    assert body["degrade_reason"] == "memory<2GB"
    assert body["depth"]["total"] == 0 and body["available"] is True


@pytest.mark.asyncio
async def test_summary_without_a_store_is_empty_not_an_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(tasks_mod.resource_status, "adaptive_state", lambda: None)
    state = SimpleNamespace(subagents=SimpleNamespace(_taskq=None), _slots={})
    resp = await tasks_mod.api_tasks_summary(_req("GET", "/api/tasks/summary", state))
    assert resp.status == 200
    body = _body(resp)
    assert body["available"] is False
    assert body["depth"]["total"] == 0
    assert body["waiting"] == [] and body["recovering"]["tasks"] == []


@pytest.mark.asyncio
async def test_summary_survives_a_mock_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """A MagicMock state (much of the suite) must read as unavailable, not 500."""
    monkeypatch.setattr(tasks_mod.resource_status, "adaptive_state", lambda: None)
    app = web.Application()
    app["state"] = MagicMock()
    resp = await tasks_mod.api_tasks_summary(
        make_mocked_request("GET", "/api/tasks/summary", app=app)
    )
    assert resp.status == 200
    assert _body(resp)["available"] is False


# ── auth ─────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method,path",
    [
        ("GET", "/api/tasks"),
        ("GET", "/api/tasks/summary"),
        ("GET", "/api/tasks/abc"),
        ("POST", "/api/tasks/abc/cancel"),
    ],
)
async def test_routes_are_refused_without_a_token(method: str, path: str) -> None:
    """Drive the REAL token middleware: no cookie, no token, no internal secret."""
    from kiro_crew.dashboard.server import _MIXED_INTERNAL_API_PATHS, _STRICT_INTERNAL_API_PATHS
    from kiro_crew.dashboard.token_auth import token_auth_middleware

    # Not internal routes: an MCP caller with only the loopback secret is
    # refused too, exactly like /api/sessions/health.
    assert path not in _STRICT_INTERNAL_API_PATHS and path not in _MIXED_INTERNAL_API_PATHS

    mw = token_auth_middleware(
        internal_paths=_STRICT_INTERNAL_API_PATHS,
        mixed_internal_paths=_MIXED_INTERNAL_API_PATHS,
        internal_secret="s3cret",
    )
    reached: list[str] = []

    async def _handler(_request: web.Request) -> web.Response:
        reached.append(_request.path)
        return web.json_response({"ok": True})

    for headers in ({}, {"X-Internal-Secret": "s3cret"}):
        req = MagicMock(spec=web.Request)
        req.path = path
        req.query = {}
        req.cookies = {}
        req.remote = "127.0.0.1"
        req.headers = headers
        req.method = method
        resp = await mw(req, _handler)
        assert resp.status in (401, 403), f"{method} {path} admitted with {headers}: {resp.status}"
    assert reached == []


def test_routes_are_registered_in_order() -> None:
    """``/summary`` must be registered before ``/{task_id}`` or it is swallowed."""
    from kiro_crew.dashboard.routes import system as system_routes

    app = web.Application()
    system_routes.register(app)
    paths = [
        r.resource.canonical
        for r in app.router.routes()
        if getattr(r.resource, "canonical", "").startswith("/api/tasks")
    ]
    assert paths.index("/api/tasks/summary") < paths.index("/api/tasks/{task_id}")
    assert "/api/tasks/{task_id}/cancel" in paths
    assert "/api/tasks" in paths
