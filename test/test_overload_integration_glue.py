"""Integration glue pinned by the final overload-resilience pass.

- ``DependencyCoordinator.subscribe`` fans wake / give-up hooks out to the
  runner adapters beside the subagent manager's own hook.
- The gateway attaches ONE ``RunnerAdmission`` to the TaskRunner and the
  WorkflowService, subscribed to the manager's coordinator (or handing its
  ``tick`` to the reaper pump when there is no coordinator).
- ``continue_conversation`` / ``/api/spawn/*`` answer the typed
  ``native_child_not_resumable`` for a harness-native child id.
- ``POST /api/tasks/{id}`` (``answer_input`` / ``cancel_wait``) and the runner
  cancel adapter behind ``POST /api/tasks/{id}/cancel``.
- ``TaskStore.list_rows(lane=)`` is a SQL predicate.
- A resume window entry skips the spawn stagger.
- ``agent.task_store_journal_mode`` reaches ``TaskStore``.
- ``session_health`` mirrors uncharged native children.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from aiohttp import web
from aiohttp.test_utils import make_mocked_request
from overload_fakes import Clock, backoff

from kiro_crew.acp.session_handle import NATIVE_CHILD_NOT_RESUMABLE
from kiro_crew.dashboard import session_health
from kiro_crew.dashboard.handlers import tasks as tasks_mod
from kiro_crew.slack.gateway import GatewayOrchestrator
from kiro_crew.subagent_manager import admission as admission_mod
from kiro_crew.subagent_manager.continuation import ContinuationCoordinator
from kiro_crew.taskq import dependency as dep_mod
from kiro_crew.taskq import model
from kiro_crew.taskq.adapters import runner as runner_mod
from kiro_crew.taskq.store import TaskStore
from kiro_crew.taskq.waits import WaitRecord


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def store(tmp_path: Path, clock: Clock):
    s = TaskStore(tmp_path / "tasks.db", clock=clock).open()
    yield s
    s.close()


# ── coordinator.subscribe ────────────────────────────────────────────────────


def test_coordinator_subscribe_fans_out_wake_and_fail(store: TaskStore, clock: Clock) -> None:
    primary: list[str] = []
    extra: list[str] = []
    failed: list[tuple[str, str]] = []
    coordinator = dep_mod.DependencyCoordinator(
        store,
        clock=clock,
        backoff=backoff(1.0, 1.0),
        max_attempts=1,
        wake_spacing_secs=0.0,
        on_wake=primary.append,
    )
    coordinator.subscribe(on_wake=extra.append, on_fail=lambda t, r: failed.append((t, r)))
    store.accept([model.TaskRecord(id="w-1", kind=model.KIND_WORKFLOW_AGENT, params={})])
    assert store.claim("w-1") is not None
    assert store.transition("w-1", model.STARTING)
    assert store.transition("w-1", model.RUNNING)
    verdict = coordinator.report(
        "w-1",
        dep_mod.DependencySignal(
            kind=dep_mod.KIND_RATE_LIMITED,
            dependency_scope="github:api",
            source="gh",
            retry_at=clock.t + 5.0,
        ),
    )
    assert verdict.outcome == "wait"
    clock.t += 5.5
    woken = coordinator.tick()
    assert woken == ["w-1"]
    assert primary == ["w-1"] and extra == ["w-1"]
    # The probe fails past max_attempts=1: every fail listener hears it.
    verdict = coordinator.report(
        "w-1",
        dep_mod.DependencySignal(
            kind=dep_mod.KIND_RATE_LIMITED, dependency_scope="github:api", source="gh"
        ),
    )
    assert verdict.outcome != "wait"
    assert failed and failed[0][0] == "w-1"


def test_subscribe_listener_errors_do_not_break_the_wake(store: TaskStore, clock: Clock) -> None:
    seen: list[str] = []

    def _boom(_task_id: str) -> None:
        raise RuntimeError("listener broke")

    coordinator = dep_mod.DependencyCoordinator(store, clock=clock, wake_spacing_secs=0.0)
    coordinator.subscribe(on_wake=_boom)
    coordinator.subscribe(on_wake=seen.append)
    store.accept([model.TaskRecord(id="w-2", kind=model.KIND_TASKRUNNER_STEP, params={})])
    assert store.claim("w-2") is not None
    assert store.transition("w-2", model.STARTING)
    assert store.transition("w-2", model.RUNNING)
    coordinator.report(
        "w-2",
        dep_mod.DependencySignal(
            kind=dep_mod.KIND_RATE_LIMITED,
            dependency_scope="s",
            source="x",
            retry_at=clock.t + 1.0,
        ),
    )
    clock.t += 2.0
    assert coordinator.tick() == ["w-2"]
    assert seen == ["w-2"]


# ── gateway wiring ───────────────────────────────────────────────────────────


class _Runner:
    def __init__(self) -> None:
        self.attached: list[Any] = []

    def attach_task_admission(self, adm: Any) -> None:
        self.attached.append(adm)


def _orch(coordinator: Any, *, store: Any = None) -> GatewayOrchestrator:
    orch = GatewayOrchestrator.__new__(GatewayOrchestrator)
    orch.subagent_mgr = SimpleNamespace(
        dependency_coordinator=lambda: coordinator, _taskq=store, max_concurrent=3
    )
    orch._cfg = SimpleNamespace(agent=SimpleNamespace(adaptive_concurrency_mode="aimd"))
    orch.task_runner = _Runner()
    orch.dashboard_state = SimpleNamespace(workflow_service=_Runner())
    orch._runner_admission = None
    return orch


def test_gateway_attaches_one_admission_to_both_consumers(store: TaskStore, clock: Clock) -> None:
    coordinator = dep_mod.DependencyCoordinator(store, clock=clock)
    orch = _orch(coordinator, store=store)
    orch._wire_runner_admission()
    adm = orch._runner_admission
    assert isinstance(adm, runner_mod.RunnerAdmission)
    assert orch.task_runner.attached == [adm]
    assert orch.dashboard_state.workflow_service.attached == [adm]
    assert adm.coordinator is coordinator
    assert adm.lane.ceiling == 3
    # The admission's wake hook is a subscriber of the manager's coordinator.
    assert adm.on_wake in coordinator._wake_listeners
    assert len(coordinator._fail_listeners) == 1
    orch._unwire_runner_admission()
    assert orch._runner_admission is None
    assert orch.task_runner.attached[-1] is None
    assert orch.dashboard_state.workflow_service.attached[-1] is None


def test_gateway_without_a_coordinator_hands_tick_to_the_reaper_pump() -> None:
    orch = _orch(None)
    orch._wire_runner_admission()
    adm = orch._runner_admission
    assert adm is not None and adm.coordinator is None
    assert orch.subagent_mgr._runner_admission_tick == adm.tick
    orch._unwire_runner_admission()
    assert not hasattr(orch.subagent_mgr, "_runner_admission_tick")


def test_gateway_without_a_manager_wires_nothing() -> None:
    orch = GatewayOrchestrator.__new__(GatewayOrchestrator)
    orch.subagent_mgr = None
    orch._runner_admission = None
    orch._wire_runner_admission()
    assert orch._runner_admission is None


# ── native child refusal ─────────────────────────────────────────────────────


class _Handle:
    def __init__(self, children: set[str], sid: str = "parent-1") -> None:
        self._children = children
        self._sid = sid

    def native_child_resume_refusal(self, conversation_id: str) -> str | None:
        if conversation_id not in self._children:
            return None
        return f"{NATIVE_CHILD_NOT_RESUMABLE}: {conversation_id} is a child of {self._sid}"


def _continuation(sessions: dict[str, Any]) -> ContinuationCoordinator:
    manager = SimpleNamespace(_sessions=SimpleNamespace(_sessions=sessions))
    return ContinuationCoordinator(manager)


def test_native_child_refusal_asks_every_live_handle() -> None:
    sessions = {
        "chat:a": SimpleNamespace(provider=SimpleNamespace(client=_Handle({"kid-1"}))),
        "chat:b": SimpleNamespace(provider=SimpleNamespace(client=object())),
        "chat:c": SimpleNamespace(provider=_Handle({"kid-2"}, sid="p2")),
    }
    cont = _continuation(sessions)
    assert cont.native_child_resume_refusal("kid-1").startswith(NATIVE_CHILD_NOT_RESUMABLE)
    assert "p2" in cont.native_child_resume_refusal("kid-2")
    assert cont.native_child_resume_refusal("nobody") is None


def test_native_child_refusal_without_a_registry_is_none() -> None:
    manager = SimpleNamespace(_sessions=SimpleNamespace())
    assert ContinuationCoordinator(manager).native_child_resume_refusal("x") is None


@pytest.mark.asyncio
async def test_api_spawn_continue_and_steer_answer_409_for_a_native_child(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kiro_crew.dashboard.handlers import messaging

    refusal = f"{NATIVE_CHILD_NOT_RESUMABLE}: kid-1 is a child of parent-1"

    class _Subagents:
        max_concurrent = 4

        def recorded_cwd(self, conv_id: str) -> None:
            return None

        def continue_conversation(self, conv_id: str, task: str, **kw: Any) -> Any:
            return SimpleNamespace(id="n1", done=True, error=refusal)

        async def steer_run(self, agent_id: str, message: str) -> tuple[bool, str]:
            return False, "not_found"

        def native_child_resume_refusal(self, conversation_id: str) -> str | None:
            return refusal if conversation_id == "kid-1" else None

    app = web.Application()
    state = SimpleNamespace(subagents=_Subagents())
    app["state"] = state

    async def _no_refusal(request: Any, *, claimed_session: str = "") -> None:
        return None

    monkeypatch.setattr(messaging, "_spawn_scope_refusal", _no_refusal)
    req = make_mocked_request(
        "POST",
        "/api/spawn/kid-1/continue",
        match_info={"agent_id": "kid-1"},
        app=app,
        payload=None,
    )
    req.json = lambda: _as_future({"task": "go on"})  # type: ignore[method-assign]
    resp = await messaging.api_spawn_continue(req)
    assert resp.status == 409
    assert json.loads(resp.text)["code"] == NATIVE_CHILD_NOT_RESUMABLE

    req2 = make_mocked_request(
        "POST", "/api/spawn/kid-1/steer", match_info={"agent_id": "kid-1"}, app=app
    )
    req2.json = lambda: _as_future({"message": "stop"})  # type: ignore[method-assign]
    resp2 = await messaging.api_spawn_steer(req2)
    assert resp2.status == 409
    assert json.loads(resp2.text)["code"] == NATIVE_CHILD_NOT_RESUMABLE

    req3 = make_mocked_request(
        "POST", "/api/spawn/other/steer", match_info={"agent_id": "other"}, app=app
    )
    req3.json = lambda: _as_future({"message": "stop"})  # type: ignore[method-assign]
    resp3 = await messaging.api_spawn_steer(req3)
    assert resp3.status == 404


def _as_future(value: Any) -> "asyncio.Future[Any]":
    fut: asyncio.Future[Any] = asyncio.get_event_loop().create_future()
    fut.set_result(value)
    return fut


# ── /api/tasks actions + runner cancel adapter ───────────────────────────────


class _Admission:
    def __init__(self, store: TaskStore) -> None:
        self.store = store
        self.answers: list[tuple[str, str]] = []
        self.cancelled: list[str] = []

    def answer_input(self, task_id: str, answer: str) -> bool:
        self.answers.append((task_id, answer))
        from kiro_crew.taskq.waits import WaitLedger

        return WaitLedger(self.store).wake(task_id, reason="input answered") is not None

    def cancel_wait(self, task_id: str, *, reason: str = "") -> bool:
        self.cancelled.append(task_id)
        return self.store.cancel(task_id, reason=reason) is not None


def _runner_state(store: TaskStore) -> SimpleNamespace:
    adm = _Admission(store)
    runner = SimpleNamespace(task_admission=adm, cancel_calls=[])
    runner.cancel = lambda run_id, exact=False: runner.cancel_calls.append((run_id, exact))
    service = SimpleNamespace(_task_admission=adm, cancel_calls=[])

    async def _svc_cancel(run_id: str) -> bool:
        service.cancel_calls.append(run_id)
        return True

    service.cancel = _svc_cancel
    subagents = SimpleNamespace(_taskq=store)

    async def _mgr_cancel(agent_id: str) -> bool:
        return False

    subagents.cancel = _mgr_cancel
    return SimpleNamespace(
        subagents=subagents, task_runner=runner, workflow_service=service, _slots={}
    )


def _req(method: str, path: str, state: Any, *, match: dict, body: Any = None):
    app = web.Application()
    app["state"] = state
    req = make_mocked_request(method, path, match_info=match, app=app)
    if body is not None:
        req.json = lambda: _as_future(body)  # type: ignore[method-assign]
    return req


def _seed_runner_rows(store: TaskStore, clock: Clock) -> None:
    store.accept(
        [
            model.TaskRecord(id="taskrunner:r1:task2", kind=model.KIND_TASKRUNNER_STEP, params={}),
            model.TaskRecord(id="workflow:w1:agent0", kind=model.KIND_WORKFLOW_AGENT, params={}),
            model.TaskRecord(id="taskrunner:r2:task1", kind=model.KIND_TASKRUNNER_STEP, params={}),
        ]
    )
    for tid in ("taskrunner:r1:task2", "workflow:w1:agent0", "taskrunner:r2:task1"):
        assert store.claim(tid) is not None
        assert store.transition(tid, model.STARTING)
        assert store.transition(tid, model.RUNNING)
    inp = WaitRecord.input("call-1", since=clock.t, reason="a prompt")
    assert store.enter_wait("taskrunner:r1:task2", inp.to_dict())


@pytest.mark.asyncio
async def test_answer_input_wakes_a_waiting_runner_row(store: TaskStore, clock: Clock) -> None:
    _seed_runner_rows(store, clock)
    st = _runner_state(store)
    resp = await tasks_mod.api_task_action(
        _req(
            "POST",
            "/api/tasks/taskrunner:r1:task2",
            st,
            match={"task_id": "taskrunner:r1:task2"},
            body={"action": "answer_input", "answer": "yes"},
        )
    )
    assert resp.status == 200, resp.text
    assert st.task_runner.task_admission.answers == [("taskrunner:r1:task2", "yes")]
    # Claimable until the runner's re-admission is granted a slot:
    # the answer is on the wake event, the row is not ``running``.
    assert store.state_of("taskrunner:r1:task2") == model.RETRY_WAIT


@pytest.mark.asyncio
async def test_answer_input_refuses_a_row_not_waiting_for_input(
    store: TaskStore, clock: Clock
) -> None:
    _seed_runner_rows(store, clock)
    st = _runner_state(store)
    resp = await tasks_mod.api_task_action(
        _req(
            "POST",
            "/api/tasks/workflow:w1:agent0",
            st,
            match={"task_id": "workflow:w1:agent0"},
            body={"action": "answer_input", "answer": "yes"},
        )
    )
    assert resp.status == 409
    assert json.loads(resp.text)["code"] == "not_waiting_input"
    empty = await tasks_mod.api_task_action(
        _req(
            "POST",
            "/api/tasks/taskrunner:r1:task2",
            st,
            match={"task_id": "taskrunner:r1:task2"},
            body={"action": "answer_input", "answer": "  "},
        )
    )
    assert empty.status == 400 and json.loads(empty.text)["code"] == "answer_required"
    bad = await tasks_mod.api_task_action(
        _req("POST", "/api/tasks/x", st, match={"task_id": "x"}, body={"action": "nope"})
    )
    assert bad.status == 400 and json.loads(bad.text)["code"] == "bad_action"


@pytest.mark.asyncio
async def test_cancel_wait_ends_the_wait_without_an_answer(store: TaskStore, clock: Clock) -> None:
    _seed_runner_rows(store, clock)
    st = _runner_state(store)
    resp = await tasks_mod.api_task_action(
        _req(
            "POST",
            "/api/tasks/taskrunner:r1:task2",
            st,
            match={"task_id": "taskrunner:r1:task2"},
            body={"action": "cancel_wait"},
        )
    )
    assert resp.status == 200
    assert store.state_of("taskrunner:r1:task2") == model.CANCELLED
    again = await tasks_mod.api_task_action(
        _req(
            "POST",
            "/api/tasks/taskrunner:r1:task2",
            st,
            match={"task_id": "taskrunner:r1:task2"},
            body={"action": "cancel_wait"},
        )
    )
    assert again.status == 409 and json.loads(again.text)["code"] == "terminal"


@pytest.mark.asyncio
async def test_action_on_a_subagent_row_has_no_input_adapter(
    store: TaskStore, clock: Clock
) -> None:
    store.accept([model.TaskRecord(id="sub-1", kind=model.KIND_SUBAGENT, params={})])
    st = _runner_state(store)
    resp = await tasks_mod.api_task_action(
        _req(
            "POST",
            "/api/tasks/sub-1",
            st,
            match={"task_id": "sub-1"},
            body={"action": "cancel_wait"},
        )
    )
    assert resp.status == 409 and json.loads(resp.text)["code"] == "no_input_adapter"


@pytest.mark.asyncio
async def test_cancel_routes_runner_rows_to_their_owner(store: TaskStore, clock: Clock) -> None:
    _seed_runner_rows(store, clock)
    st = _runner_state(store)
    # A parked (waiting_input) step: ended through cancel_wait, the run untouched.
    resp = await tasks_mod.api_task_cancel(
        _req(
            "POST",
            "/api/tasks/taskrunner:r1:task2/cancel",
            st,
            match={"task_id": "taskrunner:r1:task2"},
        )
    )
    assert resp.status == 200 and json.loads(resp.text)["cancelled"] is True
    assert st.task_runner.task_admission.cancelled == ["taskrunner:r1:task2"]
    assert st.task_runner.cancel_calls == []
    # A RUNNING step: its run is cancelled (exact id); the run settles the row.
    resp = await tasks_mod.api_task_cancel(
        _req(
            "POST",
            "/api/tasks/taskrunner:r2:task1/cancel",
            st,
            match={"task_id": "taskrunner:r2:task1"},
        )
    )
    assert resp.status == 200
    assert st.task_runner.cancel_calls == [("r2", True)]
    # A RUNNING workflow agent call: the workflow run is cancelled.
    resp = await tasks_mod.api_task_cancel(
        _req(
            "POST",
            "/api/tasks/workflow:w1:agent0/cancel",
            st,
            match={"task_id": "workflow:w1:agent0"},
        )
    )
    assert resp.status == 200
    assert st.workflow_service.cancel_calls == ["w1"]


def test_owner_of_parses_runner_ids() -> None:
    assert runner_mod.owner_of("taskrunner:r1") == ("taskrunner", "r1")
    assert runner_mod.owner_of("taskrunner:r1:task3") == ("taskrunner", "r1")
    assert runner_mod.owner_of("taskrunner:r1:task3~2") == ("taskrunner", "r1")
    assert runner_mod.owner_of("workflow:wf_000012:agent4") == ("workflow", "wf_000012")
    assert runner_mod.owner_of("workflow:wf_000012:agent4~1") == ("workflow", "wf_000012")
    assert runner_mod.owner_of("abc123") is None


def test_lane_for_is_the_shared_lane_key_policy() -> None:
    from kiro_crew.taskq import lanes

    assert runner_mod.lane_for("cron:job-1", "chat") == lanes.SYSTEM_LANE
    assert runner_mod.lane_for("_hb", "") == lanes.SYSTEM_LANE
    assert runner_mod.lane_for("web-1", "cron") == lanes.SYSTEM_LANE
    assert runner_mod.lane_for("web-1", "chat") == "web-1"


# ── store: list_rows(lane=) ──────────────────────────────────────────────────


def test_list_rows_lane_is_a_store_predicate(store: TaskStore) -> None:
    store.accept(
        [
            model.TaskRecord(id="a", kind=model.KIND_SUBAGENT, session_key="web-a", params={}),
            model.TaskRecord(id="b", kind=model.KIND_SUBAGENT, session_key="web-b", params={}),
            model.TaskRecord(id="c", kind=model.KIND_CRON, session_key="", params={}),
        ]
    )
    assert [r.id for r in store.list_rows(lane="web-a")] == ["a"]
    assert [r.id for r in store.list_rows(lane="system")] == ["c"]
    assert [r.id for r in store.list_rows(lane="web-a", state=model.QUEUED)] == ["a"]
    assert store.list_rows(lane="nope") == []


# ── admission: a resume skips the stagger ────────────────────────────────────


class _Info:
    def __init__(self, id: str) -> None:
        self.id = id
        self.done = False
        self.reaped = False
        self.user_stopped = False
        self._slot_released = True
        self._resume_pending = True
        self._wait_record = {"reason": "x"}
        self._resume_event: Any = None
        self._taskq_generation = 0
        self.parent_session_key = "web-a"
        self.batch_id = ""


def test_resume_entry_is_granted_before_the_stagger_and_does_not_consume_it() -> None:
    import time as _t

    from kiro_crew.subagent_manager.admission import CapacityView

    info = _Info("r-1")
    mgr = SimpleNamespace(
        _queue=[{"_resume_id": "r-1", "_preassigned_id": "r-1", "reason": "woke"}],
        _agents={"r-1": info},
        _running_count=0,
        _max_concurrent=2,
        _spawn_stagger_secs=2.0,
        _last_spawn_ts=_t.monotonic(),  # a start happened just now
        _drain_queue=lambda: None,
        _emit_queue_depth=lambda *a, **k: None,
    )
    calls: list[str] = []

    class _Glue(admission_mod.SpawnAdmissionCoordinator):
        def taskq_store(self):  # type: ignore[override]
            return None

        def taskq_refill_window(self, *, children_only: bool = False) -> int:  # type: ignore[override]
            return 0

        def capacity_view(self) -> CapacityView:  # type: ignore[override]
            return CapacityView(
                cap_total=2,
                running=mgr._running_count,
                child_reserve=0,
                reserve_active=False,
                waiting_parents=0,
            )

        def _maybe_resume_paused_parent(self, info: Any) -> None:  # type: ignore[override]
            return None

        def pick_window_index(self, view: Any = None) -> int | None:  # type: ignore[override]
            return None

    glue = _Glue.__new__(_Glue)
    glue._manager = mgr
    mgr._admission = glue

    async def _fire_event(etype: str, info: Any, extra: Any = None) -> None:
        calls.append(etype)

    mgr._fire_event = _fire_event
    before = mgr._last_spawn_ts
    glue._drain_queue_impl()
    assert mgr._queue == []
    assert info._slot_released is False and info._resume_pending is False
    assert mgr._running_count == 1
    assert mgr._last_spawn_ts == before  # a resume is not a process start


# ── config: task_store_journal_mode ──────────────────────────────────────────


def test_journal_mode_key_reaches_the_store(tmp_path: Path) -> None:
    from kiro_crew import taskq

    home = tmp_path / "home"
    (home / "tasks").mkdir(parents=True)
    s = taskq.open_default_store(home, journal_mode="delete", import_legacy_records=False)
    try:
        assert s.journal_mode == "delete"
        assert any("task_store_journal_mode" in w for w in s.warnings)
    finally:
        s.close()
    s2 = taskq.open_default_store(home, journal_mode="wal", import_legacy_records=False)
    try:
        assert s2.journal_mode == "wal"
        assert s2.warnings == []
    finally:
        s2.close()


def test_journal_mode_config_key_parses_and_clamps(tmp_path: Path) -> None:
    import unittest.mock

    from kiro_crew.config.loader import KiroCrewConfig

    def _loaded(data: dict) -> KiroCrewConfig:
        (tmp_path / "config.json").write_text(json.dumps(data), encoding="utf-8")
        with unittest.mock.patch("kiro_crew.config.loader.config_dir", return_value=tmp_path):
            return KiroCrewConfig.load()

    assert (
        _loaded({"agent": {"task_store_journal_mode": "DELETE"}}).agent.task_store_journal_mode
        == "delete"
    )
    assert (
        _loaded({"agent": {"task_store_journal_mode": "bogus"}}).agent.task_store_journal_mode
        == "auto"
    )
    assert _loaded({}).agent.task_store_journal_mode == "auto"


# ── session_health: uncharged native children ────────────────────────────────


def test_session_health_mirrors_uncharged_native_children() -> None:
    mirror = session_health.uncharged_mirror()
    mirror.report_uncharged("native_children", 0, label="s-old")

    class _H:
        def report_native_children(self, budget: Any) -> int:
            budget.report_uncharged("native_children", 3, label="s-1")
            return 3

    slot = SimpleNamespace(key="web-1", running=True, _acp_client=_H(), messages=[])
    snap = session_health.snapshot_slot(slot)
    assert snap.native_children == 3
    assert mirror.uncharged("native_children") >= 3
    monitor = session_health.SessionHealthMonitor(include_log_scan=False)
    payload = monitor.compute(session_health.HealthSnapshot(slots=[snap]))
    assert payload["uncharged"]["native_children"] >= 3
    assert payload["slots"]["web-1"]["native_children"] == 3
    mirror.report_uncharged("native_children", 0, label="s-1")


# ── store off-loop: the store never blocks the event loop ────────────────────


class TestStoreOffLoop:
    @pytest.mark.asyncio
    async def test_run_executes_on_the_writer_thread(self, store: TaskStore) -> None:
        import threading

        seen: dict[str, Any] = {}

        def _probe() -> int:
            seen["thread"] = threading.current_thread().name
            seen["loop_running"] = TaskStore._on_running_loop_thread()
            return store.count_pending(model.KIND_SUBAGENT)

        assert await store.run(_probe) == 0
        assert seen["thread"].startswith("taskq-writer")
        assert seen["loop_running"] is False
        assert store.loop_thread_calls == 0

    @pytest.mark.asyncio
    async def test_strict_guard_refuses_a_direct_call_on_the_loop(
        self, store: TaskStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Negative control for the guard the async paths are pinned against."""
        monkeypatch.setattr(TaskStore, "strict_loop_guard", True)
        with pytest.raises(RuntimeError, match="event-loop thread"):
            store.count_pending(model.KIND_SUBAGENT)
        # The same call through run() is fine.
        assert await store.run(store.count_pending, model.KIND_SUBAGENT) == 0

    @pytest.mark.asyncio
    async def test_spawn_async_writes_the_row_off_loop_before_starting(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``/api/spawn`` -> ``spawn_async``: the accept (``BEGIN IMMEDIATE``) runs
        on the writer thread, the row exists BEFORE the run starts, and the
        started run carries the id the row was accepted under."""
        import threading
        from unittest.mock import AsyncMock, MagicMock

        from kiro_crew.subagent import SubagentManager

        sessions = MagicMock()
        sessions.get_pid = MagicMock(return_value=None)
        provider = AsyncMock()
        provider.stream = MagicMock(side_effect=lambda *a, **k: iter(()))
        sessions.get_or_create = AsyncMock(return_value=(provider, True, False))
        sessions.get_agent = MagicMock(return_value="")
        sessions.get_approval_policy = MagicMock(return_value="auto")
        sessions.has_session = MagicMock(return_value=True)
        ctx = MagicMock()
        ctx.hooks.auto_approve_subagent_spawn = True
        ctx.hooks.auto_approve_subagent_tools = False
        mgr = SubagentManager(sessions=sessions, ctx_builder=ctx)
        await mgr.wait_taskq_ready()
        mgr._should_use_session_sharing = MagicMock(return_value=False)
        mgr._spawn_stagger_secs = 0.0
        real_store = mgr._admission.taskq_store()
        assert real_store is not None

        accepts: list[dict[str, Any]] = []
        original_accept = real_store.accept_one

        def _spy(record: Any) -> str:
            accepts.append(
                {
                    "id": record.id,
                    "thread": threading.current_thread().name,
                    "loop_running": TaskStore._on_running_loop_thread(),
                    "started_before_accept": record.id in mgr._agents,
                }
            )
            return original_accept(record)

        monkeypatch.setattr(real_store, "accept_one", _spy)
        # End to end through the ``/api/spawn`` glue, with the strict guard
        # ARMED: the accept, the window decision, the claim and the
        # registration writes all run on the writer thread; a store call on
        # the loop raises here instead of passing silently.
        from kiro_crew.dashboard.handlers import messaging

        monkeypatch.setattr(admission_mod.SpawnAdmissionCoordinator, "pump_off_loop", True)
        before = real_store.loop_thread_calls
        monkeypatch.setattr(TaskStore, "strict_loop_guard", True)
        state = SimpleNamespace(subagents=mgr)
        info = await messaging._spawn_on_loop(state, "hello", parent_session_key="web-1", agent="")
        for _ in range(10):
            await asyncio.sleep(0.01)  # let the posted registration writes land
        monkeypatch.setattr(TaskStore, "strict_loop_guard", False)
        # (The fake session makes the run itself fail right after it starts;
        # what this pins is the accept path, not the run.)
        assert info is not None and info.id and not info.queued
        assert accepts and accepts[0]["id"] == info.id
        assert accepts[0]["thread"].startswith("taskq-writer")
        assert accepts[0]["loop_running"] is False
        assert accepts[0]["started_before_accept"] is False
        assert real_store.loop_thread_calls == before  # the test's own reads come after
        assert real_store.get(info.id) is not None
        await mgr.cancel_all()

    @staticmethod
    async def _manager_with_store(monkeypatch: pytest.MonkeyPatch) -> Any:
        from unittest.mock import AsyncMock, MagicMock

        from kiro_crew.subagent import SubagentManager

        sessions = MagicMock()
        sessions.get_pid = MagicMock(return_value=None)
        provider = AsyncMock()
        provider.stream = MagicMock(side_effect=lambda *a, **k: iter(()))
        sessions.get_or_create = AsyncMock(return_value=(provider, True, False))
        sessions.get_agent = MagicMock(return_value="")
        sessions.get_approval_policy = MagicMock(return_value="auto")
        sessions.has_session = MagicMock(return_value=True)
        ctx = MagicMock()
        ctx.hooks.auto_approve_subagent_spawn = True
        ctx.hooks.auto_approve_subagent_tools = False
        mgr = SubagentManager(sessions=sessions, ctx_builder=ctx)
        await mgr.wait_taskq_ready()
        mgr._should_use_session_sharing = MagicMock(return_value=False)
        mgr._spawn_stagger_secs = 0.0
        assert mgr._admission.taskq_store() is not None
        return mgr

    @pytest.mark.asyncio
    async def test_drain_refill_reads_the_store_off_loop(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The pump on a running loop: every store read of the window refill and
        the wait-expiry sweep happens on the writer thread. Pinned with the
        strict guard armed, so a read that slipped back onto the loop raises."""
        mgr = await self._manager_with_store(monkeypatch)
        store = mgr._admission.taskq_store()
        monkeypatch.setattr(admission_mod.SpawnAdmissionCoordinator, "pump_off_loop", True)
        # Two rows waiting on disk: the pump refills the window
        # (pending_lanes / fetch_dispatchable_fair) off-loop, then picks.
        for i in range(2):
            rec = mgr._admission.taskq_build_record(
                f"row{i}",
                {"task": f"t{i}", "parent_session_key": "web-1"},
                parent_session_key="web-1",
                memory_store="",
                app="",
                model="",
                allowed_tools=None,
                approval_mode=None,
            )
            assert mgr._admission.taskq_accept_record(rec) is None
        # Nothing is stubbed: the pump's own reads (expiry sweep, pending_lanes,
        # fetch), the dispatch (``store.claim`` via ``ClaimPoint``), the
        # registration writes (``taskq_mark``), the run's terminal write
        # (``taskq_settle``) and the queue-depth chip's ``count_pending`` all
        # run under the strict guard, which raises on any loop-thread call.
        monkeypatch.setattr(TaskStore, "strict_loop_guard", True)
        before = store.loop_thread_calls
        mgr._drain_queue()
        task = getattr(mgr, "_drain_task", None)
        assert task is not None, "on a running loop with a store the pump is a coroutine"
        await task
        for _ in range(20):  # let the started runs finish and settle
            await asyncio.sleep(0.01)
        await mgr._drain_queue_async()
        for _ in range(20):
            await asyncio.sleep(0.01)
        assert store.loop_thread_calls == before
        monkeypatch.setattr(TaskStore, "strict_loop_guard", False)
        states = {rid: store.state_of(rid) for rid in ("row0", "row1")}
        # Both rows were claimed and started (the fake session makes the run
        # fail, which is itself a settle through the writer thread).
        assert all(st not in (model.QUEUED, model.ADMITTED) for st in states.values()), states
        assert any(st in model.TERMINAL for st in states.values()), states
        await mgr.cancel_all()

    @pytest.mark.asyncio
    async def test_continuation_and_app_spawn_accept_off_loop(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The two remaining accept paths -- ``continue_conversation_async`` and
        the app ``SpawnSDK`` -- write their row through ``spawn_async``."""
        import threading

        mgr = await self._manager_with_store(monkeypatch)
        store = mgr._admission.taskq_store()
        threads: list[str] = []
        original_accept = store.accept_one

        def _spy(record: Any) -> str:
            threads.append(threading.current_thread().name)
            return original_accept(record)

        monkeypatch.setattr(store, "accept_one", _spy)
        # continuation: the prelude's own checks pass for an unknown conversation
        # only through the manager's recorded state; stub it to the spawn step.
        monkeypatch.setattr(
            mgr,
            "_continue_prelude",
            lambda *a, **k: {"task": "again", "parent_session_key": "web-1"},
        )
        info = await mgr.continue_conversation_async("conv-1", "again", parent_session_key="web-1")
        assert info is not None and not info.error, info
        # app SpawnSDK
        from kiro_crew.apps import spawn_sdk

        class _Agent:
            name = "demo--worker"
            filename = "demo--worker.json"

        monkeypatch.setattr(spawn_sdk, "list_agents", lambda: [_Agent()])
        impl = spawn_sdk.build_spawn_impl(mgr)
        agent_id = await impl("do it", "demo--worker", True, "", "demo")
        assert agent_id
        assert threads and all(t.startswith("taskq-writer") for t in threads), threads
        assert len(threads) == 2
        await mgr.cancel_all()


# ── a claim the store cannot take never starts an untracked run ─────────────


@pytest.mark.asyncio
async def test_claim_unavailable_leaves_the_row_queued_instead_of_starting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from unittest.mock import AsyncMock, MagicMock

    from kiro_crew import taskq as _taskq
    from kiro_crew.subagent import SubagentManager

    sessions = MagicMock()
    sessions.get_pid = MagicMock(return_value=None)
    sessions.get_or_create = AsyncMock()
    sessions.get_agent = MagicMock(return_value="")
    sessions.get_approval_policy = MagicMock(return_value="auto")
    ctx = MagicMock()
    ctx.hooks.auto_approve_subagent_spawn = True
    mgr = SubagentManager(sessions=sessions, ctx_builder=ctx)
    await mgr.wait_taskq_ready()
    mgr._spawn_stagger_secs = 0.0
    store = mgr._admission.taskq_store()
    assert store is not None

    def _busy(*_a: Any, **_k: Any) -> Any:
        raise _taskq.TaskStoreUnavailable("database is locked")

    monkeypatch.setattr(store, "claim", _busy)
    gen, proceed, reason = mgr._admission.taskq_claim("x1")
    assert (gen, proceed, reason) == (0, False, mgr._admission.CLAIM_UNAVAILABLE)

    info = mgr.spawn("do it", parent_session_key="web-1")
    assert info is not None and info.queued and not info.done, info
    assert info.id not in mgr._agents, "an unclaimed row must not start"
    assert mgr._running_count == 0
    assert store.state_of(info.id) == _taskq.QUEUED
    await mgr.cancel_all()


# ── mutable gates run once per submission; a drained refusal cancels its row ─


@pytest.mark.asyncio
async def test_store_accepted_reentry_skips_the_mutable_gates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``spawn_async`` commits the row after ``prepare_spawn`` passed the policy
    gates; the ``_store_accepted`` re-entry must not evaluate them again, so a
    governance/cwd change during the commit cannot refuse a committed row."""
    from unittest.mock import AsyncMock, MagicMock

    from kiro_crew import subagent as subagent_mod
    from kiro_crew.subagent import SubagentManager

    sessions = MagicMock()
    sessions.get_pid = MagicMock(return_value=None)
    provider = AsyncMock()
    provider.stream = MagicMock(side_effect=lambda *a, **k: iter(()))
    sessions.get_or_create = AsyncMock(return_value=(provider, True, False))
    sessions.get_agent = MagicMock(return_value="")
    sessions.get_approval_policy = MagicMock(return_value="auto")
    sessions.has_session = MagicMock(return_value=True)
    ctx = MagicMock()
    ctx.hooks.auto_approve_subagent_spawn = True
    ctx.hooks.auto_approve_subagent_tools = False
    mgr = SubagentManager(sessions=sessions, ctx_builder=ctx)
    await mgr.wait_taskq_ready()
    mgr._should_use_session_sharing = MagicMock(return_value=False)
    mgr._spawn_stagger_secs = 0.0
    store = mgr._admission.taskq_store()
    assert store is not None

    calls: list[str] = []
    verdict = {"deny": False}

    def _gov(*_a: Any, **_k: Any) -> str | None:
        calls.append("gov")
        return "spawn disabled" if verdict["deny"] else None

    monkeypatch.setattr(subagent_mod, "_vet_spawn_governance", _gov)
    original_accept = store.accept_one

    def _flip_then_accept(record: Any) -> str:
        verdict["deny"] = True  # governance changes DURING the commit
        return original_accept(record)

    monkeypatch.setattr(store, "accept_one", _flip_then_accept)
    info = await mgr.spawn_async("hello", parent_session_key="web-1")
    assert info is not None and not info.error, info
    assert calls == ["gov"], "the gate ran exactly once, in prepare_spawn"
    assert info.id in mgr._agents and not mgr._agents[info.id].done
    await mgr.cancel_all()


@pytest.mark.asyncio
async def test_a_drained_row_refused_by_the_pump_is_failed_in_the_store(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from unittest.mock import AsyncMock, MagicMock

    from kiro_crew import subagent as subagent_mod
    from kiro_crew import taskq as _taskq
    from kiro_crew.subagent import SubagentManager

    sessions = MagicMock()
    sessions.get_pid = MagicMock(return_value=None)
    sessions.get_or_create = AsyncMock()
    sessions.get_agent = MagicMock(return_value="")
    sessions.get_approval_policy = MagicMock(return_value="auto")
    ctx = MagicMock()
    ctx.hooks.auto_approve_subagent_spawn = True
    mgr = SubagentManager(sessions=sessions, ctx_builder=ctx)
    await mgr.wait_taskq_ready()
    mgr._spawn_stagger_secs = 0.0
    store = mgr._admission.taskq_store()
    assert store is not None
    rec = mgr._admission.taskq_build_record(
        "drained1",
        {"task": "t", "parent_session_key": "web-1"},
        parent_session_key="web-1",
        memory_store="",
        app="",
        model="",
        allowed_tools=None,
        approval_mode=None,
    )
    assert mgr._admission.taskq_accept_record(rec) is None
    monkeypatch.setattr(subagent_mod, "_vet_spawn_governance", lambda *a, **k: "spawn disabled")
    info = mgr.spawn("t", parent_session_key="web-1", _from_queue=True, _preassigned_id="drained1")
    assert info is not None and info.done and "governance" in (info.error or "")
    assert (
        store.state_of("drained1") == _taskq.FAILED
    ), "the caller's refusal is the store's verdict"
    await mgr.cancel_all()


# ── wave accounting counts each member exactly once ──────────────────────────


@pytest.mark.asyncio
async def test_batch_members_are_counted_once_including_a_prepare_refusal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A three-member wave through ``spawn_async``: one member is refused in
    ``prepare_spawn`` (bad cwd). Every member is counted exactly once, so the
    wave can close, and ``counted: true`` on ``/api/spawn`` is true for the
    refused member too."""
    from unittest.mock import AsyncMock, MagicMock

    from kiro_crew.subagent import SubagentManager

    sessions = MagicMock()
    sessions.get_pid = MagicMock(return_value=None)
    provider = AsyncMock()
    provider.stream = MagicMock(side_effect=lambda *a, **k: iter(()))
    sessions.get_or_create = AsyncMock(return_value=(provider, True, False))
    sessions.get_agent = MagicMock(return_value="")
    sessions.get_approval_policy = MagicMock(return_value="auto")
    sessions.has_session = MagicMock(return_value=True)
    ctx = MagicMock()
    ctx.hooks.auto_approve_subagent_spawn = True
    ctx.hooks.auto_approve_subagent_tools = False
    mgr = SubagentManager(sessions=sessions, ctx_builder=ctx)
    await mgr.wait_taskq_ready()
    mgr._should_use_session_sharing = MagicMock(return_value=False)
    mgr._spawn_stagger_secs = 0.0
    assert mgr._admission.taskq_store() is not None

    ok1 = await mgr.spawn_async("a", parent_session_key="web-1", batch_id="w1", batch_total=3)
    bad = await mgr.spawn_async(
        "b", parent_session_key="web-1", batch_id="w1", batch_total=3, cwd="/definitely/not/here"
    )
    ok2 = await mgr.spawn_async("c", parent_session_key="web-1", batch_id="w1", batch_total=3)
    assert ok1 is not None and ok2 is not None and not ok1.queued and not ok2.queued
    assert bad is not None and bad.done and bad.error and "cwd" in bad.error
    assert mgr._batch_submitted["w1"][0] == 3, mgr._batch_submitted
    await mgr.cancel_all()


# ── an enabled queue with no store REFUSES ───────────────────────────────────


@pytest.mark.asyncio
async def test_spawn_is_refused_typed_when_the_enabled_store_is_unavailable() -> None:
    from unittest.mock import AsyncMock, MagicMock

    from kiro_crew.subagent import SubagentManager
    from kiro_crew.subagent_manager.admission import SpawnAdmissionCoordinator

    sessions = MagicMock()
    sessions.get_pid = MagicMock(return_value=None)
    sessions.get_or_create = AsyncMock()
    sessions.get_agent = MagicMock(return_value="")
    sessions.get_approval_policy = MagicMock(return_value="auto")
    ctx = MagicMock()
    ctx.hooks.auto_approve_subagent_spawn = True
    mgr = SubagentManager(sessions=sessions, ctx_builder=ctx)
    await mgr.wait_taskq_ready()
    # The queue is enabled but its store did not open (what taskq_open records).
    mgr._taskq = None
    mgr._taskq_unavailable = "durable task queue unavailable: disk I/O error"
    assert mgr._admission.taskq_required_but_unavailable()
    for entry in (mgr.spawn, mgr.spawn_async):
        result = entry("do it", parent_session_key="web-1")
        info = await result if asyncio.iscoroutine(result) else result
        assert info is not None and info.done
        assert info.error_code == SpawnAdmissionCoordinator.TASK_STORE_UNAVAILABLE_CODE
        assert "disk I/O error" in (info.error or "")
        assert info.id not in mgr._agents or mgr._agents[info.id].done
    assert mgr._queue == []
    assert mgr._running_count == 0


# ── a committed app spawn queues on capacity; a refused one fails its row ────


async def _app_manager(monkeypatch: pytest.MonkeyPatch) -> Any:
    from unittest.mock import AsyncMock, MagicMock

    from kiro_crew.subagent import SubagentManager

    sessions = MagicMock()
    sessions.get_pid = MagicMock(return_value=None)
    provider = AsyncMock()
    provider.stream = MagicMock(side_effect=lambda *a, **k: iter(()))
    sessions.get_or_create = AsyncMock(return_value=(provider, True, False))
    sessions.get_agent = MagicMock(return_value="")
    sessions.get_approval_policy = MagicMock(return_value="auto")
    sessions.has_session = MagicMock(return_value=True)
    ctx = MagicMock()
    ctx.hooks.auto_approve_subagent_spawn = True
    ctx.hooks.auto_approve_subagent_tools = False
    mgr = SubagentManager(sessions=sessions, ctx_builder=ctx, max_concurrent=1)
    await mgr.wait_taskq_ready()
    mgr._should_use_session_sharing = MagicMock(return_value=False)
    mgr._spawn_stagger_secs = 0.0
    assert mgr._admission.taskq_store() is not None
    return mgr


@pytest.mark.asyncio
async def test_committed_prevalidated_app_spawn_queues_instead_of_refusing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Capacity is a scheduling fact: an accepted (committed) app spawn that
    finds no slot QUEUES like any other row, with its prevalidation dropped so
    the drain re-validates the agent and re-proves app ownership."""
    from kiro_crew import taskq as _taskq

    mgr = await _app_manager(monkeypatch)
    store = mgr._admission.taskq_store()
    mgr._running_count = mgr._max_concurrent  # no slot
    info = await mgr.spawn_async(
        "bg work",
        parent_session_key="web-1",
        agent="demo--worker",
        app="demo",
        _agent_prevalidated=True,
    )
    assert info is not None and info.queued and not info.done, info
    assert store.state_of(info.id) == _taskq.QUEUED
    entry = next(p for p in mgr._queue if p["_preassigned_id"] == info.id)
    assert entry["_agent_prevalidated"] is False
    await mgr.cancel_all()


@pytest.mark.asyncio
async def test_drained_app_spawn_reproves_ownership_or_fails_its_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace as NS

    from kiro_crew import subagent as subagent_mod
    from kiro_crew import taskq as _taskq

    mgr = await _app_manager(monkeypatch)
    store = mgr._admission.taskq_store()
    monkeypatch.setattr(
        subagent_mod,
        "list_agents",
        lambda project_dir=None: [NS(name="other--bg", filename="other--bg.json")],
    )
    mgr._running_count = mgr._max_concurrent
    info = await mgr.spawn_async(
        "bg work",
        parent_session_key="web-1",
        agent="demo--worker",
        app="demo",
        _agent_prevalidated=True,
    )
    assert info is not None and info.queued
    mgr._running_count = 0
    # The drain re-enters the row: the agent is not one of the app's own agents.
    drained = mgr.spawn(
        **{k: v for k, v in mgr._queue[0].items() if k != "_lane"}, _from_queue=True
    )
    assert drained is not None and drained.done and "only spawn its OWN" in (drained.error or "")
    assert store.state_of(info.id) == _taskq.FAILED, "a genuine refusal of a committed row fails it"
    await mgr.cancel_all()


@pytest.mark.asyncio
async def test_durable_row_never_carries_prevalidation(monkeypatch: pytest.MonkeyPatch) -> None:
    """The store row of a prevalidated app spawn has no ``_agent_prevalidated``:
    a start rebuilt from the row (window refill, restart) runs the gates."""
    from types import SimpleNamespace as NS

    from kiro_crew import subagent as subagent_mod
    from kiro_crew import taskq as _taskq
    from kiro_crew.subagent_manager.admission import SpawnAdmissionCoordinator

    mgr = await _app_manager(monkeypatch)
    store = mgr._admission.taskq_store()
    mgr._running_count = mgr._max_concurrent
    info = await mgr.spawn_async(
        "bg work",
        parent_session_key="web-1",
        agent="demo--worker",
        app="demo",
        _agent_prevalidated=True,
    )
    assert info is not None and info.queued
    rec = store.get(info.id)
    assert rec is not None and "_agent_prevalidated" not in rec.params
    # The same holds for the sync accept path (``spawn`` off the loop).
    record = mgr._admission.taskq_build_record(
        "sub-sync",
        {"task": "t", "_agent_prevalidated": True, "cwd": ""},
        parent_session_key="web-1",
        memory_store="",
        app="demo",
        model=None,
        allowed_tools=None,
        approval_mode=None,
    )
    assert "_agent_prevalidated" not in record.params
    # A row written by a build that still stored the flag rebuilds without it.
    legacy = _taskq.TaskRecord(
        id="sub-legacy",
        kind=_taskq.KIND_SUBAGENT,
        session_key="web-1",
        params={"task": "t", "agent": "demo--worker", "app": "demo", "_agent_prevalidated": True},
    )
    assert SpawnAdmissionCoordinator._window_entry(legacy).get("_agent_prevalidated") is None
    # The row rebuilt from the store reaches the ownership gate: with a foreign
    # same-named agent under the app's filename the drained start FAILS.
    monkeypatch.setattr(
        subagent_mod,
        "list_agents",
        lambda project_dir=None: [NS(name="other--bg", filename="other--bg.json")],
    )
    mgr._queue.clear()
    mgr._running_count = 0
    entry = SpawnAdmissionCoordinator._window_entry(rec)
    drained = mgr.spawn(**{k: v for k, v in entry.items() if k != "_lane"}, _from_queue=True)
    assert drained is not None and drained.done and "only spawn its OWN" in (drained.error or "")
    assert store.state_of(info.id) == _taskq.FAILED
    await mgr.cancel_all()


# ── a drain request landing mid-drain is not lost ────────────────────────────


@pytest.mark.asyncio
async def test_drain_request_during_a_drain_runs_a_second_pass(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Capacity released while the coroutine pump is mid-pass: the request is
    coalesced into ``_drain_again`` and the SAME task runs one more pass, so
    the row that became startable is started without waiting for another
    trigger."""
    mgr = await _app_manager(monkeypatch)
    monkeypatch.setattr(admission_mod.SpawnAdmissionCoordinator, "pump_off_loop", True)
    passes: list[int] = []
    original = mgr._drain_queue_pass

    async def _counted_pass() -> None:
        passes.append(len(passes))
        if len(passes) == 1:
            mgr._drain_queue()  # a release lands while this pass runs
        await original()

    monkeypatch.setattr(mgr, "_drain_queue_pass", _counted_pass)
    mgr._drain_queue()
    task = getattr(mgr, "_drain_task")
    await task
    assert passes == [0, 1], "the coalesced request produced exactly one more pass"
    assert getattr(mgr, "_drain_again") is False
    await mgr.cancel_all()


# ── reserve-then-commit: the claim await cannot overshoot the cap ────────────


@pytest.mark.asyncio
async def test_a_spawn_during_the_parked_claim_queues_instead_of_overshooting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``ClaimPoint`` reserves the slot BEFORE the claim is awaited. A spawn that
    lands while the claim is parked on the writer thread sees the cap spent and
    queues; when the claim returns, the re-entry consumes the reservation, so
    ``_running_count`` never exceeds ``_max_concurrent``."""
    import threading

    mgr = await _app_manager(monkeypatch)  # max_concurrent=1
    store = mgr._admission.taskq_store()
    monkeypatch.setattr(admission_mod.SpawnAdmissionCoordinator, "pump_off_loop", True)
    rec = mgr._admission.taskq_build_record(
        "parked",
        {"task": "t", "parent_session_key": "web-1"},
        parent_session_key="web-1",
        memory_store="",
        app="",
        model="",
        allowed_tools=None,
        approval_mode=None,
    )
    assert mgr._admission.taskq_accept_record(rec) is None
    parked = threading.Event()
    release = threading.Event()
    real_claim = store.claim

    def _slow_claim(task_id, *a, **kw):
        parked.set()
        assert release.wait(5), "the test never released the claim"
        return real_claim(task_id, *a, **kw)

    monkeypatch.setattr(store, "claim", _slow_claim)
    mgr._drain_queue()
    drain = getattr(mgr, "_drain_task")
    while not parked.is_set():
        await asyncio.sleep(0.005)
    # The claim is parked on the writer thread; the reservation is held.
    assert mgr._running_count == 1 == mgr._max_concurrent
    loser = mgr.spawn("late", parent_session_key="web-1")  # a sync accept-path spawn
    assert loser is not None and loser.queued and not loser.done, loser
    assert loser.id not in mgr._agents
    assert mgr._running_count == 1
    release.set()
    await drain
    for _ in range(10):
        await asyncio.sleep(0.01)
    assert "parked" in mgr._agents
    assert mgr._running_count <= mgr._max_concurrent
    await mgr.cancel_all()


@pytest.mark.asyncio
async def test_a_refused_or_unavailable_claim_releases_the_reservation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kiro_crew import taskq as _taskq

    mgr = await _app_manager(monkeypatch)
    store = mgr._admission.taskq_store()
    monkeypatch.setattr(admission_mod.SpawnAdmissionCoordinator, "pump_off_loop", True)
    for rid in ("r-unavailable", "r-cancelled"):
        rec = mgr._admission.taskq_build_record(
            rid,
            {"task": "t", "parent_session_key": "web-1"},
            parent_session_key="web-1",
            memory_store="",
            app="",
            model="",
            allowed_tools=None,
            approval_mode=None,
        )
        assert mgr._admission.taskq_accept_record(rec) is None

    def _busy(*_a, **_k):
        raise _taskq.TaskStoreUnavailable("locked")

    monkeypatch.setattr(store, "claim", _busy)
    params = {"task": "t", "parent_session_key": "web-1", "_preassigned_id": "r-unavailable"}
    info = await mgr._dispatch_async(params)
    assert info is not None and info.queued and not info.done
    assert mgr._running_count == 0, "an unavailable claim gives the reserved slot back"
    monkeypatch.undo()
    monkeypatch.setattr(admission_mod.SpawnAdmissionCoordinator, "pump_off_loop", True)
    store.cancel("r-cancelled", reason="user stop")
    info = await mgr._dispatch_async({**params, "_preassigned_id": "r-cancelled"})
    assert info is not None and info.done and info.user_stopped
    assert mgr._running_count == 0, "a refused claim gives the reserved slot back"
    await mgr.cancel_all()


# ── posted store writes are drained by cancel_all ───────────────────────────


@pytest.mark.asyncio
async def test_posted_terminal_writes_are_tracked_and_drained_by_cancel_all(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A ``taskq_settle`` posted to the writer thread is a ``_report_tasks``
    member: ``cancel_all`` waits for it, so a gateway stopping right after a
    run ended does not leave a ``running`` row for the next boot to re-run."""
    import threading

    from kiro_crew import taskq as _taskq

    mgr = await _app_manager(monkeypatch)
    store = mgr._admission.taskq_store()
    monkeypatch.setattr(admission_mod.SpawnAdmissionCoordinator, "pump_off_loop", True)
    rec = mgr._admission.taskq_build_record(
        "done1",
        {"task": "t", "parent_session_key": "web-1"},
        parent_session_key="web-1",
        memory_store="",
        app="",
        model="",
        allowed_tools=None,
        approval_mode=None,
    )
    assert mgr._admission.taskq_accept_record(rec) is None
    claimed = store.claim("done1")
    assert claimed is not None
    from kiro_crew.subagent import SubagentInfo

    info = SubagentInfo(
        id="done1", task="t", parent_session_key="web-1", done=True, user_stopped=True
    )
    info._taskq_generation = claimed.generation
    gate = threading.Event()
    real_finish = store.finish

    def _slow_finish(*a, **kw):
        assert gate.wait(5)
        return real_finish(*a, **kw)

    monkeypatch.setattr(store, "finish", _slow_finish)
    mgr._admission.taskq_settle(info)
    posted = [t for t in mgr._report_tasks if not t.done()]
    assert posted, "the posted settle is tracked"
    gate.set()
    await mgr.cancel_all()
    assert all(t.done() for t in posted)
    assert store.state_of("done1") == _taskq.CANCELLED


# ── the W3 branch (nested child, parent blocked in spawn_sub_agents) off-loop ──


@pytest.mark.asyncio
async def test_nested_child_of_a_blocked_parent_runs_w3_off_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A child accepted through ``/api/spawn`` for a parent parked in
    ``spawn_sub_agents``: the parent yields its slot (``waiting_children``)
    with the ledger read, the deadline read and the wait write all on the
    writer thread -- pinned with the strict guard armed for the whole call."""
    from types import SimpleNamespace as NS

    from kiro_crew import taskq as _taskq
    from kiro_crew.dashboard.handlers import messaging

    mgr = await _app_manager(monkeypatch)
    mgr._max_concurrent = 2
    store = mgr._admission.taskq_store()
    monkeypatch.setattr(admission_mod.SpawnAdmissionCoordinator, "pump_off_loop", True)
    # The parent: a live run (a hand-built record, so the fake session cannot
    # end it), in the store as ``running``, blocked in the blocking tool.
    from kiro_crew.subagent import SubagentInfo

    rec = mgr._admission.taskq_build_record(
        "parent1",
        {"task": "parent", "parent_session_key": "web-1"},
        parent_session_key="web-1",
        memory_store="",
        app="",
        model="",
        allowed_tools=None,
        approval_mode=None,
    )
    assert mgr._admission.taskq_accept_record(rec) is None
    claimed = store.claim("parent1")
    assert claimed is not None
    store.transition("parent1", _taskq.STARTING, generation=claimed.generation)
    store.transition("parent1", _taskq.RUNNING, generation=claimed.generation)
    live = SubagentInfo(id="parent1", task="parent", parent_session_key="web-1")
    live._taskq_generation = claimed.generation
    live._inflight_tool = NS(tool_name="@kirocrew-core/spawn_sub_agents", title="call-1")
    mgr._agents["parent1"] = live
    mgr._running_count = 1
    parent = live

    async def _hang(info: Any) -> None:  # the child stays live for the scenario
        await asyncio.Event().wait()

    monkeypatch.setattr(mgr, "_run", _hang)
    before = store.loop_thread_calls
    monkeypatch.setattr(TaskStore, "strict_loop_guard", True)
    child = await messaging._spawn_on_loop(
        NS(subagents=mgr), "child", parent_session_key=f"subagent:{parent.id}"
    )
    for _ in range(20):
        await asyncio.sleep(0.01)  # posted writes land (still under the guard)
    # The accept AND the W3 branch (ledger read, deadline read, enter_wait)
    # completed without a loop-thread store call.
    assert store.loop_thread_calls == before
    monkeypatch.setattr(TaskStore, "strict_loop_guard", False)
    assert child is not None and child.id in mgr._agents
    assert live._slot_released is True, "the parent yielded on the loop, synchronously"
    assert (live._wait_record or {}).get("state") == _taskq.WAITING_CHILDREN
    assert child.id in live._wait_record["resume_condition"]["ids"]
    rec = store.get(parent.id)
    assert rec is not None and rec.state == _taskq.WAITING_CHILDREN
    await mgr.cancel_all()


# ── a raise inside the re-entry never leaks the reserved slot ────────────────


@pytest.mark.asyncio
async def test_a_raise_inside_reenter_releases_the_reservation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kiro_crew import taskq as _taskq

    mgr = await _app_manager(monkeypatch)
    store = mgr._admission.taskq_store()
    monkeypatch.setattr(admission_mod.SpawnAdmissionCoordinator, "pump_off_loop", True)
    rec = mgr._admission.taskq_build_record(
        "boom1",
        {"task": "t", "parent_session_key": "web-1"},
        parent_session_key="web-1",
        memory_store="",
        app="",
        model="",
        allowed_tools=None,
        approval_mode=None,
    )
    assert mgr._admission.taskq_accept_record(rec) is None
    before = mgr._running_count
    point = admission_mod.ClaimPoint("boom1")
    mgr._running_count += 1  # what spawn_impl does when it hands back a ClaimPoint

    def _reenter(_claimed):
        raise RuntimeError("unwrapped agents-dir scan")

    with pytest.raises(RuntimeError):
        await mgr._admission.claim_and_start(point, _reenter)
    assert mgr._running_count == before, "the reservation is released when re-entry raises"
    assert "boom1" not in mgr._agents
    # The claim itself landed; the row is leased to this incarnation and the
    # pump's next claim keeps that lease (not lost, not a phantom run).
    assert store.state_of("boom1") == _taskq.ADMITTED
    assert mgr._admission.taskq_lease_is_ours("boom1")
    # The pump path swallows the same raise without leaking either.
    monkeypatch.setattr(
        mgr,
        "spawn",
        lambda **kw: (
            (_ for _ in ()).throw(RuntimeError("boom"))
            if kw.get("_claimed")
            else (
                admission_mod.ClaimPoint(kw["_preassigned_id"])
                if not (mgr.__dict__.__setitem__("_running_count", mgr._running_count + 1))
                else None
            )
        ),
    )
    rec2 = mgr._admission.taskq_build_record(
        "boom2",
        {"task": "t", "parent_session_key": "web-1"},
        parent_session_key="web-1",
        memory_store="",
        app="",
        model="",
        allowed_tools=None,
        approval_mode=None,
    )
    assert mgr._admission.taskq_accept_record(rec2) is None
    mgr._queue.append({"task": "t", "parent_session_key": "web-1", "_preassigned_id": "boom2"})
    await mgr._drain_queue_async()
    assert mgr._running_count == before
    await mgr.cancel_all()


# ── the pump sees a deferred row only after its defer landed ─────────────────


@pytest.mark.asyncio
async def test_admitting_id_is_held_until_the_posted_defer_landed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import threading

    from kiro_crew import subagent as subagent_mod

    mgr = await _app_manager(monkeypatch)
    store = mgr._admission.taskq_store()
    monkeypatch.setattr(admission_mod.SpawnAdmissionCoordinator, "pump_off_loop", True)
    monkeypatch.setattr(subagent_mod, "check_memory_available", lambda min_gb: (False, 0.5))
    gate = threading.Event()
    real_defer = store.defer
    seen: dict[str, Any] = {}

    def _slow_defer(task_id, until, *, reason):
        assert gate.wait(5)
        seen["until"] = until
        return real_defer(task_id, until, reason=reason)

    monkeypatch.setattr(store, "defer", _slow_defer)
    task = asyncio.ensure_future(mgr.spawn_async("pressure", parent_session_key="web-1"))
    for _ in range(20):
        await asyncio.sleep(0.005)
    admitting = getattr(mgr, "_admitting_ids")
    assert (
        len(admitting) == 1
    ), "the row stays excluded from the refill while its defer is in flight"
    (aid,) = tuple(admitting)
    assert aid in mgr._admission.taskq_excluded_ids()
    gate.set()
    info = await task
    assert info is not None and info.queued
    assert not admitting, "cleared only once the defer landed"
    rec = store.get(info.id)
    assert rec is not None and rec.next_run_at == seen["until"] > store.now()
    await mgr.cancel_all()


# ── a corrupt tasks.db does not fail-close spawns: quarantined, recreated ─────


@pytest.mark.asyncio
async def test_corrupt_tasks_db_is_quarantined_and_spawns_proceed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import os

    from kiro_crew.taskq.store import TaskStore as _TS

    home = Path(os.environ["KIROCREW_HOME"])
    path = _TS.default_path(home)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"garbage, not sqlite\n" * 32)
    mgr = await _app_manager(monkeypatch)  # opens the default store at construction
    store = mgr._admission.taskq_store()
    assert store is not None, "the queue is not fail-closed by a corrupt file"
    assert store.quarantined_to is not None and store.quarantined_to.exists()
    assert mgr._admission.taskq_required_but_unavailable() is None
    info = await mgr.spawn_async("after corruption", parent_session_key="web-1")
    assert info is not None and not info.done and info.id in mgr._agents
    assert store.get(info.id) is not None
    await mgr.cancel_all()
