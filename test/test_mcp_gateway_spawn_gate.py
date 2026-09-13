"""The daemon-global spawn gate and the ``spawn_queue`` protocol around it.

Mirrors ``test_mcp_gateway_breaker.py`` in spirit: the gate is a small
in-process object, so its contract is pinned directly -- FIFO order, a permit
whose ``settle`` is exactly-once and separate from ``release``, cancellation
that is neutral at every boundary, a capacity seam that admits waiters when
raised and revokes nothing when cut, and a drain that fails every waiter and
releases every watcher. Time is injected wherever the gate reads it; the one
real ``asyncio`` timing this file relies on is sub-100 ms.

The second half drives ``gatewayd._handle_connection`` with the same scripted
reader / recording writer the coverage suites use, so the WIRE is pinned too:
an old stub never sees ``queued`` and keeps its ``fallback: true`` capacity
rejection, a new stub gets ``queued`` keepalives and a classed rejection with
no fallback, pings are answered while an acquire waits, and a fallback charges
the host budget until the connection closes.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from kiro_crew.mcp_gateway import admission as adm
from kiro_crew.mcp_gateway import gatewayd as gw
from kiro_crew.mcp_gateway import host_budget as hb
from kiro_crew.mcp_gateway.backend import Backend
from kiro_crew.mcp_gateway.pool import BackendUnavailable, PoolAtCapacity, PoolKey

# --- clock -------------------------------------------------------------------


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


async def _settle() -> None:
    """Let every ready callback run (a few loop turns)."""
    for _ in range(5):
        await asyncio.sleep(0)


# --- SpawnGate unit --------------------------------------------------------


class TestPermit:
    @pytest.mark.asyncio
    async def test_settle_is_exactly_once_and_release_is_idempotent(self) -> None:
        gate = adm.SpawnGate(2)
        permit = await gate.acquire(label="a")
        permit.settle(adm.OUTCOME_SUCCESS)
        permit.settle(adm.OUTCOME_FAILURE)  # ignored: the first outcome sticks
        assert permit.outcome == adm.OUTCOME_SUCCESS
        permit.release()
        permit.release()
        assert gate.in_flight == 0
        assert gate.snapshot()["outcomes"] == {"success": 1, "failure": 0, "neutral": 0}

    @pytest.mark.asyncio
    async def test_release_without_settle_records_neutral(self) -> None:
        gate = adm.SpawnGate(1)
        permit = await gate.acquire(label="a")
        permit.release()
        assert permit.outcome == adm.OUTCOME_NEUTRAL
        assert gate.snapshot()["outcomes"]["neutral"] == 1

    @pytest.mark.asyncio
    async def test_unknown_outcome_is_refused(self) -> None:
        gate = adm.SpawnGate(1)
        permit = await gate.acquire(label="a")
        with pytest.raises(ValueError):
            permit.settle("maybe")
        permit.release()

    @pytest.mark.asyncio
    async def test_on_settle_seam_sees_each_outcome_once(self) -> None:
        seen: list[str] = []
        gate = adm.SpawnGate(2, on_settle=seen.append)
        a = await gate.acquire(label="a")
        b = await gate.acquire(label="b")
        a.settle(adm.OUTCOME_FAILURE)
        a.release()
        b.release()
        assert seen == [adm.OUTCOME_FAILURE, adm.OUTCOME_NEUTRAL]


class TestFifo:
    @pytest.mark.asyncio
    async def test_waiters_are_admitted_in_arrival_order(self) -> None:
        gate = adm.SpawnGate(1)
        first = await gate.acquire(label="first")
        order: list[str] = []

        async def wait(label: str) -> adm.Permit:
            permit = await gate.acquire(label=label)
            order.append(label)
            return permit

        tasks = [asyncio.create_task(wait(f"w{i}")) for i in range(3)]
        await _settle()
        assert gate.queued == 3 and gate.in_flight == 1
        first.release()
        await _settle()
        assert order == ["w0"]
        (await tasks[0]).release()
        await _settle()
        (await tasks[1]).release()
        await _settle()
        (await tasks[2]).release()
        assert order == ["w0", "w1", "w2"]
        assert gate.in_flight == 0 and gate.queued == 0

    @pytest.mark.asyncio
    async def test_a_newcomer_never_jumps_the_queue(self) -> None:
        gate = adm.SpawnGate(1)
        held = await gate.acquire(label="held")
        queued = asyncio.create_task(gate.acquire(label="queued"))
        await _settle()
        held.release()
        # The slot went to the queued waiter, not to whoever asks next.
        late = asyncio.create_task(gate.acquire(label="late"))
        await _settle()
        assert queued.done() and not late.done()
        (await queued).release()
        await _settle()
        (await late).release()


class TestCancellation:
    @pytest.mark.asyncio
    async def test_a_cancelled_waiter_leaves_the_queue_and_counts_nothing(self) -> None:
        gate = adm.SpawnGate(1)
        held = await gate.acquire(label="held")
        waiter = asyncio.create_task(gate.acquire(label="w"))
        await _settle()
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert gate.queued == 0
        held.release()
        assert gate.in_flight == 0
        assert gate.snapshot()["outcomes"] == {"success": 0, "failure": 0, "neutral": 1}
        assert gate.snapshot()["cancelled"] == 1

    @pytest.mark.asyncio
    async def test_cancel_after_grant_but_before_resume_hands_the_slot_back(self) -> None:
        gate = adm.SpawnGate(1)
        held = await gate.acquire(label="held")
        waiter = asyncio.create_task(gate.acquire(label="w"))
        await _settle()
        # Grant lands (release wakes the waiter) and the cancel arrives on the
        # same turn, before the waiter's coroutine resumes.
        held.release()
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert gate.in_flight == 0, "the granted slot must be handed back"
        # And the next arrival gets it at once.
        nxt = await gate.acquire(label="next")
        nxt.release()


class TestCapacitySeam:
    @pytest.mark.asyncio
    async def test_raising_capacity_admits_queued_waiters(self) -> None:
        gate = adm.SpawnGate(1, floor=1, ceiling=8)
        held = await gate.acquire(label="held")
        waiters = [asyncio.create_task(gate.acquire(label=f"w{i}")) for i in range(3)]
        await _settle()
        assert gate.queued == 3
        assert gate.set_capacity(3) == 3
        await _settle()
        assert sum(w.done() for w in waiters) == 2 and gate.in_flight == 3
        held.release()
        await _settle()
        assert all(w.done() for w in waiters)
        for w in waiters:
            (await w).release()

    @pytest.mark.asyncio
    async def test_cutting_capacity_revokes_nothing_and_admits_less_afterwards(self) -> None:
        gate = adm.SpawnGate(4, floor=1, ceiling=8)
        permits = [await gate.acquire(label=str(i)) for i in range(4)]
        assert gate.set_capacity(1) == 1
        assert gate.in_flight == 4, "in-flight spawns finish; nothing is revoked"
        waiter = asyncio.create_task(gate.acquire(label="w"))
        for p in permits[:3]:
            p.release()
            await _settle()
            assert not waiter.done(), "still over the new capacity"
        permits[3].release()
        await _settle()
        assert waiter.done()
        (await waiter).release()

    def test_capacity_is_clamped_to_the_band(self) -> None:
        gate = adm.SpawnGate(100, floor=2, ceiling=6)
        assert gate.capacity == 6
        assert gate.set_capacity(0) == 2
        assert gate.set_capacity(4) == 4
        with pytest.raises(ValueError):
            adm.SpawnGate(1, floor=0)
        with pytest.raises(ValueError):
            adm.SpawnGate(1, floor=3, ceiling=2)


class TestDeadlineAndKeepalive:
    @pytest.mark.asyncio
    async def test_keepalive_reports_position_and_the_deadline_times_out(self) -> None:
        clock = _Clock()
        gate = adm.SpawnGate(1, clock=clock)
        held = await gate.acquire(label="held")
        ticks: list[adm.QueuePosition] = []

        async def on_queued(pos: adm.QueuePosition) -> None:
            ticks.append(pos)
            clock.now += 5.0  # each keepalive tick costs one interval

        with pytest.raises(adm.SpawnGateTimeout) as excinfo:
            await gate.acquire(
                label="w", deadline=clock.now + 12.0, on_queued=on_queued, keepalive_secs=0.01
            )
        assert len(ticks) >= 2
        assert ticks[0].position == 1 and ticks[0].capacity == 1
        assert ticks[0].frame()["type"] == "queued"
        assert excinfo.value.position == 1
        assert gate.queued == 0 and gate.snapshot()["timeouts"] == 1
        held.release()

    @pytest.mark.asyncio
    async def test_positions_are_one_based_and_skip_finished_waiters(self) -> None:
        clock = _Clock()
        gate = adm.SpawnGate(1, clock=clock)
        held = await gate.acquire(label="held")
        seen: dict[str, int] = {}

        def _cb(label: str) -> Any:
            async def on_queued(pos: adm.QueuePosition) -> None:
                seen.setdefault(label, pos.position)

            return on_queued

        w1 = asyncio.create_task(gate.acquire(label="w1", on_queued=_cb("w1"), keepalive_secs=0.01))
        await _settle()
        w2 = asyncio.create_task(gate.acquire(label="w2", on_queued=_cb("w2"), keepalive_secs=0.01))
        await asyncio.sleep(0.05)
        assert seen == {"w1": 1, "w2": 2}
        held.release()
        (await w1).release()
        (await w2).release()


class TestDrain:
    @pytest.mark.asyncio
    async def test_close_fails_waiters_refuses_newcomers_and_releases_watchers(self) -> None:
        gate = adm.SpawnGate(1)
        held = await gate.acquire(label="held")
        waiter = asyncio.create_task(gate.acquire(label="w"))
        await _settle()
        # A watcher parked on an initialize that will never come, holding the
        # permit that is blocking the waiter.
        done = asyncio.Event()
        watched = held
        task = gate.watch_initialize(
            watched,
            init_done=done,
            init_state=lambda: "unsent",
            process_exited=AsyncMock(),
            timeout=60.0,
        )
        await gate.close()
        with pytest.raises(adm.SpawnGateClosed):
            await waiter
        assert task.done() and watched.released and watched.outcome == adm.OUTCOME_NEUTRAL
        with pytest.raises(adm.SpawnGateClosed):
            await gate.acquire(label="late")
        assert gate.snapshot()["closed"] is True and gate.queued == 0


class TestInitializeWatcher:
    @staticmethod
    def _state_holder(initial: str) -> list[str]:
        return [initial]

    @pytest.mark.asyncio
    async def test_ready_settles_success_and_releases(self) -> None:
        gate = adm.SpawnGate(1)
        permit = await gate.acquire(label="p")
        done = asyncio.Event()
        state = self._state_holder("in_flight")
        exited = AsyncMock()
        task = gate.watch_initialize(
            permit, init_done=done, init_state=lambda: state[0], process_exited=exited, timeout=5.0
        )
        state[0] = "ready"
        done.set()
        await task
        assert permit.outcome == adm.OUTCOME_SUCCESS and permit.released
        assert gate.in_flight == 0
        exited.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_failed_settles_failure_and_holds_until_the_process_is_reaped(self) -> None:
        gate = adm.SpawnGate(1)
        permit = await gate.acquire(label="p")
        done = asyncio.Event()
        state = self._state_holder("in_flight")
        reaped = asyncio.Event()

        async def exited() -> None:
            await reaped.wait()

        task = gate.watch_initialize(
            permit, init_done=done, init_state=lambda: state[0], process_exited=exited, timeout=5.0
        )
        state[0] = "failed"
        done.set()
        await _settle()
        assert permit.outcome == adm.OUTCOME_FAILURE
        assert not permit.released, "a failed backend may survive SIGKILL; hold until reaped"
        reaped.set()
        await task
        assert permit.released and gate.in_flight == 0

    @pytest.mark.asyncio
    async def test_no_initialize_at_all_is_neutral(self) -> None:
        gate = adm.SpawnGate(1)
        permit = await gate.acquire(label="p")
        task = gate.watch_initialize(
            permit,
            init_done=asyncio.Event(),
            init_state=lambda: "unsent",
            process_exited=AsyncMock(),
            timeout=0.01,
        )
        await task
        assert permit.outcome == adm.OUTCOME_NEUTRAL and permit.released

    @pytest.mark.asyncio
    async def test_a_handshake_still_in_flight_at_the_deadline_gets_one_more_window(self) -> None:
        gate = adm.SpawnGate(1)
        permit = await gate.acquire(label="p")
        done = asyncio.Event()
        state = self._state_holder("in_flight")
        task = gate.watch_initialize(
            permit,
            init_done=done,
            init_state=lambda: state[0],
            process_exited=AsyncMock(),
            timeout=0.02,
        )
        await asyncio.sleep(0.03)
        assert not task.done(), "in flight at the deadline: the backend's own timer fires next"
        state[0] = "ready"
        done.set()
        await task
        assert permit.outcome == adm.OUTCOME_SUCCESS


class TestAdmissionBundle:
    @pytest.mark.asyncio
    async def test_a_host_charge_follows_the_process_not_the_shutdown(self) -> None:
        budget = hb.HostBudget(hb.HostBudgetLimits(max_procs=2))
        admission = adm.Admission(
            gate=adm.SpawnGate(1),
            budget=budget,
            initialize_timeout_secs=1.0,
            spawn_queue_wait_secs=5.0,
        )
        charge = budget.reserve(label="b")
        exited = asyncio.Event()

        async def wait() -> None:
            await exited.wait()

        admission.track_process(charge, wait)
        await _settle()
        assert budget.procs_in_use == 1, "still charged while the process lives"
        exited.set()
        await _settle()
        assert budget.procs_in_use == 0 and charge.released

    @pytest.mark.asyncio
    async def test_close_drops_every_charge_and_cancels_reapers(self) -> None:
        budget = hb.HostBudget(hb.HostBudgetLimits(max_procs=2))
        admission = adm.Admission(
            gate=adm.SpawnGate(1),
            budget=budget,
            initialize_timeout_secs=1.0,
            spawn_queue_wait_secs=5.0,
        )
        charge = budget.reserve(label="b")
        admission.track_process(charge, asyncio.Event().wait)
        await admission.close()
        assert budget.procs_in_use == 0 and admission.gate.closed
        assert admission.snapshot()["host_budget"]["charges"] == 0


# --- daemon protocol -----------------------------------------------------------

_STUB = "stub-admission-0001"


class _FakeWriter:
    def __init__(self) -> None:
        self.writes: list[bytes] = []

    def write(self, payload: bytes) -> None:
        self.writes.append(payload)

    async def drain(self) -> None:
        return None

    def frames(self) -> list[Any]:
        return [json.loads(p.decode("utf-8")) for p in self.writes]


class _ScriptedReader:
    """Queued frames, then EOF. An ``asyncio.Event`` item blocks the read until
    the event is set, which is how a test holds the socket open while an
    acquire is pending and then hangs up at a chosen moment."""

    def __init__(self, *items: Any) -> None:
        self._items = list(items)

    @property
    def remaining(self) -> int:
        return len(self._items)

    async def readuntil(self, sep: bytes = b"\n") -> bytes:
        while self._items and isinstance(self._items[0], asyncio.Event):
            await self._items[0].wait()
            self._items.pop(0)
        if not self._items:
            raise asyncio.IncompleteReadError(b"", None)
        item = self._items.pop(0)
        if isinstance(item, BaseException):
            raise item
        if isinstance(item, bytes):
            return item
        return json.dumps(item).encode("utf-8") + b"\n"


def _register_frame(**overrides: Any) -> dict[str, Any]:
    frame: dict[str, Any] = {
        "type": "register",
        "stub_uuid": _STUB,
        "poolable": True,
        "server_name": "demo-mcp",
        "agent_name": "adm-agent",
        "command_args_hash": "a" * 8,
        "effective_env_hash": "e" * 8,
        "work_dir": "/tmp/adm",
        "binary_version": "1.0",
        "os_uid": 1000,
        "sandbox_mode": "none",
        "autoapprove_set_hash": "b" * 8,
        "approval_mode": "reads",
        "trust_all_tools": False,
        "config_snapshot_hash": "c" * 8,
        "session_key": "sess-adm",
        "session_type": "dashboard",
        "ancestor_pids": [4242],
    }
    frame.update(overrides)
    return frame


def _fake_pool() -> MagicMock:
    pool = MagicMock()
    pool.unreserve = MagicMock()
    pool.reserve = MagicMock()
    pool.release_exclusive = AsyncMock(return_value=None)
    pool.get = AsyncMock(return_value=None)
    pool.all_backends = MagicMock(return_value=[])
    pool.metrics_snapshot_async = AsyncMock(return_value={"backends": 0})
    return pool


def _resolver(pool_key: PoolKey) -> tuple[str, list[str], dict[str, str], str]:
    return "demo-mcp-server", [], {}, pool_key.work_dir


async def _noop_pump() -> None:
    return None


def _fake_backend() -> Backend:
    proc = MagicMock()
    proc.returncode = None
    proc.pid = 4242
    stdin = MagicMock()
    stdin.write = MagicMock()
    stdin.drain = AsyncMock()
    now = time.monotonic()
    backend = Backend(
        pool_key=PoolKey.from_register(_register_frame()),
        process=proc,
        stdin=stdin,
        stdout=MagicMock(),
        created_at=now,
        last_used_at=now,
    )
    backend.run_stdout_pump = _noop_pump  # type: ignore[method-assign]
    return backend


def _admission(**kw: Any) -> adm.Admission:
    return adm.Admission(
        gate=adm.SpawnGate(kw.pop("capacity", 1)),
        budget=hb.HostBudget(hb.HostBudgetLimits(max_procs=kw.pop("max_procs", 0))),
        initialize_timeout_secs=kw.pop("initialize_timeout_secs", 1.0),
        spawn_queue_wait_secs=kw.pop("spawn_queue_wait_secs", 30.0),
    )


async def _handle(reader: Any, writer: Any, pool: Any, admission: adm.Admission | None) -> None:
    await gw._handle_connection(
        reader,
        writer,
        pool,
        _resolver,
        __import__("pathlib").Path("/tmp/adm.sock"),
        None,
        admission=admission,
    )


@pytest.fixture(autouse=True)
def _isolate_module_globals():
    gw._CONN_INDEX.clear()
    gw._STUB_PROBES.clear()
    yield
    gw._CONN_INDEX.clear()
    gw._STUB_PROBES.clear()


@pytest.fixture
def peer_ok(monkeypatch):
    monkeypatch.setattr(gw.socketsec, "PEER_IDENTITY_SUPPORTED", True)
    monkeypatch.setattr(
        gw.socketsec, "check_peer_is_self", lambda w: gw.socketsec.PeerCredResult.MATCH
    )
    monkeypatch.setattr(gw.socketsec, "get_peer_pid", lambda w: None)
    monkeypatch.setattr(gw, "_audit_pool_fallback", lambda *a: None)
    monkeypatch.setattr(gw, "_audit_pool_rejected", lambda *a: None)


class TestWaitBudgetNegotiation:
    def test_absent_or_malformed_budget_means_an_old_stub(self) -> None:
        admission = _admission(spawn_queue_wait_secs=600.0)
        for frame in (
            {"type": "ensure_backend"},
            {"type": "ensure_backend", "wait_budget_secs": "600"},
            {"type": "ensure_backend", "wait_budget_secs": True},
            {"type": "ensure_backend", "wait_budget_secs": 0},
            {"type": "ensure_backend", "wait_budget_secs": -3},
            {"type": "ensure_backend", "wait_budget_secs": float("inf")},
            {"type": "ensure_backend", "wait_budget_secs": float("nan")},
        ):
            assert gw._negotiated_wait_budget(frame, admission) is None, frame

    def test_the_daemon_budget_caps_the_stub_budget(self) -> None:
        admission = _admission(spawn_queue_wait_secs=100.0)
        assert gw._negotiated_wait_budget({"wait_budget_secs": 600}, admission) == 100.0
        assert gw._negotiated_wait_budget({"wait_budget_secs": 42.5}, admission) == 42.5
        assert gw._negotiated_wait_budget({"wait_budget_secs": 600}, None) is None


class TestRejectionClasses:
    @pytest.mark.parametrize(
        "exc,cls,fallback_new,fallback_legacy",
        [
            (gw._TargetUnknown("no target mapping"), "compat", True, True),
            (PoolAtCapacity("full"), "capacity", False, True),
            (hb.HostBudgetExhausted("procs", 1, 4, 4), "capacity", False, True),
            (adm.SpawnGateTimeout(3, 4, 600.0), "capacity", False, True),
            (adm.SpawnGateClosed("drain"), "capacity", False, True),
            (BackendUnavailable("breaker OPEN"), "capacity", False, True),
            (OSError(12, "ENOMEM"), "capacity", False, True),
            (OSError(2, "ENOENT"), "compat", True, True),
        ],
    )
    def test_each_failure_has_one_class(
        self, exc: BaseException, cls: str, fallback_new: bool, fallback_legacy: bool
    ) -> None:
        new = gw._classify_rejection(exc, exclusive=False, legacy=False)
        old = gw._classify_rejection(exc, exclusive=False, legacy=True)
        assert new is not None and old is not None
        assert (new.cls, new.fallback) == (cls, fallback_new)
        assert (old.cls, old.fallback) == (cls, fallback_legacy)
        if cls == "capacity":
            assert new.retry_after_secs is not None and "retry_after_secs" in new.frame("r")
        else:
            assert "retry_after_secs" not in new.frame("r")

    def test_a_private_target_the_daemon_cannot_launch_is_isolation(self) -> None:
        verdict = gw._classify_rejection(OSError(13, "EACCES"), exclusive=True, legacy=False)
        assert verdict is not None and (verdict.cls, verdict.fallback) == ("isolation", True)

    def test_an_internal_error_has_no_class(self) -> None:
        assert gw._classify_rejection(RuntimeError("bug"), exclusive=False, legacy=False) is None


class TestEnsureBackendWire:
    @pytest.mark.asyncio
    async def test_an_old_stub_keeps_the_legacy_capacity_rejection(
        self, peer_ok, monkeypatch
    ) -> None:
        monkeypatch.setattr(gw, "_acquire_backend", AsyncMock(side_effect=PoolAtCapacity("full")))
        writer = _FakeWriter()
        await _handle(
            _ScriptedReader(_register_frame(), {"type": "ensure_backend"}),
            writer,
            _fake_pool(),
            _admission(),
        )
        frames = writer.frames()
        assert [f["type"] for f in frames] == ["registered", "rejected"]
        assert "spawn_queue" in frames[0]["capabilities"]
        assert frames[1]["fallback"] is True and frames[1]["class"] == "capacity"
        assert not any(f["type"] == "queued" for f in frames)

    @pytest.mark.asyncio
    async def test_a_new_stub_gets_a_classed_capacity_rejection_with_no_fallback(
        self, peer_ok, monkeypatch
    ) -> None:
        monkeypatch.setattr(gw, "_acquire_backend", AsyncMock(side_effect=PoolAtCapacity("full")))
        writer = _FakeWriter()
        await _handle(
            _ScriptedReader(_register_frame(), {"type": "ensure_backend", "wait_budget_secs": 600}),
            writer,
            _fake_pool(),
            _admission(),
        )
        frames = writer.frames()
        assert frames[1]["type"] == "rejected" and frames[1]["class"] == "capacity"
        assert "fallback" not in frames[1] and frames[1]["retry_after_secs"] > 0

    @pytest.mark.asyncio
    async def test_a_queued_new_stub_gets_keepalives_and_pings_are_answered(
        self, peer_ok, monkeypatch
    ) -> None:
        """The wire while a spawn waits: ``queued`` every tick, ``pong`` for every
        ping, a non-control frame parked for after ``ready``."""
        admission = _admission(capacity=1)
        blocker = await admission.gate.acquire(label="blocker")
        monkeypatch.setattr(adm, "QUEUED_KEEPALIVE_SECS", 0.01)
        backend = _fake_backend()
        original = gw._acquire_backend

        async def acquire(*args: Any, **kwargs: Any) -> Any:
            # A real gate wait with the keepalive callback the handler supplied,
            # then a fake backend in place of a fork.
            permit = await admission.gate.acquire(
                label="x",
                deadline=kwargs["wait_deadline"],
                on_queued=kwargs["on_queued"],
                keepalive_secs=0.01,
            )
            permit.release()
            return backend, True

        assert original is not None
        monkeypatch.setattr(gw, "_acquire_backend", acquire)
        release_blocker = asyncio.Event()
        hangup = asyncio.Event()

        async def unblock() -> None:
            await asyncio.sleep(0.05)
            blocker.release()
            release_blocker.set()

        reader = _ScriptedReader(
            _register_frame(),
            {"type": "ensure_backend", "wait_budget_secs": 600},
            {"type": "ping"},
            {"type": "ping"},
            {"type": "unregister"},
            hangup,
        )
        writer = _FakeWriter()
        asyncio.create_task(unblock())

        async def finish() -> None:
            await release_blocker.wait()
            await asyncio.sleep(0.05)
            hangup.set()

        asyncio.create_task(finish())
        await asyncio.wait_for(_handle(reader, writer, _fake_pool(), admission), timeout=5)
        types = [f["type"] for f in writer.frames()]
        assert types[0] == "registered"
        assert types.count("pong") == 2, types
        assert "queued" in types, types
        assert types.index("queued") < types.index("ready"), "keepalives precede ready"
        queued = next(f for f in writer.frames() if f["type"] == "queued")
        assert queued["position"] == 1 and queued["capacity"] == 1
        # The parked ``unregister`` was processed after ``ready`` and ended the
        # connection cleanly rather than being dropped.
        assert types[-1] == "ready"

    @pytest.mark.asyncio
    async def test_a_fallback_is_charged_until_the_stub_hangs_up(
        self, peer_ok, monkeypatch
    ) -> None:
        monkeypatch.setattr(
            gw, "_acquire_backend", AsyncMock(side_effect=gw._TargetUnknown("no target mapping"))
        )
        admission = _admission(max_procs=4)
        hangup = asyncio.Event()
        reader = _ScriptedReader(
            _register_frame(), {"type": "ensure_backend", "wait_budget_secs": 600}, hangup
        )
        writer = _FakeWriter()
        task = asyncio.create_task(_handle(reader, writer, _fake_pool(), admission))
        for _ in range(50):
            await asyncio.sleep(0.005)
            if len(writer.writes) >= 2:
                break
        frames = writer.frames()
        assert frames[1]["type"] == "rejected" and frames[1]["class"] == "compat"
        assert frames[1]["fallback"] is True
        assert admission.budget.snapshot()["by_kind"] == {"fallback": 1}, "the exec is charged"
        assert not task.done(), "the charge holds while the socket is open"
        hangup.set()
        await asyncio.wait_for(task, timeout=5)
        assert admission.budget.procs_in_use == 0

    @pytest.mark.asyncio
    async def test_a_fallback_the_budget_cannot_take_becomes_capacity(
        self, peer_ok, monkeypatch
    ) -> None:
        monkeypatch.setattr(
            gw, "_acquire_backend", AsyncMock(side_effect=gw._TargetUnknown("no target mapping"))
        )
        admission = _admission(max_procs=1)
        admission.budget.reserve(label="occupant")
        writer = _FakeWriter()
        await _handle(
            _ScriptedReader(_register_frame(), {"type": "ensure_backend", "wait_budget_secs": 600}),
            writer,
            _fake_pool(),
            admission,
        )
        frames = writer.frames()
        assert frames[1]["class"] == "capacity" and "fallback" not in frames[1]

    @pytest.mark.asyncio
    async def test_stats_carry_the_admission_snapshot(self, peer_ok) -> None:
        writer = _FakeWriter()
        await _handle(
            _ScriptedReader({"type": "stats"}), writer, _fake_pool(), _admission(capacity=3)
        )
        (frame,) = writer.frames()
        assert frame["admission"]["spawn_gate"]["capacity"] == 3
        assert "host_budget" in frame["admission"]


class TestAcquireBackendOrder:
    """``_acquire_backend`` with a real pool: budget, then permit, then slot."""

    @pytest.mark.asyncio
    async def test_budget_is_charged_before_the_permit_and_released_on_gate_failure(
        self, monkeypatch
    ) -> None:
        from kiro_crew.mcp_gateway.pool import BackendPool

        pool = BackendPool(max_backends=4)
        admission = _admission(capacity=1, max_procs=4)
        blocker = await admission.gate.acquire(label="blocker")
        key = PoolKey.from_register(_register_frame())
        with pytest.raises(adm.SpawnGateTimeout):
            await gw._acquire_backend(
                pool, key, _resolver, admission=admission, wait_deadline=time.monotonic() + 0.02
            )
        assert admission.budget.procs_in_use == 0, "the charge is released with the gate failure"
        assert pool.resident_pending == 0
        assert admission.gate.queued == 0
        blocker.release()

    @pytest.mark.asyncio
    async def test_resident_capacity_is_refused_before_any_fork(self, monkeypatch) -> None:
        from kiro_crew.mcp_gateway.pool import BackendPool

        pool = BackendPool(max_backends=1)
        occupant = _fake_backend()
        await pool.add(PoolKey.from_register(_register_frame(server_name="other")), occupant)
        await occupant.attach_stub("s-occupant")  # attached: not evictable
        spawned = AsyncMock()
        monkeypatch.setattr(gw, "spawn_backend", spawned)
        admission = _admission(capacity=2, max_procs=4)
        with pytest.raises(PoolAtCapacity):
            await gw._acquire_backend(
                pool, PoolKey.from_register(_register_frame()), _resolver, admission=admission
            )
        spawned.assert_not_awaited()
        assert admission.gate.in_flight == 0 and admission.budget.procs_in_use == 0
        assert admission.gate.snapshot()["outcomes"]["neutral"] == 1

    @pytest.mark.asyncio
    async def test_prewarm_settles_neutral_at_once_and_a_stub_spawn_watches_initialize(
        self, monkeypatch
    ) -> None:
        from kiro_crew.mcp_gateway.pool import BackendPool

        pool = BackendPool(max_backends=4)
        admission = _admission(capacity=2, max_procs=4)
        backend = _fake_backend()
        exited = asyncio.Event()
        backend.process.wait = exited.wait

        async def fake_spawn(**kwargs: Any) -> Backend:
            assert kwargs["initialize_timeout_secs"] == admission.initialize_timeout_secs
            return backend

        monkeypatch.setattr(gw, "spawn_backend", fake_spawn)
        monkeypatch.setattr(gw, "_declared_env_to_forward", lambda k: {})
        monkeypatch.setattr(gw, "resolve_secret_uris", lambda env, home: (env, []))
        key = PoolKey.from_register(_register_frame())
        got, was_spawned = await gw._acquire_backend(
            pool, key, _resolver, admission=admission, prewarm=True
        )
        assert got is backend and was_spawned
        await _settle()
        assert admission.gate.in_flight == 0, "prewarm releases its permit at once"
        assert admission.gate.snapshot()["outcomes"]["neutral"] == 1
        assert admission.budget.procs_in_use == 1, "but the process is charged until reaped"
        exited.set()
        await _settle()
        assert admission.budget.procs_in_use == 0

        # A stub spawn on another key holds its permit until initialize resolves.
        second = _fake_backend()
        second.process.wait = asyncio.Event().wait

        async def fake_spawn2(**kwargs: Any) -> Backend:
            return second

        monkeypatch.setattr(gw, "spawn_backend", fake_spawn2)
        await gw._acquire_backend(
            pool,
            PoolKey.from_register(_register_frame(server_name="two")),
            _resolver,
            admission=admission,
        )
        await _settle()
        assert admission.gate.in_flight == 1, "held through the initialize window"
        second._init_state = "ready"
        second._init_done_event.set()
        await _settle()
        assert admission.gate.in_flight == 0
        assert admission.gate.snapshot()["outcomes"]["success"] == 1
        await admission.close()
