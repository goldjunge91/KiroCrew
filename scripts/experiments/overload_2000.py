#!/usr/bin/env python3
"""2000-task overload experiment: real scheduler, fake harness, virtual clock.

What is REAL here: ``SubagentManager`` (admission order, the bounded dispatch
window, fairness lanes, the child reserve, ``yield_slot`` / ``request_resume``
/ ``resume_grant``), the ``taskq`` SQLite store (write-before-ack, fenced
claims, wait records, reconcile-on-boot), ``AdaptiveController`` +
``AdaptivePolicy`` (AIMD over the exec cap and the spawn-gate capacity),
``DependencyCoordinator`` (one retry schedule per scope, probe then ramp by
capacity), ``RecoveryLadder`` (L1..L5 counting and backoff), the gatewayd
``SpawnGate`` and ``HostBudget`` (in-process, the daemon's own classes) and
the ``AcpRuntime`` ``StartCollector`` (a late ``session/new`` adopted, never
requeued).

What is FAKE: the run itself. ``SubagentManager._run`` is replaced by a worker
that walks the same admission seams a real run walks -- cold start through the
gate and the host budget, a provider call, optional nested ``spawn_sub_agents``
children, then a body of virtual seconds -- and settles through
``_claim_finalize`` like the production ``finally`` does. Time is a
discrete-event virtual clock injected into every component that accepts one, so
2000 tasks finish in seconds of wall time and every wait is deterministic.

Fault knobs (``Faults``): injected start timeouts for the controller path
(10 -> 6 -> 4 -> recover), a gateway restart mid-run (store closed, manager
dropped, a fresh manager reconciles; the real ``StartCollector`` adopts a late
start), a long gatewayd outage (the gate unreachable for a virtual 20 min;
runs park on the ``mcp_gateway:capacity`` scope through the L1 ladder and no
session is severed), a provider 429 storm on one scope from every session (one
coordinated schedule), and a parent tree S -> A -> B at cap 2.

Metrics (SPEC §五): accepted / completed / failed / cancelled /
unknown_side_effect / remaining, queue-wait p50 / p95 / max, max in-flight,
the effective-cap timeline, HostBudget peaks, recovery attempts by ladder
layer, restarts, fairness (per-lane first-grant index and max starvation),
duplicate dispatches (must be 0) and lost tasks (must be 0).

Usage::

    KIROCREW_HOME=$KIROCREW_SCRATCH/overload-home PYTHONPATH=src \\
        python scripts/experiments/overload_2000.py --n 2000 --out $KIROCREW_SCRATCH/overload

The driver REFUSES to run against the default ``~/.kiro/crew`` data home (RFC
§11): it writes a task store and 2000 subagent folders, so it picks a fresh
temporary home when ``KIROCREW_HOME`` is unset.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import heapq
import json
import os
import random
import statistics
import sys
import tempfile
import time
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Optional
from unittest.mock import AsyncMock, MagicMock, patch

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SRC = _REPO_ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

#: Scenario names, in the order the report lists them.
SCENARIOS = ("baseline", "controller", "controller_raw", "restart", "outage", "throttle", "tree")

#: The shared scope every gatewayd capacity refusal parks on (X1's run loop).
GATEWAY_SCOPE = "mcp_gateway:capacity"
#: The provider scope the 429 storm hits.
PROVIDER_SCOPE = "provider:bedrock"

_DEFAULT_HOME = Path.home() / ".kiro" / "crew"


def ensure_experiment_home() -> Path:
    """Point ``KIROCREW_HOME`` at a scratch directory, never the real data home.

    Also pins ``KIROCREW_PROFILE=standalone`` (as the test suite's rootdir
    conftest does): an inherited enterprise profile makes the governance hook
    fail closed with no companion installed, which is the operator's shell
    state, not this experiment's input.
    """
    os.environ.setdefault("KIROCREW_PROFILE", "standalone")
    if os.environ.get("KIROCREW_PROFILE") != "standalone":
        os.environ["KIROCREW_PROFILE"] = "standalone"
    env = os.environ.get("KIROCREW_HOME")
    if env and Path(env).expanduser().resolve() != _DEFAULT_HOME.resolve():
        return Path(env)
    base = os.environ.get("KIROCREW_SCRATCH") or tempfile.gettempdir()
    home = Path(tempfile.mkdtemp(prefix="overload-home-", dir=base))
    os.environ["KIROCREW_HOME"] = str(home)
    return home


# ── virtual clock ──────────────────────────────────────────────────────────────


class VirtualClock:
    """Discrete-event clock. Callbacks may be sync or return an awaitable."""

    def __init__(self, start: float = 1_700_000_000.0) -> None:
        self.t = float(start)
        self._heap: list[tuple[float, int, Callable[[], Any]]] = []
        self._seq = 0
        self.events_fired = 0

    def now(self) -> float:
        return self.t

    def at(self, when: float, cb: Callable[[], Any]) -> None:
        self._seq += 1
        heapq.heappush(self._heap, (max(float(when), self.t), self._seq, cb))

    def after(self, delay: float, cb: Callable[[], Any]) -> None:
        self.at(self.t + max(0.0, float(delay)), cb)

    def next_time(self) -> Optional[float]:
        return self._heap[0][0] if self._heap else None

    def pending(self) -> int:
        return len(self._heap)

    async def sleep(self, delay: float) -> None:
        """Await ``delay`` virtual seconds."""
        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()

        def _wake() -> None:
            if not fut.done():
                fut.set_result(None)

        self.after(delay, _wake)
        await fut

    async def fire_next(self, *, until: Optional[float] = None) -> bool:
        """Advance to and run the earliest event. False when none (or past ``until``)."""
        if not self._heap:
            return False
        when, _seq, cb = self._heap[0]
        if until is not None and when > until:
            return False
        heapq.heappop(self._heap)
        self.t = max(self.t, when)
        self.events_fired += 1
        out = cb()
        if asyncio.iscoroutine(out) or isinstance(out, asyncio.Future):
            await out
        return True


async def settle_loop(spins: int = 6) -> None:
    """Let every ready asyncio task make progress (no wall-clock sleep)."""
    for _ in range(spins):
        await asyncio.sleep(0)


# ── faults / metrics ───────────────────────────────────────────────────────────


@dataclass
class Faults:
    """Every knob is off by default; a scenario turns on the ones it studies."""

    #: Runs STARTED inside ``[start, end)`` virtual seconds after t0 time out
    #: with probability ``fraction`` (the controller path).
    timeout_window: Optional[tuple[float, float, float]] = None
    #: Crash the gateway once this many runs have completed.
    restart_after_completed: Optional[int] = None
    #: The gate is unreachable for ``[start, start + duration)`` seconds after t0.
    outage: Optional[tuple[float, float]] = None
    #: Provider 429 storm on ``PROVIDER_SCOPE`` for ``[start, end)`` seconds after t0.
    throttle_window: Optional[tuple[float, float]] = None
    #: Every ``tree_every``-th root spawns a chain of ``tree_depth`` children.
    tree_every: int = 0
    tree_depth: int = 2
    #: Attach the adaptive controller (exec cap starts at ``adaptive_initial``).
    controller: bool = False
    adaptive_initial: int = 4
    #: Present the gate's ``outcomes.failure`` counter to the controller as a
    #: 60 s WINDOW instead of the daemon-lifetime total the snapshot carries.
    #: ``False`` reproduces defect D1 (two lifetime init failures pin the cap).
    gate_stats_windowed: bool = True
    #: Feed the L1 ladder's per-RUN delay to the shared scope as ``retry_at``
    #: (what ``run.py`` does today). ``True`` reproduces defect D2: every woken
    #: probe is a different run at L1 attempt 1, so the SCOPE retries every
    #: ~2 s and burns its 20-attempt cap in about a minute of outage.
    infra_retry_at_from_ladder: bool = False
    #: Also run the real ``StartCollector`` late-adoption probe in the restart scenario.
    start_collector_probe: bool = True


@dataclass
class Metrics:
    scenario: str = ""
    n: int = 0
    user_max: int = 10
    accepted: int = 0
    completed: int = 0
    failed: int = 0
    cancelled: int = 0
    unknown_side_effect: int = 0
    remaining: int = 0
    lost: int = 0
    duplicate_dispatches: int = 0
    starts: int = 0
    queue_wait_p50_s: float = 0.0
    queue_wait_p95_s: float = 0.0
    queue_wait_max_s: float = 0.0
    max_in_flight: int = 0
    peak_window: int = 0
    effective_cap_timeline: list[dict[str, Any]] = field(default_factory=list)
    effective_cap_min: int = 0
    controller_counts: dict[str, int] = field(default_factory=dict)
    host_budget_peaks: dict[str, int] = field(default_factory=dict)
    gate: dict[str, Any] = field(default_factory=dict)
    recovery_attempts_by_layer: dict[str, int] = field(default_factory=dict)
    restarts: int = 0
    escalations_notified: int = 0
    waits_entered: int = 0
    waits_by_state: dict[str, int] = field(default_factory=dict)
    resumes_granted: int = 0
    coordinator_scopes_seen: int = 0
    coordinator_scopes: list[str] = field(default_factory=list)
    coordinator_reports: int = 0
    coordinator_wakes: int = 0
    coordinator_max_scopes_live: int = 0
    throttle_reports_by_session: dict[str, int] = field(default_factory=dict)
    outage_recovery_secs: Optional[float] = None
    infra_refusals: int = 0
    scope_attempts_peak: int = 0
    throttle_recovery_secs: Optional[float] = None
    restart: dict[str, Any] = field(default_factory=dict)
    start_collector: dict[str, Any] = field(default_factory=dict)
    fairness_first_grant_index: dict[str, int] = field(default_factory=dict)
    fairness_max_starvation: dict[str, int] = field(default_factory=dict)
    per_lane_accepted: dict[str, int] = field(default_factory=dict)
    tree: dict[str, Any] = field(default_factory=dict)
    virtual_secs: float = 0.0
    wall_secs: float = 0.0
    virtual_events: int = 0
    manual_interventions: int = 0
    invariants: dict[str, bool] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


# ── the harness ────────────────────────────────────────────────────────────────


def _fake_sessions() -> MagicMock:
    sessions = MagicMock()
    sessions.get_pid = MagicMock(return_value=None)
    sessions.get_approval_policy = MagicMock(return_value="auto")
    sessions.get_agent = MagicMock(return_value="")
    sessions.has_session = MagicMock(return_value=True)
    sessions.release = MagicMock()
    sessions.reset = AsyncMock()
    return sessions


def _fake_ctx() -> MagicMock:
    ctx = MagicMock()
    ctx.hooks.auto_approve_subagent_spawn = True
    return ctx


class _RunCrashed(Exception):
    """Raised inside a fake run when the gateway 'dies' under it."""


class Harness:
    """One scenario's fixtures: the real components, the fake worker, the ledger."""

    def __init__(
        self,
        *,
        scenario: str,
        n: int,
        faults: Faults,
        user_max: int = 10,
        seed: int = 7,
        run_secs: float = 5.0,
        init_secs: float = 1.0,
        sessions: int = 3,
        system_share: float = 0.04,
    ) -> None:
        self.scenario = scenario
        self.n = int(n)
        self.faults = faults
        self.user_max = int(user_max)
        self.rng = random.Random(seed)
        self.run_secs = float(run_secs)
        self.init_secs = float(init_secs)
        self.session_keys = [f"dashboard:s{i + 1}" for i in range(sessions)]
        self.system_key = "cron:nightly"
        self.system_share = float(system_share)
        self.clock = VirtualClock()
        self.t0 = self.clock.now()
        self.metrics = Metrics(scenario=scenario, n=self.n, user_max=self.user_max)
        # ledgers
        self.accepted_at: dict[str, float] = {}
        self.lane_of: dict[str, str] = {}
        self.start_log: list[tuple[float, str, str]] = []
        self.starts_per_id: Counter[str] = Counter()
        self.queue_waits: list[float] = []
        self.cap_timeline: list[dict[str, Any]] = []
        self.peak_procs = 0
        self.peak_fds = 0
        self.peak_rss = 0
        self.peak_window = 0
        self.max_in_flight = 0
        self.charges: dict[str, Any] = {}
        self.completed_ids: set[str] = set()
        self.tree_roots: set[str] = set()
        self.tree_children_of: dict[str, list[str]] = defaultdict(list)
        self.tree_depth_of: dict[str, int] = {}
        self.throttle_reports: Counter[str] = Counter()
        self.coordinator_reports = 0
        self.coordinator_wakes = 0
        self.max_scopes_live = 0
        self.scope_attempts_peak = 0
        self.scopes_seen: set[str] = set()
        self.waits_by_state: Counter[str] = Counter()
        self.resumes = 0
        self.ladder_attempts: Counter[str] = Counter()
        self.restarts = 0
        self.notified = 0
        self.infra_refusals = 0
        self.gate_reachable = True
        self.outage_ended_at: Optional[float] = None
        self.outage_recovered_at: Optional[float] = None
        self.throttle_ended_at: Optional[float] = None
        self.throttle_recovered_at: Optional[float] = None
        self.crashed = False
        self.restart_info: dict[str, Any] = {}
        self._patches: list[Any] = []
        self._run_tasks: set[asyncio.Task[Any]] = set()
        self._live_infos: dict[str, Any] = {}
        self.wall_start = time.monotonic()
        self._controller_period = 5.0
        self._window_starts = 0

    # -- construction --------------------------------------------------------

    def _build(self) -> None:
        import kiro_crew.resource_status as resource_status
        import kiro_crew.subagent as subagent_mod
        from kiro_crew.mcp_gateway.admission import SpawnGate
        from kiro_crew.mcp_gateway.host_budget import HostBudget, HostBudgetLimits
        from kiro_crew.recovery.ladder import RecoveryLadder
        from kiro_crew.taskq.dependency import DependencyCoordinator

        # Pin the host-memory readings spawn() consults (same seam as the test
        # fixture ``healthy_host_memory``): the verdict is the scenario's, not
        # the machine's.
        self._patches.append(
            patch.object(
                subagent_mod,
                "check_memory_available",
                lambda min_gb=None, path=None: (True, 8.0),
            )
        )
        self._patches.append(
            patch.object(
                subagent_mod,
                "cached_admission_check",
                lambda: resource_status.AdmissionDecision(
                    admitted=True, posture=resource_status.POSTURE_AMPLE, available_gb=8.0
                ),
            )
        )
        self._patches.append(patch.object(subagent_mod, "Stats"))
        self._patches.append(patch.object(subagent_mod, "sel"))
        for p in self._patches:
            p.start()

        self.gate = SpawnGate(capacity=4, floor=1, ceiling=8, clock=self.clock.now)
        self.budget = HostBudget(
            HostBudgetLimits(max_procs=max(16, self.user_max + 6), max_rss_mb=0, max_fds=0)
        )
        self.ladder = RecoveryLadder(
            clock=self.clock.now, rng=random.Random(self.rng.random()), notifier=self._notify
        )
        self.mgr = self._new_manager()
        self.coordinator = DependencyCoordinator(
            self.mgr._taskq,
            clock=self.clock.now,
            rng=random.Random(self.rng.random()),
            capacity=lambda: max(1, int(self.mgr._max_concurrent)),
            on_wake=self._on_coordinator_wake,
            wake_through=self._wake_through,
            on_fail=self._on_coordinator_fail,
        )
        self.controller = None
        if self.faults.controller:
            self.controller = self._new_controller()

    def _new_manager(self) -> Any:
        from kiro_crew.subagent import SubagentManager

        mgr = SubagentManager(
            sessions=_fake_sessions(), ctx_builder=_fake_ctx(), max_concurrent=self.user_max
        )
        mgr._spawn_stagger_secs = 0.0
        mgr._last_spawn_ts = 0.0
        if mgr._taskq is None:
            raise RuntimeError("the durable task queue did not open; the experiment needs it")
        mgr._taskq._clock = self.clock.now
        return mgr

    def _new_controller(self) -> Any:
        from kiro_crew.adaptive.controller import AdaptiveController, HostSample

        cfg = SimpleNamespace(
            agent=SimpleNamespace(
                adaptive_concurrency=True,
                adaptive_concurrency_mode="aimd",
                adaptive_initial=self.faults.adaptive_initial,
                adaptive_floor=1,
                controller_sample_secs=self._controller_period,
                resource_pressure_gb=4.0,
                resource_critical_gb=2.0,
            )
        )

        async def _set_gate(n: int) -> Optional[int]:
            return self.gate.set_capacity(n)

        history: list[tuple[float, int]] = []

        async def _stats() -> dict[str, Any]:
            snap = self.gate.snapshot()
            if self.faults.gate_stats_windowed:
                now = self.clock.now()
                cum = int(snap["outcomes"].get("failure", 0))
                history.append((now, cum))
                while len(history) > 1 and history[0][0] < now - 60.0:
                    history.pop(0)
                snap = dict(snap)
                snap["outcomes"] = dict(snap["outcomes"])
                snap["outcomes"]["failure"] = cum - history[0][1]
            return {"admission": {"spawn_gate": snap, "host_budget": self.budget.snapshot()}}

        return AdaptiveController(
            self.mgr,
            cfg=cfg,
            set_gate_capacity=_set_gate,
            read_gate_stats=_stats,
            host_probe=lambda: HostSample(
                free_mem_mb=8192.0, rss_mb=300.0, fd_count=64, fd_limit=8192
            ),
            clock=self.clock.now,
            sleep=self.clock.sleep,
            gate_initial=4,
            gate_floor=1,
            gate_ceiling=8,
        )

    def close(self) -> None:
        with contextlib.suppress(Exception):
            self.mgr._taskq.close()
        for p in reversed(self._patches):
            with contextlib.suppress(Exception):
                p.stop()
        self._patches.clear()

    # -- hooks -----------------------------------------------------------------

    def _notify(self, layer: str, message: str) -> None:
        self.notified += 1

    def _wake_through(self, task_id: str, generation: Optional[int]) -> bool:
        info = self.mgr._agents.get(task_id)
        if info is None or info.done or not info._slot_released:
            return False
        self.coordinator_wakes += 1
        return bool(
            self.mgr._admission.request_resume(info, reason=f"dependency wake gen={generation}")
        )

    def _on_coordinator_wake(self, task_id: str) -> None:
        # A parked ROW (no live run) became eligible: pump the window.
        if task_id not in self.mgr._agents:
            self.mgr._drain_queue()

    def _on_coordinator_fail(self, task_id: str, reason: str) -> None:
        info = self.mgr._agents.get(task_id)
        if info is None or info.done:
            return
        info.error = info.error or f"dependency gave up: {reason}"
        ev = getattr(info, "_resume_event", None)
        if isinstance(ev, asyncio.Event):
            ev.set()

    # -- submission ------------------------------------------------------------

    def lane_for(self, i: int) -> str:
        """Session keys are submitted in BLOCKS (all of s1, then all of s2, ...)
        so FIFO would starve every later session; the system lane is last."""
        system_n = int(self.n * self.system_share)
        per_session = (self.n - system_n) // len(self.session_keys)
        if i >= per_session * len(self.session_keys):
            return self.system_key
        return self.session_keys[i // per_session]

    def submit_all(self) -> list[str]:
        ids: list[str] = []
        for i in range(self.n):
            key = self.lane_for(i)
            is_tree_root = self.faults.tree_every > 0 and i % self.faults.tree_every == 0
            info = self.mgr.spawn(f"task {i}", parent_session_key=key)
            if info is None or (info.done and info.error):
                raise RuntimeError(f"spawn {i} refused: {getattr(info, 'error', 'no info')}")
            self._accepted(info.id, key)
            if is_tree_root:
                self.tree_roots.add(info.id)
                self.tree_depth_of[info.id] = self.faults.tree_depth
            ids.append(info.id)
        self.metrics.accepted = len(ids)
        return ids

    def _accepted(self, task_id: str, session_key: str) -> None:
        from kiro_crew.taskq import lanes

        self.accepted_at[task_id] = self.clock.now()
        self.lane_of[task_id] = lanes.lane_key_for(session_key)

    # -- the fake worker ---------------------------------------------------------

    def _elapsed(self) -> float:
        return self.clock.now() - self.t0

    def _in_window(self, window: Optional[tuple[float, ...]]) -> bool:
        if window is None:
            return False
        e = self._elapsed()
        return window[0] <= e < window[1]

    def _outage_active(self) -> bool:
        if self.faults.outage is None:
            return False
        start, dur = self.faults.outage
        e = self._elapsed()
        return start <= e < start + dur

    async def run(self, info: Any) -> None:
        """The patched ``SubagentManager._run``. Mirrors the production run's
        admission seams; every settle goes through ``_claim_finalize``."""
        mgr = self.mgr
        from kiro_crew.taskq import model

        self.starts_per_id[info.id] += 1
        self.start_log.append((self.clock.now(), info.id, self.lane_of.get(info.id, "?")))
        if info.id in self.accepted_at and self.starts_per_id[info.id] == 1:
            self.queue_waits.append(self.clock.now() - self.accepted_at[info.id])
        self.max_in_flight = max(self.max_in_flight, mgr._running_count)
        self.peak_window = max(self.peak_window, len(mgr._queue))
        self._live_infos[info.id] = info
        mgr._admission.taskq_mark(info, model.RUNNING)
        timed_out = False
        try:
            timed_out = await self._cold_start(info)
            if info.error:
                return
            if not timed_out:
                await self._provider_call(info)
                if info.error:
                    return
                if info.id in self.tree_depth_of and self.tree_depth_of[info.id] > 0:
                    await self._spawn_children_and_wait(info)
                    if info.error:
                        return
                await self.clock.sleep(self.run_secs * self.rng.uniform(0.6, 1.4))
            else:
                await self.clock.sleep(35.0)
                info.error = "startup timed out: session/new exceeded its budget"
        except asyncio.CancelledError:
            # The gateway died under this run: no settle, no release -- the
            # next boot's reconcile is what settles the row.
            raise
        finally:
            if not self.crashed:
                self._finish(info)

    def _finish(self, info: Any) -> None:
        mgr = self.mgr
        info.done = True
        if not info.error:
            info.result = "ok"
        self._live_infos.pop(info.id, None)
        charge = self.charges.pop(info.id, None)
        if charge is not None:
            charge.release()
        mgr._claim_finalize(info)
        self.coordinator.forget(info.id)
        if not info.error:
            self.completed_ids.add(info.id)
        if info._slot_released and not info._resume_pending:
            # A run that ended while yielded holds no slot: nothing to return.
            mgr._drain_queue()
            return
        if mgr._release_slot(info):
            mgr._running_count -= 1
            mgr._drain_queue()

    def _injected_timeout(self) -> bool:
        """Deterministic: inside the window, ``fraction`` of the starts (by
        start index, evenly spaced) time out -- 0.4 means 2 of every 5."""
        if not self._in_window(self.faults.timeout_window):
            return False
        fraction = float(self.faults.timeout_window[2])  # type: ignore[index]
        self._window_starts += 1
        period = 5
        hits = max(0, min(period, round(fraction * period)))
        return (self._window_starts % period) < hits

    async def _cold_start(self, info: Any) -> bool:
        """Gate permit -> host charge -> initialize. Returns True when the start
        is an injected timeout (the run then stalls and ends attributable)."""
        from kiro_crew.mcp_gateway.admission import (
            OUTCOME_FAILURE,
            OUTCOME_SUCCESS,
            SpawnGateClosed,
        )
        from kiro_crew.mcp_gateway.host_budget import HostBudgetExhausted

        while True:
            if not self.gate_reachable:
                ok = await self._infra_refusal(info)
                if not ok:
                    return False
                continue
            t_gate = self.clock.now()
            try:
                permit = await self.gate.acquire(label=info.id)
            except SpawnGateClosed:
                ok = await self._infra_refusal(info)
                if not ok:
                    return False
                continue
            try:
                charge = self.budget.reserve(label=info.id, kind="pooled")
            except HostBudgetExhausted:
                permit.settle(OUTCOME_FAILURE)
                permit.release()
                ok = await self._infra_refusal(info)
                if not ok:
                    return False
                continue
            self.charges[info.id] = charge
            snap = self.budget.snapshot()
            self.peak_procs = max(self.peak_procs, int(snap["procs"]))
            self.peak_fds = max(self.peak_fds, int(snap["fds"]))
            self.peak_rss = max(self.peak_rss, int(snap["rss_mb"]))
            timed_out = self._injected_timeout()
            if timed_out:
                info.stalled = True
                await self.clock.sleep(30.0)
                permit.settle(OUTCOME_FAILURE)
                permit.release()
                if self.controller is not None:
                    self.controller.record_start(
                        (self.clock.now() - t_gate) * 1000.0,
                        ok=False,
                        attributable_timeout=True,
                        key="acp:session/new",
                    )
                return True
            await self.clock.sleep(self.init_secs)
            permit.settle(OUTCOME_SUCCESS)
            permit.release()
            if self.controller is not None:
                self.controller.record_start(
                    (self.clock.now() - t_gate) * 1000.0, ok=True, key="acp:session/new"
                )
            return False

    async def _infra_refusal(self, info: Any) -> bool:
        """A ``-32001 capacity`` refusal: L1 ladder decides the delay, the run
        parks on the shared gateway scope, and the ladder climbs while the
        daemon stays down. Returns False when the wait failed the run."""
        from kiro_crew.recovery import ladder as L
        from kiro_crew.taskq.dependency import KIND_DEPENDENCY_UNAVAILABLE, DependencySignal

        decision = self.ladder.observe_failure(
            L.L1_TOOL_CALL, f"subagent:{info.id}", reason="gatewayd unreachable (-32001 capacity)"
        )
        self.ladder_attempts[L.L1_TOOL_CALL] += 1
        delay = decision.delay_secs
        layer = (
            decision.next_layer if decision.action in (L.ACTION_ESCALATE, L.ACTION_NOTIFY) else None
        )
        while layer is not None and self.ladder.layer_policy(layer).automatic:
            up = self.ladder.observe_failure(layer, "gatewayd", reason="daemon down")
            self.ladder_attempts[layer] += 1
            if layer == L.L4_GATEWAYD:
                self.ladder.record_restart(L.L4_GATEWAYD)
                self.restarts += 1
            if up.retry:
                delay = max(delay, up.delay_secs)
                break
            layer = up.next_layer
        # A ladder RETRY names the delay (honoured exactly by the scope); an
        # escalation without one leaves ``retry_at`` unset so the coordinator
        # applies its own capped exponential backoff to the shared scope.
        signal = DependencySignal(
            kind=KIND_DEPENDENCY_UNAVAILABLE,
            dependency_scope=GATEWAY_SCOPE,
            source="stub",
            retry_at=(
                (self.clock.now() + delay)
                if delay > 0 and self.faults.infra_retry_at_from_ladder
                else None
            ),
            detail="spawn gate unreachable",
        )
        self.infra_refusals += 1
        return await self._park(info, signal)

    async def _provider_call(self, info: Any) -> None:
        from kiro_crew.taskq.dependency import KIND_RATE_LIMITED, DependencySignal

        while self._in_window(self.faults.throttle_window):
            session = str(info.parent_session_key or "")
            self.throttle_reports[session] += 1
            if self.controller is not None:
                self.controller.record_provider_throttle(PROVIDER_SCOPE)
            signal = DependencySignal(
                kind=KIND_RATE_LIMITED,
                dependency_scope=PROVIDER_SCOPE,
                source="acp",
                detail="HTTP 429",
            )
            if not await self._park(info, signal):
                return
        if self.throttle_ended_at is not None and self.throttle_recovered_at is None:
            self.throttle_recovered_at = self.clock.now()

    async def _park(self, info: Any, signal: Any) -> bool:
        """``coordinator.report`` + ``yield_slot`` + await the grant (X1's path)."""
        from kiro_crew.taskq import WaitRecord
        from kiro_crew.taskq.waits import EVIDENCE_DEPENDENCY_ADAPTER

        mgr = self.mgr
        event = asyncio.Event()
        info._resume_event = event
        self.coordinator_reports += 1
        self.scopes_seen.add(signal.dependency_scope)
        verdict = self.coordinator.report(
            info.id, signal, generation=info._taskq_generation or None
        )
        self.max_scopes_live = max(self.max_scopes_live, len(self.coordinator.scopes()))
        self.scope_attempts_peak = max(
            self.scope_attempts_peak, int(getattr(verdict, "attempts", 0) or 0)
        )
        if verdict.outcome != "wait":
            info.error = info.error or f"dependency terminal: {verdict.reason}"
            return False
        record = WaitRecord.dependency(
            signal.dependency_scope,
            since=self.clock.now(),
            retry_at=verdict.retry_at,
            source=EVIDENCE_DEPENDENCY_ADAPTER,
        )
        if mgr._admission.yield_slot(info, record, persist=False):
            self.waits_by_state[record.state] += 1
            self.metrics.waits_entered += 1
        await event.wait()
        if info.error:
            return False
        self.resumes += 1
        if (
            signal.dependency_scope == GATEWAY_SCOPE
            and self.outage_ended_at is not None
            and self.outage_recovered_at is None
        ):
            self.outage_recovered_at = self.clock.now()
        return True

    async def _spawn_children_and_wait(self, info: Any) -> None:
        """A blocking ``spawn_sub_agents``: the execution layer's in-flight tool
        name is what lets admission put the parent in ``waiting_children``."""
        mgr = self.mgr
        depth = self.tree_depth_of.get(info.id, 0)
        info._inflight_tool = SimpleNamespace(tool_name="spawn_sub_agents", title=f"tc-{info.id}")
        event = asyncio.Event()
        info._resume_event = event
        child = mgr.spawn(f"{info.task}/child", parent_session_key=f"subagent:{info.id}")
        if child is None or (child.done and child.error):
            info.error = f"child spawn refused: {getattr(child, 'error', 'none')}"
            return
        self._accepted(child.id, f"subagent:{info.id}")
        self.metrics.accepted += 1
        self.tree_children_of[info.id].append(child.id)
        if depth - 1 > 0:
            self.tree_depth_of[child.id] = depth - 1
        if not info._slot_released:
            # admission did not yield the parent (no store / no tool name):
            # the experiment treats that as a defect worth surfacing.
            info.error = "parent kept its slot while blocked in spawn_sub_agents"
            return
        self.waits_by_state["waiting_children"] += 1
        self.metrics.waits_entered += 1
        await event.wait()
        info._inflight_tool = None
        if not info.error:
            self.resumes += 1

    # -- the loop ------------------------------------------------------------

    def _run_patch(self) -> Any:
        from kiro_crew.subagent import SubagentManager

        harness = self

        async def _run(_mgr_self: Any, info: Any) -> None:
            await harness.run(info)

        return patch.object(SubagentManager, "_run", new=_run)

    def _record_cap(self, force: bool = False) -> None:
        cap = int(self.mgr._max_concurrent)
        gate = int(self.gate.capacity)
        if (
            force
            or not self.cap_timeline
            or self.cap_timeline[-1]["exec_cap"] != cap
            or (self.cap_timeline[-1]["gate_cap"] != gate)
        ):
            self.cap_timeline.append(
                {
                    "t": round(self._elapsed(), 1),
                    "exec_cap": cap,
                    "gate_cap": gate,
                    "running": int(self.mgr._running_count),
                    "queued_window": len(self.mgr._queue),
                    "procs": int(self.budget.snapshot()["procs"]),
                    "reason": self._last_decision_reason(),
                }
            )

    def _last_decision_reason(self) -> str:
        if self.controller is None:
            return ""
        last = self.controller.policy.last_decision
        return f"{last.action}: {last.reason}" if last is not None else ""

    async def _controller_tick(self) -> None:
        if self.controller is None or self.crashed:
            return
        await self.controller.tick(loop_lag_ms=0.0)
        self._record_cap()
        if self._live_infos or not self._all_settled():
            self.clock.after(self._controller_period, self._controller_tick)

    def _schedule_faults(self) -> None:
        if self.faults.outage is not None:
            start, dur = self.faults.outage

            def _down() -> None:
                self.gate_reachable = False
                self._record_cap(force=True)

            def _up() -> None:
                from kiro_crew.recovery import ladder as L

                self.gate_reachable = True
                self.outage_ended_at = self.clock.now()
                # The daemon registered again: the scope recovers NOW (staged
                # wake), the ladder counts the outage as over at every layer.
                self.coordinator.recovered(GATEWAY_SCOPE)
                for layer in (L.L2_BACKEND, L.L3_ACP_RUNTIME, L.L4_GATEWAYD):
                    self.ladder.observe_success(layer, "gatewayd")
                self._record_cap(force=True)

            self.clock.at(self.t0 + start, _down)
            self.clock.at(self.t0 + start + dur, _up)
        if self.faults.throttle_window is not None:
            _s, end = self.faults.throttle_window

            def _storm_over() -> None:
                self.throttle_ended_at = self.clock.now()
                # A provider limit lifting is not announced: the scope's own
                # probe discovers it at retry_at. Nothing to call here.

            self.clock.at(self.t0 + end, _storm_over)

    def _all_settled(self) -> bool:
        from kiro_crew.taskq import model

        by_state = self.mgr._taskq.count_by_state()
        live = {s: c for s, c in by_state.items() if s not in model.TERMINAL}
        return not live

    async def drive(self, *, max_virtual_secs: float = 6 * 3600.0) -> None:
        """Run the scenario to completion under the virtual clock."""
        await asyncio.to_thread(self._build)
        with self._run_patch():
            self._schedule_faults()
            if self.controller is not None:
                self.clock.after(self._controller_period, self._controller_tick)
            self.submit_all()
            self._record_cap(force=True)
            await settle_loop()
            self.mgr._drain_queue()
            await settle_loop()
            deadline = self.t0 + max_virtual_secs
            idle_rounds = 0
            while self.clock.now() < deadline:
                if self._maybe_crash():
                    await self._restart_gateway()
                    continue
                next_evt = self.clock.next_time()
                next_dep = self.coordinator.next_deadline()
                if next_dep is not None and (next_evt is None or next_dep <= next_evt):
                    self.clock.t = max(self.clock.t, next_dep)
                    woken = self.coordinator.tick()
                    await settle_loop()
                    self.mgr._drain_queue()
                    await settle_loop()
                    idle_rounds = 0
                    after = self.coordinator.next_deadline()
                    if woken or after is None or after > self.clock.t:
                        continue
                    # A scope holding only in-flight probes keeps a due
                    # deadline (production re-arms its pump at a 50 ms floor);
                    # let the runs' own timers fire instead of spinning here.
                    if next_evt is None:
                        if self._all_settled() and not self._live_infos:
                            break
                        idle_rounds += 1
                        if idle_rounds > 3:
                            break
                        continue
                fired = await self.clock.fire_next()
                await settle_loop()
                if not fired:
                    # No timer, no dependency deadline: either done, or the
                    # pump has work it can start now.
                    self.mgr._drain_queue()
                    await settle_loop()
                    if self._all_settled() and not self._live_infos:
                        break
                    idle_rounds += 1
                    if idle_rounds > 3:
                        break
                    continue
                idle_rounds = 0
                self.max_in_flight = max(self.max_in_flight, self.mgr._running_count)
            self._finalize()

    def _maybe_crash(self) -> bool:
        n = self.faults.restart_after_completed
        return (
            n is not None
            and not self.crashed
            and self.restarts == 0
            and len(self.completed_ids) >= n
        )

    async def _restart_gateway(self) -> None:
        """The gateway process dies: in-flight runs vanish without settling,
        the store is closed unflushed, a fresh manager reconciles on boot."""
        from kiro_crew.mcp_gateway.admission import SpawnGate
        from kiro_crew.taskq import model
        from kiro_crew.taskq.dependency import DependencyCoordinator

        self.crashed = True
        store = self.mgr._taskq
        before = store.count_by_state()
        live_before = {k: v for k, v in before.items() if k not in model.TERMINAL}
        in_flight_ids = list(self._live_infos)
        # kill every run coroutine (a dead process settles nothing)
        for task in list(self.mgr._tasks.values()):
            if isinstance(task, asyncio.Task) and not task.done():
                task.cancel()
        await settle_loop()
        for task in list(self.mgr._tasks.values()):
            if isinstance(task, asyncio.Task):
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
        self.budget.release_all()
        with contextlib.suppress(Exception):
            await self.gate.close()
        store.close()
        old_incarnation = store.incarnation
        # drop the dead process's virtual timers (its runs are gone)
        self.clock._heap.clear()
        self._live_infos.clear()
        self.charges.clear()
        t_down = self.clock.now()
        # the new process comes up 3 virtual seconds later
        self.clock.t += 3.0
        self.gate = SpawnGate(capacity=4, floor=1, ceiling=8, clock=self.clock.now)
        self.mgr = await asyncio.to_thread(self._new_manager)
        self.coordinator = DependencyCoordinator(
            self.mgr._taskq,
            clock=self.clock.now,
            rng=random.Random(self.rng.random()),
            capacity=lambda: max(1, int(self.mgr._max_concurrent)),
            on_wake=self._on_coordinator_wake,
            wake_through=self._wake_through,
            on_fail=self._on_coordinator_fail,
        )
        self.coordinator.rebuild()
        if self.controller is not None:
            self.controller = self._new_controller()
            self.clock.after(self._controller_period, self._controller_tick)
        self.restarts += 1
        after = self.mgr._taskq.count_by_state()
        redispatched = [i for i in in_flight_ids if after.get(model.QUEUED)]  # informational
        self.restart_info = {
            "at_virtual_secs": round(t_down - self.t0, 1),
            "completed_before": len(self.completed_ids),
            "live_before": live_before,
            "in_flight_at_crash": len(in_flight_ids),
            "after_reconcile": after,
            "old_incarnation": old_incarnation[:8],
            "new_incarnation": self.mgr._taskq.incarnation[:8],
            "terminal_regressed": self._terminal_regressed(before, after),
            "queued_survived": after.get(model.QUEUED, 0) == before.get(model.QUEUED, 0),
        }
        del redispatched
        self.crashed = False
        if self.faults.start_collector_probe:
            self.metrics.start_collector = await start_collector_probe()
        self._record_cap(force=True)
        self.mgr._drain_queue()
        await settle_loop()

    @staticmethod
    def _terminal_regressed(before: dict[str, int], after: dict[str, int]) -> bool:
        from kiro_crew.taskq import model

        return any(after.get(s, 0) < before.get(s, 0) for s in model.TERMINAL)

    def _finalize(self) -> None:
        from kiro_crew.taskq import model

        m = self.metrics
        store = self.mgr._taskq
        by_state = store.count_by_state()
        m.completed = by_state.get(model.DONE, 0)
        m.failed = by_state.get(model.FAILED, 0)
        m.cancelled = by_state.get(model.CANCELLED, 0)
        m.unknown_side_effect = by_state.get(model.UNKNOWN_SIDE_EFFECT, 0)
        terminal = sum(by_state.get(s, 0) for s in model.TERMINAL)
        m.remaining = store.count() - terminal
        m.lost = len(self.accepted_at) - store.count()
        m.duplicate_dispatches = sum(max(0, c - 1) for c in self.starts_per_id.values())
        m.starts = sum(self.starts_per_id.values())
        if self.queue_waits:
            waits = sorted(self.queue_waits)
            m.queue_wait_p50_s = round(statistics.median(waits), 2)
            m.queue_wait_p95_s = round(waits[min(len(waits) - 1, int(0.95 * len(waits)))], 2)
            m.queue_wait_max_s = round(waits[-1], 2)
        m.max_in_flight = self.max_in_flight
        m.peak_window = self.peak_window
        m.effective_cap_timeline = self.cap_timeline
        m.effective_cap_min = min((row["exec_cap"] for row in self.cap_timeline), default=0)
        if self.controller is not None:
            m.controller_counts = dict(self.controller.state().get("counts", {}))
        m.host_budget_peaks = {
            "procs": self.peak_procs,
            "fds": self.peak_fds,
            "rss_mb": self.peak_rss,
        }
        m.gate = self.gate.snapshot()
        m.recovery_attempts_by_layer = dict(self.ladder_attempts)
        m.restarts = self.restarts
        m.escalations_notified = self.notified
        m.waits_by_state = dict(self.waits_by_state)
        m.resumes_granted = self.resumes
        m.coordinator_scopes_seen = len(self.scopes_seen)
        m.coordinator_scopes = sorted(self.scopes_seen)
        m.coordinator_reports = self.coordinator_reports
        m.coordinator_wakes = self.coordinator_wakes
        m.coordinator_max_scopes_live = self.max_scopes_live
        m.throttle_reports_by_session = dict(self.throttle_reports)
        if self.outage_ended_at is not None and self.outage_recovered_at is not None:
            m.outage_recovery_secs = round(self.outage_recovered_at - self.outage_ended_at, 1)
        if self.throttle_ended_at is not None and self.throttle_recovered_at is not None:
            m.throttle_recovery_secs = round(self.throttle_recovered_at - self.throttle_ended_at, 1)
        m.restart = self.restart_info
        m.infra_refusals = self.infra_refusals
        m.scope_attempts_peak = self.scope_attempts_peak
        first, starvation = fairness(self.start_log, self.accepted_at, self.lane_of)
        m.fairness_first_grant_index = first
        m.fairness_max_starvation = starvation
        m.per_lane_accepted = dict(Counter(self.lane_of.values()))
        if self.tree_roots:
            m.tree = self._tree_summary()
        m.virtual_secs = round(self._elapsed(), 1)
        m.wall_secs = round(time.monotonic() - self.wall_start, 2)
        m.virtual_events = self.clock.events_fired
        m.invariants = {
            "no_duplicate_dispatch": m.duplicate_dispatches == 0,
            "no_lost_task": m.lost == 0 and m.remaining == 0,
            "in_flight_within_user_max": m.max_in_flight
            <= m.user_max + FairnessReserve.lift_allowance(),
            "window_bounded": m.peak_window <= store.window,
            "host_procs_bounded": self.peak_procs <= self.budget.limits.max_procs,
            "no_manual_intervention": m.manual_interventions == 0,
        }

    def _tree_summary(self) -> dict[str, Any]:
        from kiro_crew.taskq import model

        store = self.mgr._taskq
        roots_done = sum(1 for r in self.tree_roots if store.state_of(r) == model.DONE)
        children = [c for kids in self.tree_children_of.values() for c in kids]
        children_done = sum(1 for c in children if store.state_of(c) == model.DONE)
        depth_seen = 0
        for root in self.tree_roots:
            d, cur = 0, root
            while self.tree_children_of.get(cur):
                cur = self.tree_children_of[cur][0]
                d += 1
            depth_seen = max(depth_seen, d)
        return {
            "roots": len(self.tree_roots),
            "roots_done": roots_done,
            "children": len(children),
            "children_done": children_done,
            "max_depth": depth_seen,
            "waiting_children_entered": self.waits_by_state.get("waiting_children", 0),
        }


class FairnessReserve:
    """The child reserve may lift the effective cap by one under an adaptive squeeze."""

    @staticmethod
    def lift_allowance() -> int:
        return 1


def fairness(
    start_log: list[tuple[float, str, str]],
    accepted_at: dict[str, float],
    lane_of: dict[str, str],
) -> tuple[dict[str, int], dict[str, int]]:
    """Per-lane first-grant index and the longest run of other-lane starts a
    lane sat through while it still had pending work."""
    first: dict[str, int] = {}
    pending: Counter[str] = Counter(lane_of[t] for t in accepted_at if t in lane_of)
    gap: Counter[str] = Counter()
    worst: dict[str, int] = defaultdict(int)
    for idx, (_t, task_id, lane) in enumerate(start_log):
        first.setdefault(lane, idx)
        for other in list(pending):
            if other == lane or pending[other] <= 0:
                continue
            gap[other] += 1
            worst[other] = max(worst[other], gap[other])
        gap[lane] = 0
        pending[lane] -= 1
    return dict(first), dict(worst)


# ── StartCollector probe (real AcpRuntime, fake pipe) ─────────────────────────


async def start_collector_probe() -> dict[str, Any]:
    """A ``session/new`` whose answer outlives the budget is ADOPTED by the
    real ``StartCollector``, not requeued: no orphan session, the gate permit
    released exactly once. Same shape as ``test_session_start_gate.py``."""
    import kiro_crew.acp.runtime as runtime_mod
    from kiro_crew.acp.runtime import AcpRuntime, AcpSessionStartTimeout

    runtime_mod._session_start_gates.clear()
    out: dict[str, Any] = {"ran": True}
    with patch.object(runtime_mod, "_resolve_session_start_concurrency", lambda: 2):
        rt = AcpRuntime(work_dir=tempfile.gettempdir())
        reader = asyncio.StreamReader()
        proc = MagicMock()
        proc.stdout = reader
        proc.stdin = MagicMock()
        proc.stdin.write = MagicMock()
        proc.stdin.drain = AsyncMock()
        proc.returncode = None
        proc.pid = 4242
        rt._process = proc
        rt._pid = 4242
        rt._initialized = True
        rt._expect_mcp_reports = False
        rt._session_start_timeout = 0.05
        rt._start_collect_timeout = 2.0
        reader_task = asyncio.ensure_future(rt._reader_loop())
        await asyncio.sleep(0)
        adopted: list[str] = []

        async def _adopter(handle: Any) -> bool:
            adopted.append(str(getattr(handle, "session_id", "")))
            return True

        gate = await runtime_mod.session_start_gate()
        try:
            try:
                await rt.create_session(cwd="/w", mcp_servers=[], late_adopter=_adopter)
                out["outcome"] = "no_timeout"
                return out
            except AcpSessionStartTimeout as exc:
                collector = exc.collector
            if collector is None:
                out["outcome"] = "no_collector"
                return out
            req_id = collector.req_id
            out["request_still_owned"] = req_id in rt._pending_requests
            out["adopted_flag"] = req_id in rt._pending_requests.adopted
            with patch.object(rt, "terminate_session", AsyncMock()):
                reader.feed_data(
                    (
                        json.dumps({"id": req_id, "result": {"sessionId": "late-sid"}}) + "\n"
                    ).encode()
                )
                deadline = time.monotonic() + 3.0
                answered: set[int] = set()
                while not collector.settled.is_set() and time.monotonic() < deadline:
                    for rid in list(rt._pending_requests):
                        if rid != req_id and rid not in answered:
                            answered.add(rid)
                            reader.feed_data(
                                (json.dumps({"id": rid, "result": {}}) + "\n").encode()
                            )
                    await asyncio.sleep(0)
            out["outcome"] = collector.outcome
            out["adopted_sessions"] = adopted
            out["gate_releases"] = gate.releases
            out["gate_active_after"] = gate.active
            out["request_dropped_after"] = req_id not in rt._pending_requests
        finally:
            reader_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await reader_task
            runtime_mod._session_start_gates.clear()
    return out


# ── scenarios ──────────────────────────────────────────────────────────────────


def faults_for(scenario: str, n: int) -> Faults:
    if scenario == "baseline":
        return Faults()
    if scenario == "controller":
        # timeouts strike runs started between 30 s and 130 s (40 %), then stop
        return Faults(controller=True, adaptive_initial=10, timeout_window=(30.0, 60.0, 0.4))
    if scenario == "controller_raw":
        # same injection, but the gate counter is fed as the daemon reports it
        return Faults(
            controller=True,
            adaptive_initial=10,
            timeout_window=(30.0, 60.0, 0.4),
            gate_stats_windowed=False,
        )
    if scenario == "restart":
        return Faults(restart_after_completed=max(5, n // 4))
    if scenario == "outage":
        return Faults(outage=(20.0, 20 * 60.0))
    if scenario == "throttle":
        return Faults(controller=True, adaptive_initial=10, throttle_window=(10.0, 130.0))
    if scenario == "tree":
        return Faults(tree_every=4, tree_depth=2)
    raise ValueError(f"unknown scenario {scenario!r}")


def scenario_home(root: Path, scenario: str) -> Path:
    """A FRESH data home per run: a reused store would carry the previous
    run's rows into this one's counts."""
    root.mkdir(parents=True, exist_ok=True)
    return Path(tempfile.mkdtemp(prefix=f"home-{scenario}-", dir=root))


async def run_scenario(
    scenario: str,
    *,
    n: int,
    home_root: Path,
    seed: int = 7,
    user_max: Optional[int] = None,
    max_virtual_secs: float = 6 * 3600.0,
) -> Metrics:
    faults = faults_for(scenario, n)
    if user_max is None:
        user_max = 2 if scenario == "tree" else 10
    home = scenario_home(home_root, scenario)
    previous = os.environ.get("KIROCREW_HOME")
    os.environ["KIROCREW_HOME"] = str(home)
    try:
        h = Harness(scenario=scenario, n=n, faults=faults, user_max=user_max, seed=seed)
        try:
            await h.drive(max_virtual_secs=max_virtual_secs)
        finally:
            h.close()
        return h.metrics
    finally:
        if previous is not None:
            os.environ["KIROCREW_HOME"] = previous


# ── report ─────────────────────────────────────────────────────────────────────


def _md_table(rows: list[list[Any]], header: list[str]) -> str:
    out = ["| " + " | ".join(header) + " |", "|" + "|".join("---" for _ in header) + "|"]
    for row in rows:
        out.append("| " + " | ".join(str(c) for c in row) + " |")
    return "\n".join(out)


def render_markdown(results: list[Metrics], *, machine: dict[str, Any]) -> str:
    lines = ["# Overload experiment results", ""]
    lines.append(
        "Machine: "
        + ", ".join(f"{k}={v}" for k, v in machine.items())
        + ". Fake harness, virtual clock."
    )
    lines.append("")
    header = [
        "scenario",
        "n",
        "user max",
        "done",
        "failed",
        "cancelled",
        "unknown_se",
        "remaining",
        "lost",
        "dup",
        "wait p50",
        "wait p95",
        "wait max",
        "max in-flight",
        "peak procs",
        "cap min",
        "restarts",
        "virtual s",
        "wall s",
    ]
    rows = []
    for m in results:
        rows.append(
            [
                m.scenario,
                m.accepted,
                m.user_max,
                m.completed,
                m.failed,
                m.cancelled,
                m.unknown_side_effect,
                m.remaining,
                m.lost,
                m.duplicate_dispatches,
                m.queue_wait_p50_s,
                m.queue_wait_p95_s,
                m.queue_wait_max_s,
                m.max_in_flight,
                m.host_budget_peaks.get("procs", 0),
                m.effective_cap_min,
                m.restarts,
                m.virtual_secs,
                m.wall_secs,
            ]
        )
    lines.append(_md_table(rows, header))
    lines.append("")
    for m in results:
        lines.append(f"## {m.scenario}")
        lines.append("")
        lines.append(
            "Invariants: "
            + ", ".join(f"{k}={'OK' if v else 'FAIL'}" for k, v in sorted(m.invariants.items()))
        )
        lines.append("")
        if m.effective_cap_timeline:
            lines.append("Effective-cap timeline (virtual seconds since t0):")
            lines.append("")
            lines.append(
                _md_table(
                    [
                        [
                            r["t"],
                            r["exec_cap"],
                            r["gate_cap"],
                            r["running"],
                            r["queued_window"],
                            r["procs"],
                        ]
                        for r in m.effective_cap_timeline[:60]
                    ],
                    ["t", "exec cap", "gate cap", "running", "window", "procs"],
                )
            )
            lines.append("")
        if m.controller_counts:
            lines.append("Controller decisions: " + json.dumps(m.controller_counts))
            lines.append("")
        if m.recovery_attempts_by_layer:
            lines.append("Recovery attempts by layer: " + json.dumps(m.recovery_attempts_by_layer))
            lines.append(
                f"Restarts: {m.restarts}; L5 notifications: {m.escalations_notified}; "
                f"outage recovery: {m.outage_recovery_secs}s"
            )
            lines.append("")
        if m.waits_entered:
            lines.append(
                f"Waits entered: {m.waits_entered} {json.dumps(m.waits_by_state)}; resumes granted: "
                f"{m.resumes_granted}; coordinator scopes seen: {m.coordinator_scopes_seen} "
                f"(max live {m.coordinator_max_scopes_live}); reports: {m.coordinator_reports}; "
                f"wakes: {m.coordinator_wakes}"
            )
            lines.append("")
        if m.throttle_reports_by_session:
            lines.append(
                "429 reports by session: "
                + json.dumps(m.throttle_reports_by_session)
                + f"; recovery after storm: {m.throttle_recovery_secs}s"
            )
            lines.append("")
        if m.restart:
            lines.append("Restart: " + json.dumps(m.restart, sort_keys=True))
            lines.append("")
        if m.start_collector:
            lines.append("StartCollector probe: " + json.dumps(m.start_collector, sort_keys=True))
            lines.append("")
        if m.fairness_first_grant_index:
            lines.append(
                _md_table(
                    [
                        [
                            lane,
                            m.per_lane_accepted.get(lane, 0),
                            m.fairness_first_grant_index.get(lane, "-"),
                            m.fairness_max_starvation.get(lane, 0),
                        ]
                        for lane in sorted(m.per_lane_accepted)
                    ],
                    ["lane", "accepted", "first-grant index", "max starvation (starts)"],
                )
            )
            lines.append("")
        if m.tree:
            lines.append("Tree: " + json.dumps(m.tree, sort_keys=True))
            lines.append("")
    return "\n".join(lines) + "\n"


def machine_info() -> dict[str, Any]:
    import platform

    return {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "cpus": os.cpu_count(),
    }


async def _main_async(args: argparse.Namespace) -> int:
    ensure_experiment_home()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    home_root = Path(os.environ["KIROCREW_HOME"])
    scenarios = list(SCENARIOS) if args.scenario == "all" else [args.scenario]
    results: list[Metrics] = []
    for name in scenarios:
        n = args.n if name != "tree" else min(args.n, args.tree_n)
        m = await run_scenario(name, n=n, home_root=home_root, seed=args.seed)
        results.append(m)
        print(
            f"[{name}] accepted={m.accepted} done={m.completed} failed={m.failed} "
            f"unknown_se={m.unknown_side_effect} remaining={m.remaining} lost={m.lost} "
            f"dup={m.duplicate_dispatches} p95_wait={m.queue_wait_p95_s}s "
            f"max_inflight={m.max_in_flight} cap_min={m.effective_cap_min} "
            f"virtual={m.virtual_secs}s wall={m.wall_secs}s",
            flush=True,
        )
    machine = machine_info()
    payload: dict[str, Any] = {"machine": machine, "results": [m.as_dict() for m in results]}
    (out / "overload-results.json").write_text(json.dumps(payload, indent=2, sort_keys=True))
    (out / "overload-results.md").write_text(render_markdown(results, machine=machine))
    print(f"wrote {out / 'overload-results.json'} and {out / 'overload-results.md'}")
    bad = [m.scenario for m in results if not all(m.invariants.values())]
    return 1 if bad else 0


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--n", type=int, default=2000)
    parser.add_argument("--tree-n", type=int, default=400, help="cap for the tree scenario")
    parser.add_argument("--scenario", choices=("all",) + SCENARIOS, default="all")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--out", default=os.path.join(tempfile.gettempdir(), "overload-experiment"))
    args = parser.parse_args(argv)
    return asyncio.run(_main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
