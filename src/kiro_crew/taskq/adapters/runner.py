"""Runner adapters: TaskRunner steps and workflow agent calls as taskq rows.

The subagent entry writes a row before it acknowledges a spawn, defers it
under memory pressure instead of refusing, and claims it under a lease when a
slot frees (``subagent_manager/admission/``). The two other entry points
that start agent turns -- a TaskRunner step (``task_executor.execute_task``)
and a workflow ``ctx.agent()`` call (``workflows/agent_pool.py``) -- run on
``SessionManager`` sessions, not on ``SubagentManager.spawn``, so they cannot
reuse that code path literally. This module gives them the same CONTRACT over
the same store:

* :class:`RunnerLane` -- one execution gate bounded by the live effective cap.
  It is the "equivalent hook" the adaptive controller (RFC §5.2) needs for the
  runner entries: ``set_effective_cap`` lowers or lifts the bound at runtime,
  and ``mode=fixed`` pins it. Waiters are FIFO and hold nothing but a future.
* :class:`RunnerAdmission` -- ``accept`` (write-before-ack; a failed write is a
  refusal, never an id), ``admit`` (defer on pressure, then a lane slot, then
  ``TaskStore.claim`` under a lease), and the :class:`Admitted` handle the
  runner drives: ``running`` / ``progress`` / ``renew`` / ``settle``,
  ``recovering`` for a turn that ended on a non-success stop reason, and the
  two waits a step can enter -- ``yield_dependency`` (a dependency signal parks
  the row in ``waiting_dependency`` and the lane slot is released until the
  coordinator wakes it) and ``waiting_input`` (a terminal question).
* :func:`adopt_orphaned_rows` -- the recovery adapter for the rows the boot
  reconciler could only stamp ``awaiting_adapter``: a run whose interrupted
  step is safe to retry is resumed from the runner's own checkpoint; one whose
  step may have had a side effect is settled ``unknown_side_effect`` and left
  for a human, never re-run blind.

Lanes follow RFC §13 Q5: a run launched by a cron or a hook queues in the
``system`` lane; anything else queues under the session that launched it.

Specification: ``docs/system-specs/modules/taskq.md`` (§ Runner adapters),
``taskrunner.md``, ``workflows.md``.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Iterable

from kiro_crew.recovery.ladder import L3_ACP_RUNTIME, RecoveryDecision, RecoveryLadder

from ..dependency import DependencyCoordinator, DependencySignal
from ..lanes import SYSTEM_LANE, lane_key_for
from ..migrate import TASKRUNNER_ID_PREFIX
from ..model import (
    ADMITTED,
    CANCELLED,
    CLAIMABLE,
    DONE,
    FAILED,
    KIND_TASKRUNNER_STEP,
    KIND_WORKFLOW_AGENT,
    QUEUED,
    RECOVERING,
    RUNNING,
    SIDE_EFFECT_IDEMPOTENT_KEY,
    SIDE_EFFECT_NONE,
    SIDE_EFFECT_UNKNOWN,
    STARTING,
    TERMINAL,
    UNKNOWN_SIDE_EFFECT,
    WAITING,
    WAITING_DEPENDENCY,
    WAITING_INPUT,
    TaskRecord,
)
from ..store import TaskStore, TaskStoreUnavailable
from ..waits import WaitLedger, WaitRecord

logger = logging.getLogger(__name__)

#: The lane every cron- and hook-launched run shares (RFC §13 Q5, weight 1).
LANE_SYSTEM = SYSTEM_LANE
#: Launch sources that have no watching session and therefore no lane of their own.
SYSTEM_SOURCES: frozenset[str] = frozenset({"cron", "hook"})

WORKFLOW_ID_PREFIX = "workflow:"

MODE_AIMD = "aimd"
MODE_FIXED = "fixed"

#: Kinds this module knows how to rebuild from a stored row.
RUNNER_RECOVERY_ADAPTERS: frozenset[str] = frozenset({KIND_TASKRUNNER_STEP, KIND_WORKFLOW_AGENT})

#: Row param naming the lane the row queues in.
PARAM_LANE = "lane"
#: Row param: the runner may re-run the interrupted step from its checkpoint.
PARAM_SAFE_RETRY = "safe_retry"

_DEFAULT_ADMIT_WAIT_SECS = 30.0
#: Bound on one sleep inside ``admit``: a deferred row re-checks the store at
#: least this often so a cancel landing during the wait is seen promptly.
_MAX_ADMIT_SLEEP_SECS = 30.0


def lane_for(session_key: str | None, source: str = "") -> str:
    """The lane a run queues in: ``taskq.lanes.lane_key_for`` (the ONE lane
    key policy: automation keys and the empty key are ``system``), with the
    runner's launch *source* as the extra fact the key alone cannot carry --
    a cron- or hook-launched run is a ``system`` root whatever its key says.
    """
    if str(source or "") in SYSTEM_SOURCES:
        return LANE_SYSTEM
    return lane_key_for(session_key)


def step_task_id(run_id: str, index: int) -> str:
    """Row id of one TaskRunner step; the run row is ``taskrunner:<run_id>``."""
    return f"{TASKRUNNER_ID_PREFIX}{run_id}:task{int(index)}"


def run_task_id(run_id: str) -> str:
    return f"{TASKRUNNER_ID_PREFIX}{run_id}"


def workflow_task_id(run_id: str, call_no: int) -> str:
    return f"{WORKFLOW_ID_PREFIX}{run_id}:agent{int(call_no)}"


OWNER_TASKRUNNER = "taskrunner"
OWNER_WORKFLOW = "workflow"


def owner_of(task_id: str) -> tuple[str, str] | None:
    """``(owner, run_id)`` for a runner row id, else None.

    ``taskrunner:<run>`` and ``taskrunner:<run>:task<N>`` belong to the
    TaskRunner run ``<run>``; ``workflow:<run>:agent<N>`` to the workflow run
    ``<run>``. A ``~N`` re-run suffix on the last segment is dropped. The
    cancel adapter routes on this: a step or an agent call is one unit of its
    run, so cancelling it means cancelling that run.
    """
    if task_id.startswith(TASKRUNNER_ID_PREFIX):
        rest = task_id[len(TASKRUNNER_ID_PREFIX) :]
        run_id = rest.split(":", 1)[0].split("~", 1)[0]
        return (OWNER_TASKRUNNER, run_id) if run_id else None
    if task_id.startswith(WORKFLOW_ID_PREFIX):
        rest = task_id[len(WORKFLOW_ID_PREFIX) :]
        run_id = rest.rsplit(":", 1)[0] if ":" in rest else rest.split("~", 1)[0]
        return (OWNER_WORKFLOW, run_id) if run_id else None
    return None


class RunnerAdmissionRefused(RuntimeError):
    """The row could not be written: NOTHING was accepted."""


class RunnerTaskCancelled(RuntimeError):
    """The row was cancelled (or ended) before the runner could start it."""


# ── lane ──────────────────────────────────────────────────────────────────────


class RunnerLane:
    """FIFO execution gate whose bound is the LIVE effective cap.

    ``cap`` is the ceiling source -- a callable read on every decision (the
    manager's ``max_concurrent``, which the adaptive controller already
    clamps), or a constant. ``set_effective_cap`` is the controller's own
    seam for this lane: ``None`` removes the bound, ``0`` pauses new grants
    (in-flight work finishes; nothing is cancelled), a raise wakes waiters.
    ``mode`` (callable or constant) selects ``fixed``, which pins the bound
    at ``pinned`` (or the ceiling) whatever the controller says -- the one
    config flip that turns the adaptive lane back into a plain semaphore.

    No asyncio primitive is created at import or construction: every waiter
    is a future minted on the loop that called :meth:`acquire`.
    """

    def __init__(
        self,
        cap: Callable[[], int] | int = 4,
        *,
        mode: Callable[[], str] | str = MODE_AIMD,
        pinned: int | None = None,
        name: str = "runner-lane",
    ) -> None:
        if callable(cap):
            self._cap: Callable[[], int] = cap
        else:
            fixed_cap = int(cap)
            self._cap = lambda: fixed_cap
        if callable(mode):
            self._mode: Callable[[], str] = mode
        else:
            fixed_mode = str(mode)
            self._mode = lambda: fixed_mode
        self._pinned = None if pinned is None else max(0, int(pinned))
        self._override: int | None = None
        self._running = 0
        self._waiters: deque[tuple[asyncio.Future[None], str]] = deque()
        self._name = name
        self._granted = 0

    # -- bound ---------------------------------------------------------------

    @property
    def ceiling(self) -> int:
        try:
            return max(0, int(self._cap()))
        except Exception:  # noqa: BLE001 - a broken cap source pins at one
            logger.debug("%s: cap source failed; assuming 1", self._name, exc_info=True)
            return 1

    @property
    def mode(self) -> str:
        try:
            value = str(self._mode() or MODE_AIMD)
        except Exception:  # noqa: BLE001 - a broken mode source reads as aimd
            value = MODE_AIMD
        return MODE_FIXED if value == MODE_FIXED else MODE_AIMD

    @property
    def effective(self) -> int:
        """The bound in force now: pinned under ``fixed``, else min(ceiling, override)."""
        ceiling = self.ceiling
        if self.mode == MODE_FIXED:
            return ceiling if self._pinned is None else self._pinned
        if self._override is None:
            return ceiling
        return max(0, min(ceiling, self._override))

    def set_effective_cap(self, cap: int | None) -> int:
        """Adaptive-controller seam; returns the effective bound now in force."""
        self._override = None if cap is None else max(0, int(cap))
        self.pump()
        return self.effective

    # -- occupancy -----------------------------------------------------------

    @property
    def running(self) -> int:
        return self._running

    @property
    def waiting(self) -> int:
        return len(self._waiters)

    def stats(self) -> dict[str, Any]:
        return {
            "name": self._name,
            "running": self._running,
            "waiting": len(self._waiters),
            "effective": self.effective,
            "ceiling": self.ceiling,
            "mode": self.mode,
            "granted": self._granted,
        }

    def pump(self) -> int:
        """Grant slots to the oldest waiters while the bound allows; returns grants."""
        granted = 0
        while self._waiters and self._running < self.effective:
            fut, _task_id = self._waiters.popleft()
            if fut.done():
                continue
            self._running += 1
            self._granted += 1
            fut.set_result(None)
            granted += 1
        return granted

    async def acquire(self, task_id: str = "") -> None:
        """Wait for a slot. FIFO; a cancelled waiter leaves the queue cleanly."""
        if not self._waiters and self._running < self.effective:
            self._running += 1
            self._granted += 1
            return
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[None] = loop.create_future()
        self._waiters.append((fut, task_id))
        try:
            await fut
        except asyncio.CancelledError:
            # The task's cancel reaches the future first: a waiter that was
            # still parked sees ``fut.cancelled()`` and holds no slot, so only
            # its queue entry goes (``_running`` is untouched). A future that
            # already holds a grant was resolved before the cancel landed:
            # that slot is handed on. Mirrors ``asyncio.Semaphore.acquire``.
            if fut.cancelled():
                self._waiters = deque(w for w in self._waiters if w[0] is not fut)
            else:
                self._running -= 1
                self.pump()
            raise

    def release(self) -> None:
        if self._running > 0:
            self._running -= 1
        self.pump()


# ── admission ─────────────────────────────────────────────────────────────────


@dataclass
class Admitted:
    """A claimed row the runner is executing. Every write is generation-fenced."""

    admission: "RunnerAdmission"
    task_id: str
    kind: str
    lane: str
    generation: int
    session_key: str = ""
    side_effect_class: str = SIDE_EFFECT_UNKNOWN
    safe_retry: bool = False
    _slot_held: bool = field(default=True, repr=False)
    _settled: bool = field(default=False, repr=False)
    _state: str = field(default=STARTING, repr=False)

    @property
    def state(self) -> str:
        return self._state

    @property
    def slot_held(self) -> bool:
        return self._slot_held

    def _store(self) -> TaskStore | None:
        return self.admission.store

    def _write(self, state: str, **kw: Any) -> bool:
        store = self._store()
        if store is None:
            self._state = state
            return True
        try:
            ok = store.transition(self.task_id, state, generation=self.generation, **kw)
        except TaskStoreUnavailable:
            logger.debug("taskq runner: %s -> %s write failed", self.task_id, state, exc_info=True)
            return False
        if ok:
            self._state = state
        return ok

    def running(self, progress: dict[str, Any] | None = None) -> bool:
        """``starting -> running`` (also the re-entry after a recovery claim)."""
        if self._state == RUNNING:
            return True
        return self._write(RUNNING, progress=progress)

    def progress(self, marker: dict[str, Any]) -> bool:
        store = self._store()
        if store is None:
            return True
        try:
            return store.record_progress(self.task_id, self.generation, marker)
        except TaskStoreUnavailable:
            return False

    def renew(self) -> bool:
        """Extend the lease; False means another incarnation owns the row now."""
        store = self._store()
        if store is None:
            return True
        try:
            return store.renew_lease(self.task_id, self.generation)
        except TaskStoreUnavailable:
            return False

    def release_slot(self) -> None:
        """Give the lane slot back (idempotent). The row keeps its state."""
        if self._slot_held:
            self._slot_held = False
            self.admission.lane.release()

    async def reacquire_slot(self) -> None:
        if not self._slot_held:
            await self.admission.lane.acquire(self.task_id)
            self._slot_held = True

    def settle(
        self, state: str, *, result_ref: str | None = None, error: str | None = None
    ) -> bool:
        """Terminal write FIRST, then slot release and forget. A second call
        is a no-op.

        The handle is marked settled only once the terminal write committed,
        or once the admission's durable retry owns it (the store was
        unavailable: the write is replayed by ``tick`` until it commits). A
        write fenced by a newer generation means another owner already ended
        the row -- nothing to retry, the handle just lets go. Returns True
        only for a committed write.
        """
        if self._settled:
            return False
        store = self._store()
        ok = True
        if store is not None:
            try:
                ok = store.finish(
                    self.task_id,
                    state,
                    generation=self.generation,
                    result_ref=result_ref,
                    error=error,
                )
            except TaskStoreUnavailable:
                logger.warning(
                    "taskq runner: terminal write for %s deferred (store unavailable)",
                    self.task_id,
                )
                self.admission.defer_terminal_write(
                    self.task_id, self.generation, state, result_ref=result_ref, error=error
                )
                ok = False
        self._settled = True
        self.release_slot()
        if ok:
            self._state = state
        if self.task_id not in self.admission._pending_finish:
            self.admission.forget(self.task_id)
        return ok

    def done(self, result_ref: str | None = None) -> bool:
        return self.settle(DONE, result_ref=result_ref)

    def fail(self, error: str) -> bool:
        return self.settle(FAILED, error=error)

    def cancel(self, reason: str = "") -> bool:
        return self.settle(CANCELLED, error=reason or None)

    def recovering(self, *, reason: str, delay_secs: float) -> bool:
        """``running -> recovering`` with ``next_run_at = now + delay``; slot released.

        The row becomes claimable again after the delay; :meth:`reclaim` takes
        it back under a NEW generation, so a late callback from the interrupted
        turn is fenced out as ``stale_result``.
        """
        self.release_slot()
        until = self.admission.now() + max(0.0, float(delay_secs))
        return self._write(
            RECOVERING,
            next_run_at=until,
            detail={"reason": str(reason)[:200], "delay_secs": round(float(delay_secs), 3)},
        )

    async def reclaim(self) -> bool:
        """Re-admit a ``recovering`` row: a lane slot, then a fresh claim."""
        handle = await self.admission.admit(self.task_id, kind=self.kind, lane=self.lane)
        self.generation = handle.generation
        self._slot_held = True
        self._state = handle.state
        return True


class RunnerAdmission:
    """Admission for runner entries over the shared task store.

    ``store`` is a :class:`TaskStore`, a zero-arg callable returning one (or
    ``None`` while the durable queue is off), or ``None``. With no store the
    lane still bounds concurrency; nothing is persisted and every handle
    reports success -- the legacy in-memory behaviour, kept so a broken
    ``tasks.db`` never turns into a refused run.

    ``pressure`` answers ``resource_status.cached_admission_check`` shape
    (``.admitted``/``.reason``); a refusal defers the row for
    ``admit_wait_secs`` instead of refusing it. ``clock`` / ``sleep`` are
    injectable so tests run on a fake clock.
    """

    def __init__(
        self,
        store: TaskStore | Callable[[], TaskStore | None] | None,
        *,
        lane: RunnerLane | None = None,
        pressure: Callable[[], Any] | None = None,
        admit_wait_secs: float = _DEFAULT_ADMIT_WAIT_SECS,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        ladder: RecoveryLadder | None = None,
        coordinator: DependencyCoordinator | None = None,
        name: str = "runner",
        require_store: bool = False,
        store_error: Callable[[], str | None] | None = None,
    ) -> None:
        self._store_src: Callable[[], TaskStore | None]
        if store is None:
            self._store_src = lambda: None
        elif isinstance(store, TaskStore):
            fixed_store: TaskStore = store
            self._store_src = lambda: fixed_store
        else:
            self._store_src = store
        self.lane = lane or RunnerLane(name=f"{name}-lane")
        self._pressure = pressure
        self._admit_wait = max(0.0, float(admit_wait_secs))
        self._clock = clock
        self._sleep = sleep
        self._ladder = ladder
        self._coordinator = coordinator
        self._name = name
        self._wakes: dict[str, asyncio.Event] = {}
        self._input_answers: dict[str, str | None] = {}
        self._deferred = 0
        #: ``agent.task_queue_enabled``: with the queue ON, a missing store is
        #: a REFUSAL (typed), never a fall-back to lane-only admission whose
        #: rows a restart cannot see. ``store_error`` names why it is missing.
        self._require_store = bool(require_store)
        self._store_error = store_error
        #: Terminal writes that hit ``TaskStoreUnavailable``: (generation,
        #: state, result_ref, error) per task, retried by :meth:`tick` until
        #: the write commits or the row is fenced. A handle is settled only
        #: once its write committed OR this durable retry owns it.
        self._pending_finish: dict[str, tuple[int, str, str | None, str | None]] = {}

    # -- plumbing ------------------------------------------------------------

    @property
    def store(self) -> TaskStore | None:
        try:
            return self._store_src()
        except Exception:  # noqa: BLE001 - a broken getter means no store
            return None

    @property
    def coordinator(self) -> DependencyCoordinator | None:
        return self._coordinator

    def attach_coordinator(self, coordinator: DependencyCoordinator | None) -> None:
        self._coordinator = coordinator

    @property
    def ladder(self) -> RecoveryLadder | None:
        return self._ladder

    def now(self) -> float:
        return float(self._clock())

    @property
    def deferred_count(self) -> int:
        return self._deferred

    def forget(self, task_id: str) -> None:
        self._wakes.pop(task_id, None)
        self._input_answers.pop(task_id, None)
        if self._coordinator is not None:
            try:
                self._coordinator.forget(task_id)
            except Exception:  # noqa: BLE001 - advisory
                logger.debug("dependency coordinator forget failed", exc_info=True)

    def stats(self) -> dict[str, Any]:
        return {
            **self.lane.stats(),
            "deferred": self._deferred,
            "store": self.store is not None,
            "pending_terminal_writes": len(self._pending_finish),
        }

    def _refuse_if_store_required(self) -> None:
        if self._require_store and self.store is None:
            why = None
            if self._store_error is not None:
                try:
                    why = self._store_error()
                except Exception:  # noqa: BLE001 - the reason is advisory
                    why = None
            raise RunnerAdmissionRefused(
                why or "durable task queue enabled but its store is unavailable"
            )

    def defer_terminal_write(
        self,
        task_id: str,
        generation: int,
        state: str,
        *,
        result_ref: str | None,
        error: str | None,
    ) -> None:
        """Own a terminal write the store could not take right now; :meth:`tick`
        retries it. Newest write per task wins (a task ends once)."""
        self._pending_finish[task_id] = (generation, state, result_ref, error)

    def retry_terminal_writes(self) -> int:
        """Replay pending terminal writes; returns how many committed or were
        fenced (either way the retry stops owning them)."""
        store = self.store
        if store is None or not self._pending_finish:
            return 0
        settled = 0
        for task_id, (generation, state, result_ref, error) in list(self._pending_finish.items()):
            try:
                store.finish(
                    task_id, state, generation=generation, result_ref=result_ref, error=error
                )
            except TaskStoreUnavailable:
                continue  # still down; keep owning the write
            # Committed, or fenced by a newer generation (another owner ended
            # the row): in both cases the row's state is someone's decision.
            self._pending_finish.pop(task_id, None)
            self._wakes.pop(task_id, None)
            self._input_answers.pop(task_id, None)
            settled += 1
        return settled

    # -- accept (write-before-ack) ------------------------------------------

    def accept(
        self,
        *,
        kind: str,
        task_id: str,
        session_key: str = "",
        source: str = "",
        params: dict[str, Any] | None = None,
        workspace: str | None = None,
        scope_ref: dict[str, Any] | None = None,
        side_effect_class: str = SIDE_EFFECT_UNKNOWN,
        parent_id: str | None = None,
        deadline_at: float | None = None,
        provider: str | None = None,
    ) -> TaskRecord | None:
        """Persist the row and return it; ``None`` when no store is attached.

        A row that already exists and is still claimable is RETURNED, not
        duplicated -- that is how a resumed run re-attaches to the row a
        crashed incarnation left ``recovering``. A terminal row with the same
        id (a step re-run after a failure) gets a ``~N`` suffix so the earlier
        outcome stays on the record. Raises :class:`RunnerAdmissionRefused`
        when the write fails: the caller has accepted nothing.
        """
        self._refuse_if_store_required()
        store = self.store
        if store is None:
            return None
        lane = lane_for(session_key, source)
        body = dict(params or {})
        body[PARAM_LANE] = lane
        body.setdefault("source", str(source or ""))
        root_id = ""
        if parent_id:
            try:
                parent = store.get(parent_id)
            except TaskStoreUnavailable:
                parent = None
            if parent is None:
                parent_id = None
            else:
                root_id = parent.root_id or parent.id
        chosen = task_id
        try:
            existing = store.get(task_id)
            suffix = 1
            while existing is not None and existing.state in TERMINAL:
                suffix += 1
                chosen = f"{task_id}~{suffix}"
                existing = store.get(chosen)
        except TaskStoreUnavailable as exc:
            raise RunnerAdmissionRefused(str(exc)) from exc
        if existing is not None:
            if existing.state in CLAIMABLE or existing.state in WAITING:
                return existing
            if existing.lease_owner is None and existing.state in (STARTING, RUNNING):
                # Left active by a dead incarnation and never reconciled; the
                # adopter would move it, but a caller re-accepting it now is
                # the same act: make it claimable and hand it back.
                try:
                    store.transition(existing.id, RECOVERING, detail={"readopted": True})
                    refreshed = store.get(existing.id)
                except TaskStoreUnavailable as exc:
                    raise RunnerAdmissionRefused(str(exc)) from exc
                return refreshed or existing
            raise RunnerAdmissionRefused(
                f"row {existing.id} is {existing.state} under another owner; not re-accepted"
            )
        record = TaskRecord(
            id=chosen,
            kind=kind,
            session_key=str(session_key or ""),
            parent_id=parent_id,
            root_id=root_id,
            provider=provider,
            params=body,
            workspace=workspace,
            scope_ref=dict(scope_ref or {}),
            side_effect_class=side_effect_class,
            deadline_at=deadline_at,
        )
        try:
            store.accept_one(record)
        except TaskStoreUnavailable as exc:
            raise RunnerAdmissionRefused(str(exc)) from exc
        return record

    # -- admit ---------------------------------------------------------------

    async def admit(
        self,
        task_id: str,
        *,
        kind: str = KIND_TASKRUNNER_STEP,
        lane: str = LANE_SYSTEM,
        session_key: str = "",
    ) -> Admitted:
        """Bring *task_id* to ``starting`` under a lease, or raise.

        Order (the subagent path's, minus the policy refusals that happened at
        ``accept``): memory posture -> DEFER (row stays queued, eligible again
        after ``admit_wait_secs``) -> a lane slot by effective cap -> ``claim``.
        A row cancelled while it waited raises :class:`RunnerTaskCancelled`.
        Without a store the lane slot is the whole admission.
        """
        self._refuse_if_store_required()
        store = self.store
        while True:
            if store is not None:
                self._raise_if_ended(store, task_id)
            if self._pressure is not None:
                decision = self._pressure()
                if not getattr(decision, "admitted", True):
                    self._deferred += 1
                    reason = str(getattr(decision, "reason", "") or "memory pressure")
                    if store is not None:
                        try:
                            store.defer(task_id, self.now() + self._admit_wait, reason=reason)
                        except TaskStoreUnavailable:
                            logger.debug("taskq runner: defer of %s failed", task_id, exc_info=True)
                    logger.info("taskq runner: %s deferred (%s)", task_id, reason)
                    await self._sleep(min(self._admit_wait, _MAX_ADMIT_SLEEP_SECS))
                    continue
            await self.lane.acquire(task_id)
            if store is None:
                return Admitted(
                    self, task_id, kind, lane, 0, session_key=session_key, _state=STARTING
                )
            try:
                claimed = store.claim(task_id)
            except TaskStoreUnavailable as exc:
                self.lane.release()
                raise RunnerAdmissionRefused(str(exc)) from exc
            if claimed is None:
                self.lane.release()
                rec = store.get(task_id)
                if rec is None or rec.state in TERMINAL:
                    raise RunnerTaskCancelled(
                        f"{task_id} is {rec.state if rec else 'missing'}; not started"
                    )
                if rec.state in CLAIMABLE:
                    # Deferred, or in a recovery backoff: wait it out, then retry.
                    wait = max(0.0, float(rec.next_run_at or 0.0) - self.now())
                    await self._sleep(min(wait or self._admit_wait, _MAX_ADMIT_SLEEP_SECS))
                    continue
                raise RunnerTaskCancelled(f"{task_id} is {rec.state} under another owner")
            rec = claimed.record
            handle = Admitted(
                self,
                task_id,
                rec.kind,
                str(rec.params.get(PARAM_LANE) or lane),
                claimed.generation,
                session_key=rec.session_key or session_key,
                side_effect_class=rec.side_effect_class,
                safe_retry=bool(rec.params.get(PARAM_SAFE_RETRY, False)),
            )
            handle._write(STARTING)
            return handle

    def claim_only(
        self, task_id: str, *, kind: str = KIND_TASKRUNNER_STEP, lane: str = LANE_SYSTEM
    ) -> Admitted | None:
        """Claim a row WITHOUT taking a lane slot -- for a container row.

        A TaskRunner run row groups its steps (parentage, one resume unit) but
        executes nothing itself; its steps are what the lane meters. ``None``
        when the row is not claimable (cancelled, owned elsewhere) or when no
        store is attached.
        """
        store = self.store
        if store is None:
            return None
        try:
            claimed = store.claim(task_id)
        except TaskStoreUnavailable:
            return None
        if claimed is None:
            return None
        rec = claimed.record
        handle = Admitted(
            self,
            task_id,
            rec.kind or kind,
            str(rec.params.get(PARAM_LANE) or lane),
            claimed.generation,
            session_key=rec.session_key,
            side_effect_class=rec.side_effect_class,
            safe_retry=bool(rec.params.get(PARAM_SAFE_RETRY, False)),
            _slot_held=False,
        )
        handle._write(STARTING)
        return handle

    @staticmethod
    def _raise_if_ended(store: TaskStore, task_id: str) -> None:
        try:
            state = store.state_of(task_id)
        except TaskStoreUnavailable:
            return
        if state is not None and state in TERMINAL:
            raise RunnerTaskCancelled(f"{task_id} is {state}; not started")

    # -- stop-reason recovery ------------------------------------------------

    def decide_recovery(
        self, handle: Admitted, *, unit: str, reason: str
    ) -> RecoveryDecision | None:
        """Consult the recovery ladder (L3: the ACP runtime) for one stalled turn.

        ``None`` when no ladder is attached: the caller keeps its own bounded
        retry. A ``retry`` decision carries the jittered delay; anything else
        means the layer is exhausted and the step fails with its partial kept.
        """
        if self._ladder is None:
            return None
        return self._ladder.observe_failure(
            L3_ACP_RUNTIME, unit, reason=reason, task_id=handle.task_id
        )

    def recovered(self, unit: str) -> None:
        if self._ladder is not None:
            self._ladder.observe_success(L3_ACP_RUNTIME, unit)

    # -- waits ---------------------------------------------------------------

    def _wake_event(self, task_id: str) -> asyncio.Event:
        ev = self._wakes.get(task_id)
        if ev is None:
            ev = asyncio.Event()
            self._wakes[task_id] = ev
        return ev

    def on_wake(self, task_id: str) -> None:
        """Coordinator / ledger hook: the row's wait ended; resume its coroutine."""
        ev = self._wakes.get(task_id)
        if ev is not None:
            ev.set()

    def signal(self, scope: str, *, reason: str = "") -> list[str]:
        """A dependency recovered: wake every row waiting on *scope*."""
        store = self.store
        woken: list[str] = []
        if self._coordinator is not None and self._coordinator.recovered(scope):
            woken.extend(self._coordinator.tick())
        elif store is not None:
            try:
                woken.extend(WaitLedger(store, clock=self._clock).signal(scope, reason=reason))
            except TaskStoreUnavailable:
                logger.debug("taskq runner: signal(%s) failed", scope, exc_info=True)
        for task_id in woken:
            self.on_wake(task_id)
        return woken

    def tick(self) -> list[str]:
        """Wake due time-based waits (the coordinator's, or the ledger's) and
        replay terminal writes the store refused earlier."""
        self.retry_terminal_writes()
        woken: list[str] = []
        if self._coordinator is not None:
            woken.extend(self._coordinator.tick())
        store = self.store
        if store is not None and self._coordinator is None:
            ledger = WaitLedger(store, clock=self._clock)
            try:
                due = ledger.due_dependency_waits(self.now())
            except TaskStoreUnavailable:
                due = []
            for rec in due:
                try:
                    if ledger.wake(rec.id, reason="retry_at reached") is not None:
                        woken.append(rec.id)
                except TaskStoreUnavailable:
                    continue
        for task_id in woken:
            self.on_wake(task_id)
        return woken

    async def yield_dependency(
        self,
        handle: Admitted,
        signal: DependencySignal,
        *,
        deadline_at: float | None = None,
    ) -> bool:
        """Park *handle* until its dependency scope is retried; True to retry.

        The lane slot is released for the whole wait (the session stays
        resident -- the residency charge is the record's statement of that).
        With a coordinator attached the scope's ONE schedule owns the retry
        instant; without one the row carries ``retry_at`` and :meth:`tick`
        wakes it. A terminal signal, a scope deadline or a cancel returns
        False: the row is already ``failed``/``cancelled`` and the step must
        not retry.
        """
        store = self.store
        handle.release_slot()
        ev = self._wake_event(handle.task_id)
        ev.clear()
        parked = True
        if self._coordinator is not None:
            verdict = self._coordinator.report(
                handle.task_id, signal, generation=handle.generation, from_state=RUNNING
            )
            if verdict.outcome != "wait":
                self.forget(handle.task_id)
                await handle.reacquire_slot()
                return False
        elif store is not None:
            record = WaitRecord.dependency(
                signal.dependency_scope,
                since=self.now(),
                retry_at=signal.retry_at,
                reason=signal.detail or f"dependency {signal.dependency_scope} {signal.kind}",
                deadline_at=deadline_at,
            )
            try:
                parked = store.enter_wait(
                    handle.task_id, record.to_dict(), generation=handle.generation
                )
            except TaskStoreUnavailable:
                parked = False
        if store is None or not parked:
            # Nothing persisted: honour the signal's own retry instant locally.
            wait = max(0.0, float(signal.retry_at or 0.0) - self.now()) if store is None else 0.0
            if signal.retry_at is not None:
                await self._sleep(wait)
            await handle.reacquire_slot()
            return signal.retryable
        handle._state = WAITING_DEPENDENCY
        await ev.wait()
        return await self._resume_after_wait(handle)

    async def _resume_after_wait(self, handle: Admitted) -> bool:
        store = self.store
        if store is None:
            await handle.reacquire_slot()
            return True
        rec = store.get(handle.task_id)
        if rec is None or rec.state in TERMINAL:
            self.forget(handle.task_id)
            return False
        # Re-admission goes through capacity like any other start.
        await handle.reacquire_slot()
        if rec.state == QUEUED or rec.state in CLAIMABLE:
            # Parked in retry_wait / queued: take it back under a new claim.
            handle.release_slot()
            fresh = await self.admit(handle.task_id, kind=handle.kind, lane=handle.lane)
            handle.generation = fresh.generation
            handle._slot_held = True
            handle.running()
            return True
        handle.generation = rec.generation
        handle._state = rec.state
        return rec.state == RUNNING

    async def waiting_input(
        self,
        handle: Admitted,
        *,
        tool_call_id: str,
        reason: str = "",
        deadline_at: float | None = None,
    ) -> str | None:
        """Park *handle* in ``waiting_input`` until :meth:`answer_input`.

        Returns the answer text, or ``None`` when the wait ended without one
        (cancelled, failed, deadline) -- the caller then treats the step as
        not completed. The lane slot is released for the wait.
        """
        store = self.store
        handle.release_slot()
        ev = self._wake_event(handle.task_id)
        ev.clear()
        self._input_answers.pop(handle.task_id, None)
        if store is not None:
            record = WaitRecord.input(
                tool_call_id,
                since=self.now(),
                reason=reason or "a command run by this step is waiting for input",
                deadline_at=deadline_at,
            )
            try:
                store.enter_wait(handle.task_id, record.to_dict(), generation=handle.generation)
            except TaskStoreUnavailable:
                logger.debug("taskq runner: input wait write failed", exc_info=True)
        handle._state = WAITING_INPUT
        await ev.wait()
        answer = self._input_answers.pop(handle.task_id, None)
        if answer is None:
            # Woken without an answer in this process's memory (answered
            # through another gateway instance, or after a rebuild): the
            # answer is on the persisted wake event.
            answer = self.recorded_answer(handle.task_id)
        if not await self._resume_after_wait(handle):
            return None
        return answer

    def recorded_answer(self, task_id: str) -> str | None:
        """The operator answer persisted on the row's latest ``wake`` event, if
        the step has not consumed it yet.

        ``answer_input`` writes the answer INTO the wake event before waking
        anything, so a crash between the answer and the re-admission keeps
        it: a rebuilt step (a new incarnation re-dispatching the ``retry_wait``
        row) reads it from here instead of from the RAM the crash lost. An
        answer is consumed once the step records it (:meth:`consume_answer`,
        an ``input_consumed`` event); a wake older than that marker is not an
        answer for the current turn.
        """
        store = self.store
        if store is None:
            return None
        try:
            events = store.events(task_id)
        except TaskStoreUnavailable:
            return None
        for event in reversed(events):
            data = event.data if isinstance(event.data, dict) else {}
            if event.kind == "input_consumed":
                return None
            if event.kind == "wake" and isinstance(data.get("answer"), str):
                return str(data["answer"])
        return None

    def consume_answer(self, task_id: str, question_id: str) -> bool:
        """Record that the step took the persisted answer into its turn, so a
        later rebuild does not replay it."""
        store = self.store
        if store is None:
            return True
        try:
            store.append_event(task_id, "input_consumed", {"question_id": str(question_id)[:200]})
        except TaskStoreUnavailable:
            return False
        return True

    def answer_input(self, task_id: str, answer: str) -> bool:
        """Deliver the user's answer to a row in ``waiting_input``; wakes it."""
        store = self.store
        woke = True
        if store is not None:
            try:
                # The answer is recorded on the wake event, so a crash between
                # this write and the re-admission does not lose it; the row is
                # claimable (``retry_wait``) until the lane is granted again.
                woke = (
                    WaitLedger(store, clock=self._clock).wake(
                        task_id, reason="input answered", detail={"answer": str(answer)[:2000]}
                    )
                    is not None
                )
            except TaskStoreUnavailable:
                woke = False
        if not woke:
            return False
        self._input_answers[task_id] = str(answer)
        self.on_wake(task_id)
        return True

    def cancel_wait(self, task_id: str, *, reason: str = "cancelled") -> bool:
        """End a wait without an answer: the row is cancelled, the coroutine resumes."""
        store = self.store
        if store is not None:
            try:
                store.cancel(task_id, reason=reason)
            except TaskStoreUnavailable:
                return False
        self.on_wake(task_id)
        return True


# ── recovery adapter (adopt rows the reconciler could not settle) ─────────────


@dataclass
class AdoptReport:
    examined: int = 0
    resumed: list[str] = field(default_factory=list)
    unknown_side_effect: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)


def is_safe_retry(rec: TaskRecord) -> bool:
    """Whether the interrupted step may be re-run from the runner's checkpoint.

    True when the row says so (``params.safe_retry``) or when its side-effect
    class is one the reconciler would itself re-dispatch (``none`` /
    ``idempotent_key``). Class ``unknown`` with no explicit flag is NOT safe:
    an external operation may have happened.
    """
    if bool(rec.params.get(PARAM_SAFE_RETRY, False)):
        return True
    return rec.side_effect_class in (SIDE_EFFECT_NONE, SIDE_EFFECT_IDEMPOTENT_KEY)


def adopt_orphaned_rows(
    store: TaskStore,
    *,
    kinds: Iterable[str] = RUNNER_RECOVERY_ADAPTERS,
    resume: Callable[[TaskRecord], bool] | None = None,
    now: float | None = None,
) -> AdoptReport:
    """Settle every runner row a dead incarnation left behind.

    The boot reconciler leaves a kind it has no adapter for untouched except
    for dropping the lease and stamping ``awaiting_adapter``; legacy
    ``runs.json`` imports arrive as ``recovering``. This is that adapter:

    * an unleased ACTIVE row (``starting``/``running``/``waiting_*``) or a
      ``recovering`` row whose step is :func:`is_safe_retry` is handed to
      *resume* (the runner's checkpoint restart); a resume that returns True
      leaves the row claimable for the runner's own ``admit``;
    * one that is NOT safe to retry becomes ``unknown_side_effect`` -- the
      run stays paused for a human, and nothing is re-run blind;
    * a row still leased by THIS incarnation is live and skipped.
    """
    ts = store.now() if now is None else now
    wanted = frozenset(kinds)
    report = AdoptReport()
    try:
        rows = [r for r in store.active_rows() if r.kind in wanted]
    except TaskStoreUnavailable:
        return report
    for rec in rows:
        report.examined += 1
        if (
            rec.lease_owner == store.incarnation
            and rec.lease_expires_at
            and (rec.lease_expires_at > ts)
        ):
            report.skipped.append(rec.id)
            continue
        if rec.parent_id:
            # A step row: its run row decides whether the run resumes. The
            # interrupted step itself is settled here -- a safe-retry step is
            # ``failed`` (the run's resume re-runs it under a fresh row from
            # the checkpoint), an unsafe one is ``unknown_side_effect``.
            if is_safe_retry(rec):
                if store.finish(
                    rec.id, FAILED, error="interrupted by a gateway restart; re-run on resume"
                ):
                    report.failed.append(rec.id)
            elif store.transition(rec.id, UNKNOWN_SIDE_EFFECT, detail={"adopted": "orphan_step"}):
                report.unknown_side_effect.append(rec.id)
            continue
        if not is_safe_retry(rec):
            if store.transition(rec.id, UNKNOWN_SIDE_EFFECT, detail={"adopted": "not_safe_retry"}):
                report.unknown_side_effect.append(rec.id)
            continue
        if rec.state == ADMITTED:
            # Claimed, never started: back to the queue, as the reconciler does.
            if not store.transition(rec.id, QUEUED, detail={"adopted": "lost_owner"}):
                report.skipped.append(rec.id)
                continue
        elif rec.state != RECOVERING:
            if not store.transition(rec.id, RECOVERING, detail={"adopted": "lost_owner"}):
                report.skipped.append(rec.id)
                continue
        elif rec.lease_owner is not None:
            store.release_lease(rec.id)
        if resume is None:
            report.skipped.append(rec.id)
            continue
        try:
            ok = bool(resume(rec))
        except Exception as exc:  # noqa: BLE001 - one bad row must not stop the sweep
            logger.warning("taskq runner: resume of %s failed: %s", rec.id, exc)
            store.finish(rec.id, FAILED, error=f"resume failed: {exc}")
            report.failed.append(rec.id)
            continue
        if ok:
            report.resumed.append(rec.id)
        else:
            store.finish(
                rec.id,
                FAILED,
                error="not resumed from this row; the owning run restarts through its own registry",
            )
            report.failed.append(rec.id)
    if report.examined:
        logger.info(
            "taskq runner adopt: examined=%d resumed=%d unknown_side_effect=%d failed=%d skipped=%d",
            report.examined,
            len(report.resumed),
            len(report.unknown_side_effect),
            len(report.failed),
            len(report.skipped),
        )
    return report


# ── factory ───────────────────────────────────────────────────────────────────


def runner_admission_for(
    manager: Any,
    *,
    cfg: Any = None,
    ladder: RecoveryLadder | None = None,
    coordinator: DependencyCoordinator | None = None,
    pressure: Callable[[], Any] | None = None,
) -> RunnerAdmission:
    """Build the runner admission over a ``SubagentManager``'s store and cap.

    The lane's ceiling is the manager's ``max_concurrent`` -- already the
    adaptive controller's effective cap clamped under the user's ceiling --
    so one controller decision moves the subagent queue and the runner lane
    together. ``agent.adaptive_concurrency_mode`` read live from *cfg*
    selects ``fixed``; ``agent.admit_wait_secs`` is the defer interval.
    """

    def _store() -> TaskStore | None:
        return getattr(manager, "_taskq", None)

    def _cap() -> int:
        return int(getattr(manager, "max_concurrent", 4) or 0)

    def _mode() -> str:
        agent = getattr(cfg, "agent", None)
        return str(getattr(agent, "adaptive_concurrency_mode", MODE_AIMD) or MODE_AIMD)

    agent_cfg = getattr(cfg, "agent", None)
    wait = float(getattr(agent_cfg, "admit_wait_secs", _DEFAULT_ADMIT_WAIT_SECS) or 0.0)
    if pressure is None:
        try:
            from kiro_crew.resource_status import cached_admission_check

            pressure = cached_admission_check
        except Exception:  # noqa: BLE001 - the probe is optional
            pressure = None

    def _store_error() -> str | None:
        err = getattr(manager, "_taskq_unavailable", None)
        return str(err) if err else None

    return RunnerAdmission(
        _store,
        lane=RunnerLane(_cap, mode=_mode, name="runner-lane"),
        pressure=pressure,
        admit_wait_secs=wait or _DEFAULT_ADMIT_WAIT_SECS,
        require_store=bool(getattr(agent_cfg, "task_queue_enabled", True)) if agent_cfg else False,
        store_error=_store_error,
        ladder=ladder,
        coordinator=coordinator,
    )


__all__ = [
    "LANE_SYSTEM",
    "MODE_AIMD",
    "MODE_FIXED",
    "PARAM_LANE",
    "PARAM_SAFE_RETRY",
    "RUNNER_RECOVERY_ADAPTERS",
    "SYSTEM_SOURCES",
    "WORKFLOW_ID_PREFIX",
    "AdoptReport",
    "Admitted",
    "RunnerAdmission",
    "RunnerAdmissionRefused",
    "RunnerLane",
    "RunnerTaskCancelled",
    "adopt_orphaned_rows",
    "is_safe_retry",
    "lane_for",
    "run_task_id",
    "runner_admission_for",
    "step_task_id",
    "workflow_task_id",
]
