"""Waits: yield the lane slot, keep the residency, resume through admission; child registration and deadlines."""

from __future__ import annotations

import asyncio as _asyncio
import logging as _logging
import time as _time
from typing import TYPE_CHECKING, Any

from kiro_crew import taskq as _taskq
from kiro_crew.taskq import waits as _waits

from .._component import ManagerComponent

_glue_logger = _logging.getLogger("kiro_crew.subagent_manager.admission")

if TYPE_CHECKING:
    pass

    from ...subagent import SubagentInfo, asyncio, time


class _WaitsMixin(ManagerComponent):
    __slots__ = ()

    if TYPE_CHECKING:
        # Sibling-mixin methods this module reaches through ``self``; typing only.
        def _maybe_pause_parent(
            self, info: "SubagentInfo", record: "_taskq.WaitRecord"
        ) -> None: ...

        def _maybe_resume_paused_parent(self, info: "SubagentInfo") -> None: ...

        def _post_store_write(
            self, store: "_taskq.TaskStore", what: str, fn: Any, *args: Any, **kw: Any
        ) -> "_asyncio.Task[Any] | None": ...

        def taskq_store(self) -> "_taskq.TaskStore | None": ...

    # ── waits: yield the lane slot, keep the residency, resume through admission ──
    #
    # Addendum §1-3 / RFC §14. A live run that cannot progress (children still
    # running, dependency cooling down, a command wanting real input, an
    # approval pending) yields its LANE slot here so the pump can start queued
    # work, while its runtime stays resident and charged. Waking re-enters the
    # same pump as a ``_resume_id`` entry, so a wake never bypasses capacity.

    @staticmethod
    def taskq_parent_id_for(parent_session_key: str | None) -> str | None:
        """The task id of the subagent whose session spawned this one, if any."""
        key = str(parent_session_key or "")
        prefix = "subagent:"
        if not key.startswith(prefix):
            return None
        return key[len(prefix) :] or None

    def taskq_ledger(self) -> "_taskq.WaitLedger | None":
        store = self.taskq_store()
        return _taskq.WaitLedger(store) if store is not None else None

    def yield_slot(
        self, info: SubagentInfo, record: "_taskq.WaitRecord", *, persist: bool = True
    ) -> bool:
        """Release *info*'s lane slot for a wait; the residency charge stays.

        Idempotent per run: a run that already yielded answers False. The wait
        record is written on the row (``running -> record.state``) under the
        run's generation; the runtime (session handle, process, FDs) is NOT
        touched -- ``residency_charged`` is the record's statement of that.
        ``persist=False`` skips that row write for a caller whose wait the
        ``DependencyCoordinator`` has already written (one wait, one write);
        the in-memory bookkeeping and the ``subagent_waiting`` event are the
        same either way.
        """
        if info.done or info._slot_released or info._resume_pending:
            return False
        released = self._manager._release_slot(info)
        if not released:
            return False
        self._manager._running_count -= 1
        record.slot_released = True
        info._wait_record = record.to_dict()
        store = self.taskq_store() if persist else None
        if store is not None:
            try:
                ok = store.enter_wait(
                    info.id, record.to_dict(), generation=info._taskq_generation or None
                )
                if not ok:
                    _glue_logger.debug("taskq: wait write for %s refused", info.id)
            except _taskq.TaskStoreUnavailable:
                _glue_logger.debug("taskq: wait write for %s failed", info.id, exc_info=True)
        try:
            _asyncio.get_event_loop().create_task(
                self._manager._fire_event(
                    "subagent_waiting",
                    info,
                    {
                        "state": record.state,
                        "reason": record.reason,
                        "resume": record.resume_condition.kind,
                        "residency_charged": record.residency_charged,
                    },
                )
            )
        except RuntimeError:
            pass
        _glue_logger.info(
            "Subagent %s: %s (%s) -- lane slot yielded, residency kept",
            info.id,
            record.state,
            record.reason,
        )
        self._maybe_pause_parent(info, record)
        self._manager._drain_queue()
        return True

    def request_resume(self, info: SubagentInfo, *, reason: str = "") -> bool:
        """Queue a resume entry for a yielded run; the pump grants it by capacity.

        Resume entries go to the FRONT of the window: the run is already
        resident, so granting it first shortens the interval in which a woken
        run continues without a slot. It still waits for a free slot like any
        other start, but not for the spawn stagger -- a resume starts no
        process, so there is no burst to smooth.
        """
        if info.done or not info._slot_released or info._resume_pending:
            return False
        info._resume_pending = True
        self._manager._queue.insert(
            0,
            {
                "_resume_id": info.id,
                "_preassigned_id": info.id,
                "parent_session_key": info.parent_session_key,
                "batch_id": info.batch_id,
                "reason": reason,
            },
        )
        self._manager._drain_queue()
        return True

    def resume_grant(self, params: dict[str, Any]) -> bool:
        """The pump popped a resume entry: hand the slot back to the run.

        Re-admission is a NEW generation in the store (``wake_wait``), so any
        callback issued during the wait is fenced out. A run that ended while
        it waited (cancelled, reaped) takes nothing.
        """
        agent_id = str(params.get("_resume_id") or "")
        info = self._manager._agents.get(agent_id)
        if info is None or info.done or info.reaped or info.user_stopped:
            if info is not None:
                info._resume_pending = False
            return False
        self._manager._running_count += 1
        # Not a process start: the spawn stagger (``_last_spawn_ts``) is left
        # alone so a resume never delays the next real start.
        info._slot_released = False
        info._resume_pending = False
        info._wait_record = None
        store = self.taskq_store()
        if store is not None:
            try:
                # The slot is granted and the runtime is resident: this is
                # the ONE wake that may write ``running`` directly.
                new_gen = store.wake_wait(
                    info.id,
                    reason=str(params.get("reason") or "resumed through admission"),
                    generation=info._taskq_generation or None,
                    to=_taskq.RUNNING,
                )
            except _taskq.TaskStoreUnavailable:
                new_gen = None
            if new_gen is not None:
                info._taskq_generation = new_gen
        event = getattr(info, "_resume_event", None)
        if event is not None:
            event.set()
            # One event per wait: a later yield arms a fresh one, so a holder
            # that arrives after this grant cannot read a stale set().
            info._resume_event = None
        self._maybe_resume_paused_parent(info)
        try:
            _asyncio.get_event_loop().create_task(
                self._manager._fire_event(
                    "subagent_resumed", info, {"generation": info._taskq_generation}
                )
            )
        except RuntimeError:
            pass
        _glue_logger.info("Subagent %s: resumed (slot re-admitted)", agent_id)
        return True

    def resume_granted(self, agent_id: str) -> bool:
        """Whether a yielded run holds its slot again (for a caller holding a tool result)."""
        info = self._manager._agents.get(agent_id)
        return info is not None and not info._slot_released and not info._resume_pending

    def taskq_child_registered(self, child: SubagentInfo) -> None:
        """A child started or queued under a live subagent parent.

        The parent enters ``waiting_children`` only when the EXECUTION LAYER
        shows it blocked on its children: its in-flight tool is the blocking
        ``spawn_sub_agents`` (trusted ``_meta.kiro`` tool name, never model
        text). A parent that used the non-blocking ``spawn_run`` keeps
        running and keeps its slot -- it has work of its own.
        """
        parent_id = self.taskq_parent_id_for(child.parent_session_key)
        if not parent_id:
            return
        parent = self._manager._agents.get(parent_id)
        if parent is None or parent.done:
            return
        inflight = getattr(parent, "_inflight_tool", None)
        tool_name = str(getattr(inflight, "tool_name", "") or "")
        if not tool_name.endswith("spawn_sub_agents"):
            return
        outstanding = self.taskq_outstanding_children(parent_id)
        if child.id not in outstanding:
            outstanding.append(child.id)
        current = _taskq.WaitRecord.from_dict(parent._wait_record)
        if parent._slot_released:
            # Already waiting: a later child of the same blocking call joins
            # the awaited set, so the wake stays "on the LAST child".
            if current is None or current.state != _taskq.WAITING_CHILDREN:
                return
            merged = list(current.resume_condition.ids)
            merged.extend(i for i in outstanding if i not in merged)
            current.resume_condition.ids = merged
            parent._wait_record = current.to_dict()
            store = self.taskq_store()
            if store is not None:
                try:
                    store.update_wait(
                        parent_id, current.to_dict(), generation=parent._taskq_generation or None
                    )
                except _taskq.TaskStoreUnavailable:
                    _glue_logger.debug("taskq: wait update for %s failed", parent_id, exc_info=True)
            return
        record = _taskq.WaitRecord.children(
            outstanding,
            since=self._store_now(),
            tool_call_id=str(getattr(inflight, "title", "") or ""),
            deadline_at=self.taskq_deadline_of(parent_id),
        )
        self.yield_slot(parent, record)

    async def taskq_child_registered_async(self, child: SubagentInfo) -> None:
        """:meth:`taskq_child_registered` for event-loop callers: the store
        reads (the ledger's outstanding children, the parent's deadline) run
        on the writer thread and the wait writes (``update_wait`` /
        ``enter_wait``) are posted there; the parent's slot yield and the
        in-memory bookkeeping stay on the loop."""
        parent_id = self.taskq_parent_id_for(child.parent_session_key)
        if not parent_id:
            return
        parent = self._manager._agents.get(parent_id)
        if parent is None or parent.done:
            return
        inflight = getattr(parent, "_inflight_tool", None)
        tool_name = str(getattr(inflight, "tool_name", "") or "")
        if not tool_name.endswith("spawn_sub_agents"):
            return
        store = self.taskq_store()
        ledger_ids: list[str] = []
        deadline: float | None = None
        if store is not None:
            try:
                ledger_ids, deadline = await store.run(
                    self._child_registration_reads, store, parent_id
                )
            except _taskq.TaskStoreUnavailable:
                pass
        outstanding = self._merge_outstanding_children(parent_id, ledger_ids)
        if child.id not in outstanding:
            outstanding.append(child.id)
        current = _taskq.WaitRecord.from_dict(parent._wait_record)
        if parent._slot_released:
            if current is None or current.state != _taskq.WAITING_CHILDREN:
                return
            merged = list(current.resume_condition.ids)
            merged.extend(i for i in outstanding if i not in merged)
            current.resume_condition.ids = merged
            parent._wait_record = current.to_dict()
            if store is not None:
                self._post_store_write(
                    store,
                    f"wait update {parent_id}",
                    store.update_wait,
                    parent_id,
                    current.to_dict(),
                    generation=parent._taskq_generation or None,
                )
            return
        record = _taskq.WaitRecord.children(
            outstanding,
            since=self._store_now(),
            tool_call_id=str(getattr(inflight, "title", "") or ""),
            deadline_at=deadline,
        )
        # The yield itself is loop bookkeeping; its row write is posted.
        if self.yield_slot(parent, record, persist=False) and store is not None:
            self._post_store_write(
                store,
                f"wait write {parent_id}",
                store.enter_wait,
                parent_id,
                record.to_dict(),
                generation=parent._taskq_generation or None,
            )

    @staticmethod
    def _child_registration_reads(
        store: "_taskq.TaskStore", parent_id: str
    ) -> tuple[list[str], float | None]:
        """Store half of the W3 branch: outstanding children + the parent's deadline."""
        ids = list(_taskq.WaitLedger(store).outstanding_children(parent_id))
        rec = store.get(parent_id)
        return ids, (rec.deadline_at if rec is not None else None)

    def _merge_outstanding_children(self, parent_id: str, ids: list[str]) -> list[str]:
        out = list(ids)
        key = f"subagent:{parent_id}"
        for info in self._manager._agents.values():
            if info.parent_session_key == key and not info.done and info.id not in out:
                out.append(info.id)
        return out

    def taskq_outstanding_children(self, parent_id: str) -> list[str]:
        """Non-terminal children of *parent_id*: store rows plus live in-memory runs."""
        ids: list[str] = []
        ledger = self.taskq_ledger()
        if ledger is not None:
            try:
                ids.extend(ledger.outstanding_children(parent_id))
            except _taskq.TaskStoreUnavailable:
                pass
        key = f"subagent:{parent_id}"
        for info in self._manager._agents.values():
            if info.parent_session_key == key and not info.done and info.id not in ids:
                ids.append(info.id)
        return ids

    def taskq_deadline_of(self, agent_id: str) -> float | None:
        store = self.taskq_store()
        if store is None:
            return None
        try:
            rec = store.get(agent_id)
        except _taskq.TaskStoreUnavailable:
            return None
        return rec.deadline_at if rec is not None else None

    def _store_now(self) -> float:
        store = self.taskq_store()
        return store.now() if store is not None else _time.time()

    def taskq_child_terminal(self, child: SubagentInfo, state: str) -> None:
        """Propagate a child's terminal state to its parent (after the store write).

        ``continue`` (default): the parent wakes when its LAST awaited child
        ends. ``fail_parent``: the parent fails now and its other children are
        cancelled, children first. Completed siblings are untouched either way.
        """
        ledger = self.taskq_ledger()
        parent_id = self.taskq_parent_id_for(child.parent_session_key)
        if ledger is None or not parent_id:
            return
        parent = self._manager._agents.get(parent_id)
        # A LIVE parent (resident runtime, lane slot yielded) is re-admitted
        # through the pump: its row stays ``waiting_children`` until
        # ``resume_grant`` writes ``running`` with the slot actually granted.
        # Without a live parent the ledger wakes the row to a claimable
        # ``retry_wait`` and the dispatcher re-dispatches it.
        live_parent = parent is not None and not parent.done and parent._slot_released
        try:
            outcome = ledger.on_child_terminal(child.id, state, defer_wake=live_parent)
        except _taskq.TaskStoreUnavailable:
            _glue_logger.debug("taskq: child propagation for %s failed", child.id, exc_info=True)
            return
        if outcome.fail_parent:
            for sibling in outcome.cancel_siblings:
                self._cancel_live_or_row(sibling, reason=_waits.WAIT_REASON_CHILD_FAILED)
            if parent is not None and not parent.done:
                parent.error = parent.error or (
                    f"child {child.id} {state} (on_child_failure=fail_parent)"
                )
                self._schedule_cancel(parent_id)
            return
        if outcome.wake_parent and parent is not None:
            if live_parent:
                # The row is still ``waiting_children``; the pump's grant is
                # what writes ``running`` (``resume_grant`` -> ``wake_wait``).
                self.request_resume(parent, reason=f"last awaited child {child.id} {state}")
                return
            # The ledger already moved the row to a claimable ``retry_wait``
            # under a new generation; adopt it so a late write is not fenced
            # as stale, and let the pump re-dispatch.
            store = self.taskq_store()
            if store is not None:
                try:
                    rec = store.get(parent_id)
                except _taskq.TaskStoreUnavailable:
                    rec = None
                if rec is not None:
                    parent._taskq_generation = rec.generation
            self.request_resume(parent, reason=f"last awaited child {child.id} {state}")

    def _cancel_live_or_row(self, agent_id: str, *, reason: str) -> None:
        info = self._manager._agents.get(agent_id)
        if info is not None and not info.done:
            self._schedule_cancel(agent_id)
            return
        ledger = self.taskq_ledger()
        if ledger is not None:
            try:
                ledger.cancel_tree(agent_id, reason=reason)
            except _taskq.TaskStoreUnavailable:
                pass

    def _schedule_cancel(self, agent_id: str) -> None:
        try:
            _asyncio.get_event_loop().create_task(self._manager.cancel(agent_id))
        except RuntimeError:
            pass

    def taskq_cancel_children_of(self, agent_id: str, *, reason: str) -> list[str]:
        """Cancel-tree for a parent being cancelled: store rows first, live runs scheduled."""
        ledger = self.taskq_ledger()
        cancelled: list[str] = []
        if ledger is not None:
            try:
                for child in ledger.children_of(agent_id):
                    if child.terminal:
                        continue
                    live = self._manager._agents.get(child.id)
                    if live is not None and not live.done:
                        self._schedule_cancel(child.id)
                        cancelled.append(child.id)
                    else:
                        cancelled.extend(ledger.cancel_tree(child.id, reason=reason))
            except _taskq.TaskStoreUnavailable:
                pass
        return cancelled

    def taskq_expire_waits(self) -> list[str]:
        """Fail waits past their deadline and cancel the live runs they belonged to.

        Rate-limited to once per second: the pump calls this on every refill,
        and a deadline is a wall-clock fact that does not need sub-second checks.
        """
        expired = self.taskq_expire_waits_store()
        self.taskq_expire_waits_apply(expired)
        return expired

    def taskq_expire_waits_store(self) -> list[str]:
        """Store half of :meth:`taskq_expire_waits` (safe on the writer thread):
        the rate limit and the ledger sweep. Returns the expired ids."""
        ledger = self.taskq_ledger()
        if ledger is None:
            return []
        now = _time.monotonic()
        last = float(getattr(self._manager, "_taskq_last_wait_expiry", 0.0) or 0.0)
        if now - last < 1.0:
            return []
        setattr(self._manager, "_taskq_last_wait_expiry", now)
        try:
            return ledger.expire()
        except _taskq.TaskStoreUnavailable:
            return []

    def taskq_expire_waits_apply(self, expired: list[str]) -> None:
        """Loop half of :meth:`taskq_expire_waits`: cancel the live runs the
        expired waits belonged to (schedules tasks, so it runs on the loop)."""
        for agent_id in expired:
            info = self._manager._agents.get(agent_id)
            if info is not None and not info.done:
                info.error = info.error or "wait deadline passed"
                self._schedule_cancel(agent_id)
