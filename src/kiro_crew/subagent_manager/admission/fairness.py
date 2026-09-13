"""Fairness: lanes, the child reserve, the capacity view, the parent pauser (RFC section 6, Q3, Q5)."""

from __future__ import annotations

import asyncio as _asyncio
import logging as _logging
import time as _time
from typing import TYPE_CHECKING, Any, Mapping

from kiro_crew import taskq as _taskq
from kiro_crew.taskq import lanes as _lanes

from .._component import ManagerComponent
from .types import (
    FAIRNESS_SETTINGS_TTL_SECS,
    CapacityView,
    FairnessSettings,
    NoopParentPauser,
    ParentPauser,
)

_glue_logger = _logging.getLogger("kiro_crew.subagent_manager.admission")

if TYPE_CHECKING:
    pass

    from ...subagent import SubagentInfo, asyncio, time


class _FairnessMixin(ManagerComponent):
    __slots__ = ()

    if TYPE_CHECKING:
        # Sibling-mixin methods this module reaches through ``self``; typing only.
        pump_off_loop: bool

        def resume_granted(self, agent_id: str) -> bool: ...

        def taskq_excluded_ids(self) -> list[str]: ...

        def taskq_store(self) -> "_taskq.TaskStore | None": ...

    # ── fairness: lanes, the child reserve, the parent pauser (RFC §6, Q3, Q5) ──
    #
    # A lane is one root session's queue (automation roots share ``system``).
    # The pump picks the next lane by smooth weighted round-robin, so a
    # 1000-row fan-out from one session takes turns with another session's
    # single spawn and with cron/hook work instead of starving them. The
    # child reserve keeps the last slot(s) for nested rows and resuming
    # parents while a tree is in flight.

    def fairness_settings(self) -> FairnessSettings:
        """Config-backed knobs, cached on the manager for a short TTL."""
        cached = getattr(self._manager, "_fairness_settings", None)
        stamp = float(getattr(self._manager, "_fairness_settings_ts", 0.0) or 0.0)
        now = _time.monotonic()
        if isinstance(cached, FairnessSettings) and now - stamp < FAIRNESS_SETTINGS_TTL_SECS:
            return cached
        settings = cached if isinstance(cached, FairnessSettings) else FairnessSettings()
        try:
            from kiro_crew.config.loader import KiroCrewConfig

            settings = FairnessSettings.from_agent_config(KiroCrewConfig.load().agent)
        except Exception:
            _glue_logger.debug("fairness settings: config unreadable, keeping last", exc_info=True)
        setattr(self._manager, "_fairness_settings", settings)
        setattr(self._manager, "_fairness_settings_ts", now)
        return settings

    def set_fairness_settings(self, settings: FairnessSettings | None) -> None:
        """Install *settings* now (live reload, tests); None re-reads config next time."""
        if settings is None:
            setattr(self._manager, "_fairness_settings", None)
            setattr(self._manager, "_fairness_settings_ts", 0.0)
            return
        setattr(self._manager, "_fairness_settings", settings)
        # Pinned: a TTL far in the future keeps the pump from re-reading config.
        setattr(self._manager, "_fairness_settings_ts", _time.monotonic() + 1e12)
        scheduler = getattr(self._manager, "_lane_scheduler", None)
        if isinstance(scheduler, _lanes.LaneScheduler):
            scheduler.weights = settings.lane_weights

    def lane_scheduler(self) -> "_lanes.LaneScheduler":
        """The manager's one weighted round-robin state, shared by refill and pick."""
        scheduler = getattr(self._manager, "_lane_scheduler", None)
        settings = self.fairness_settings()
        if not isinstance(scheduler, _lanes.LaneScheduler):
            scheduler = _lanes.LaneScheduler(weights=settings.lane_weights)
            setattr(self._manager, "_lane_scheduler", scheduler)
        else:
            scheduler.weights = settings.lane_weights
        return scheduler

    def lane_for_session(self, session_key: str | None) -> str:
        """The lane a spawn from *session_key* is dispatched under.

        A ``subagent:<id>`` key resolves up the live parent chain to the root
        session (the store row is consulted for a parent that is not live);
        any other key is a root and maps by :func:`lanes.lane_key_for`.
        """
        key = str(session_key or "")
        seen: set[str] = set()
        while key.startswith(_lanes.SUBAGENT_PREFIX) and key not in seen:
            seen.add(key)
            parent_id = key[len(_lanes.SUBAGENT_PREFIX) :]
            live = self._manager._agents.get(parent_id)
            if live is not None:
                key = str(live.parent_session_key or "")
                continue
            store = self.taskq_store()
            rec = None
            if store is not None:
                try:
                    rec = store.get(parent_id)
                except _taskq.TaskStoreUnavailable:
                    rec = None
            if rec is None:
                return key
            if rec.lane:
                return rec.lane
            key = rec.session_key
        return _lanes.lane_key_for(key)

    def lane_of_entry(self, params: Mapping[str, Any]) -> str:
        lane = str(params.get("_lane") or "")
        if lane:
            return lane
        return self.lane_for_session(str(params.get("parent_session_key") or ""))

    @staticmethod
    def entry_is_child(params: Mapping[str, Any]) -> bool:
        return str(params.get("parent_session_key") or "").startswith(_lanes.SUBAGENT_PREFIX)

    def waiting_parents(self) -> list["SubagentInfo"]:
        """Live runs that yielded their slot to wait on children."""
        out = []
        for info in self._manager._agents.values():
            if info.done or not info._slot_released:
                continue
            record = info._wait_record if isinstance(info._wait_record, dict) else None
            if record is not None and record.get("state") == _taskq.WAITING_CHILDREN:
                out.append(info)
        return out

    def pending_children(self) -> int:
        """Nested rows waiting to start, in the window or store-only.

        The store half is a read the capacity snapshot needs synchronously.
        On the event loop (off-loop pump on) it comes from the cache the
        coroutine pump refreshes on the writer thread at the start of every
        pass (:meth:`refresh_pending_children_async`); inline callers read
        the store directly.
        """
        count = sum(1 for p in self._manager._queue if self.entry_is_child(p))
        store = self.taskq_store()
        # A nested row has a live parent (orphans are cancelled at boot), so
        # with no live run at all the store cannot hold one: skip the query.
        any_live = any(not info.done for info in self._manager._agents.values())
        if store is None or not any_live:
            return count
        try:
            _asyncio.get_running_loop()
            on_loop = True
        except RuntimeError:
            on_loop = False
        if on_loop and type(self).pump_off_loop:
            return count + int(getattr(self._manager, "_pending_children_store", 0) or 0)
        return count + self._store_pending_children(store, self.taskq_excluded_ids())

    @staticmethod
    def _store_pending_children(store: "_taskq.TaskStore", exclude_ids: list[str]) -> int:
        try:
            lanes = store.pending_lanes(
                _taskq.KIND_SUBAGENT, exclude_ids=exclude_ids, children_only=True
            )
        except _taskq.TaskStoreUnavailable:
            return 0
        return sum(lanes.values())

    async def ensure_coordinator_async(self) -> None:
        """Build the dependency coordinator on the writer thread when it does
        not exist yet: its one-time ``rebuild`` reads the waiting rows, and
        the first loop-side caller should not pay for that on the loop."""
        store = self.taskq_store()
        if (
            store is None
            or getattr(self._manager, "_taskq_dependency_coordinator", None) is not None
        ):
            return
        await store.run(self._manager.dependency_coordinator)

    async def refresh_pending_children_async(self) -> None:
        """Refresh the store half of :meth:`pending_children` on the writer thread."""
        store = self.taskq_store()
        if store is None:
            return
        exclude = self.taskq_excluded_ids()
        value = await store.run(self._store_pending_children, store, exclude)
        setattr(self._manager, "_pending_children_store", int(value))

    def capacity_view(self) -> CapacityView:
        """Read the cap, the running count and the child reserve as one snapshot."""
        settings = self.fairness_settings()
        cap = int(self._manager._max_concurrent)
        running = int(self._manager._running_count)
        waiting = len(self.waiting_parents())
        lifted_from: int | None = None
        if waiting and settings.child_reserve > 0:
            # Honour the reserve under an adaptive squeeze: never above the
            # user's ceiling, and only when a controller has lowered the cap
            # (a cap set by the user or a test is not lifted).
            adaptive = getattr(self._manager, "_adaptive_cap", None)
            ceiling = int(getattr(self._manager, "_user_max_concurrent", cap) or cap)
            if adaptive is not None:
                floor_with_reserve = min(ceiling, settings.adaptive_floor + settings.child_reserve)
                if floor_with_reserve > cap:
                    lifted_from = cap
                    cap = floor_with_reserve
        # The reserve is for STARTS that unblock a tree: nested rows waiting to
        # start and resumes waiting for a slot. A parent that merely waits
        # while its children run reserves nothing -- unrelated work fills the
        # cap (RFC §14.3: siblings and other sessions keep going).
        reserve_active = settings.child_reserve > 0 and (
            any(p.get("_resume_id") for p in self._manager._queue) or self.pending_children() > 0
        )
        return CapacityView(
            cap_total=cap,
            running=running,
            child_reserve=settings.child_reserve,
            reserve_active=reserve_active,
            waiting_parents=waiting,
            lifted_from=lifted_from,
        )

    def root_may_start(self) -> bool:
        """Whether a depth-0 spawn may take a slot right now (the reserve honoured)."""
        return self.capacity_view().root_slot

    def pick_window_index(self, view: CapacityView | None = None) -> int | None:
        """Which ``_queue`` entry the pump takes next, or None when none may start.

        Order: a queued resume (front, FIFO among resumes), then the weighted
        round-robin over lanes among eligible entries. With only the reserve
        left, eligible means nested; with no slot at all, nothing is.
        """
        queue = self._manager._queue
        if not queue:
            return None
        view = view or self.capacity_view()
        if not view.any_slot:
            return None
        for idx, params in enumerate(queue):
            if params.get("_resume_id"):
                return idx
        roots_ok = view.root_slot

        def eligible(params: Mapping[str, Any]) -> bool:
            return roots_ok or self.entry_is_child(params)

        return self.lane_scheduler().pick_index(
            queue, lane_of=self.lane_of_entry, eligible=eligible
        )

    def lane_snapshot(self) -> dict[str, Any]:
        """Per-lane queue depth and running count with the scheduler's balance."""
        settings = self.fairness_settings()
        scheduler = self.lane_scheduler()
        lanes: dict[str, dict[str, Any]] = {}

        def bucket(lane: str) -> dict[str, Any]:
            return lanes.setdefault(
                lane,
                {"queued": 0, "running": 0, "waiting": 0, "weight": scheduler.weight_of(lane)},
            )

        for params in self._manager._queue:
            bucket(self.lane_of_entry(params))["queued"] += 1
        store = self.taskq_store()
        if store is not None:
            try:
                for lane, n in store.pending_lanes(
                    _taskq.KIND_SUBAGENT, exclude_ids=self.taskq_excluded_ids()
                ).items():
                    bucket(lane)["queued"] += n
            except _taskq.TaskStoreUnavailable:
                pass
        for info in self._manager._agents.values():
            if info.done:
                continue
            b = bucket(self.lane_for_session(info.parent_session_key))
            if info._slot_released:
                b["waiting"] += 1
            else:
                b["running"] += 1
        return {
            "lanes": lanes,
            "credit": scheduler.snapshot(),
            "capacity": self.capacity_view().to_dict(),
            "child_reserve": settings.child_reserve,
        }

    def parent_pauser(self) -> ParentPauser:
        """The pauser in force: no-op unless ``agent.parent_checkpoint_pause`` is on.

        With the flag on, an installed ``manager._parent_pauser`` is used; with
        nothing installed the no-op still answers, so the flag alone changes
        nothing until an implementation is wired.
        """
        if not self.fairness_settings().parent_checkpoint_pause:
            return NoopParentPauser()
        installed = getattr(self._manager, "_parent_pauser", None)
        if installed is None:
            return NoopParentPauser()
        return installed  # type: ignore[no-any-return]

    def _maybe_pause_parent(self, info: "SubagentInfo", record: "_taskq.WaitRecord") -> None:
        """On-path of the checkpoint pause: schedule ``pause`` for a children wait."""
        if record.state != _taskq.WAITING_CHILDREN:
            return
        pauser = self.parent_pauser()
        if isinstance(pauser, NoopParentPauser) or not pauser.can_pause(info):
            return
        # ``_pause_pending`` / ``_paused`` live on the info as plain attributes:
        # they are this seam's own bookkeeping, not part of the run record.
        setattr(info, "_pause_pending", True)

        async def _pause() -> None:
            try:
                paused = await pauser.pause(info)
            except Exception:
                _glue_logger.warning("parent pause of %s failed", info.id, exc_info=True)
                paused = False
            setattr(info, "_pause_pending", False)
            setattr(info, "_paused", bool(paused))

        try:
            _asyncio.get_event_loop().create_task(_pause())
        except RuntimeError:
            setattr(info, "_pause_pending", False)

    def _maybe_resume_paused_parent(self, info: "SubagentInfo") -> None:
        """Counterpart of :meth:`_maybe_pause_parent` on the slot grant."""
        if not getattr(info, "_paused", False):
            return
        pauser = self.parent_pauser()
        setattr(info, "_paused", False)

        async def _resume() -> None:
            try:
                await pauser.resume(info)
            except Exception:
                _glue_logger.warning("parent resume of %s failed", info.id, exc_info=True)

        try:
            _asyncio.get_event_loop().create_task(_resume())
        except RuntimeError:
            pass

    async def wait_resume_granted(self, agent_id: str, *, timeout: float) -> bool:
        """Await the slot grant for a yielded run; True when it holds its slot.

        Event-driven, not polled: :meth:`resume_grant` sets the per-run event.
        Answers True at once for a run that never yielded or is unknown here
        (nothing to hold for), False when *timeout* passes first.
        """
        info = self._manager._agents.get(agent_id)
        if info is None or info.done or self.resume_granted(agent_id):
            return True
        event = getattr(info, "_resume_event", None)
        if not isinstance(event, _asyncio.Event):
            event = _asyncio.Event()
            info._resume_event = event
        if self.resume_granted(agent_id):
            return True
        try:
            await _asyncio.wait_for(event.wait(), timeout=max(0.0, float(timeout)))
        except _asyncio.TimeoutError:
            return self.resume_granted(agent_id)
        return True
