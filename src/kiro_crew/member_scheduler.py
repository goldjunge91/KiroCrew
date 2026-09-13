"""Per-member wake scheduler (member inbox model, M0).

One scheduler per gateway, one lane per member. It owns *when* a member wakes;
what a wake does lives in :mod:`kiro_crew.dashboard.member_wake` and is injected
as ``wake_fn`` so this module stays testable with a stub.

Triggers (member inbox model RFC, *Scheduler*):

* **inbox non-empty** — :meth:`MemberScheduler.notify` after an envelope lands.
  ``user_dm`` (:data:`~kiro_crew.member_inbox.IMMEDIATE_KINDS`) wakes
  immediately; every other kind waits a short coalescing window so a burst
  drains as one wake.
* **interval** — a ``wake_timer`` envelope is minted on the member's cadence
  (``members.<slug>.wake_interval_secs``) and coalesced: no second timer
  envelope is minted while one is still pending.

The RFC's hook trigger and its ``worker_report`` / ``escalation_reply`` kinds
arrive with the milestones that produce them; a producer only has to append its
envelope and call :meth:`notify`, nothing here is kind-specific.

Serial per member: one wake at a time, and a trigger that fires during a wake
marks the lane dirty so a follow-up wake runs when the current one ends. The
scheduler's own state is derivable from disk — :meth:`start` scans every flagged
member's pending set — so a gateway restart resumes pending wakes with no
persisted row that a restart could leave marked stopped.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Awaitable, Callable

from kiro_crew.member_inbox import (
    IMMEDIATE_KINDS,
    InboxStore,
    flagged_member_slugs,
    inbox_model_enabled,
    make_envelope,
    member_inbox_setting,
)

logger = logging.getLogger(__name__)

WakeFn = Callable[[str], Awaitable[None]]

DEFAULT_COALESCE_SECS = 5.0
#: Delay before a lane re-runs when a wake ended with envelopes still pending
#: (a failed turn, a wall-clock stop). Bounded by the store's attempt ceiling:
#: each retry raises ``attempts`` and the envelope dead-letters past
#: ``MAX_ATTEMPTS``, so a poison envelope costs at most that many retries.
RETRY_DELAY_SECS = 30.0
#: Below this a cadence is refused: a timer that fires every few seconds is a
#: busy loop wearing a schedule, and each wake is a model turn.
MIN_WAKE_INTERVAL_SECS = 60.0


class _Lane:
    __slots__ = ("slug", "running", "dirty", "pending_task", "timer_task", "wakes", "last_wake_ts")

    def __init__(self, slug: str) -> None:
        self.slug = slug
        self.running = False
        self.dirty = False
        self.pending_task: asyncio.Task | None = None
        self.timer_task: asyncio.Task | None = None
        self.wakes = 0
        self.last_wake_ts = 0.0


class MemberScheduler:
    def __init__(
        self,
        wake_fn: WakeFn,
        *,
        coalesce_secs: float = DEFAULT_COALESCE_SECS,
        interval_for: Callable[[str], float | None] | None = None,
    ) -> None:
        self._wake_fn = wake_fn
        self._coalesce = max(0.0, float(coalesce_secs))
        self._interval_for = interval_for or _configured_interval
        self._lanes: dict[str, _Lane] = {}
        self._stopped = False
        self._loop: asyncio.AbstractEventLoop | None = None

    # ------------------------------------------------------------------ lifecycle
    async def start(self) -> None:
        """Rebuild from disk: wake every flagged member with pending envelopes and
        arm every configured timer. Idempotent."""
        self._stopped = False
        self._loop = asyncio.get_running_loop()
        # Config read and directory scans are filesystem work: off the loop, so a
        # large accumulated inbox never stalls whatever else the gateway is doing.
        from kiro_crew.member_peer import reconcile_peer_sends

        for slug in await asyncio.to_thread(flagged_member_slugs):
            lane = self._lane(slug)
            self._arm_timer(lane, await asyncio.to_thread(self._interval_for, slug))
            # A send the previous process journaled but did not finish is
            # completed (or marked failed) BEFORE the receiver's inbox is
            # scanned, so a recovered envelope is part of this start's resume.
            try:
                await asyncio.to_thread(reconcile_peer_sends, slug)
            except Exception:  # noqa: BLE001 - reconciliation is logged, never fatal at boot
                logger.warning("member scheduler: reconcile failed for %s", slug, exc_info=True)
            try:
                pending = await asyncio.to_thread(InboxStore(slug).pending)
            except Exception:  # noqa: BLE001 - one member's bad directory must not stop the others
                logger.warning("member scheduler: cannot scan inbox for %s", slug, exc_info=True)
                continue
            if pending:
                immediate = any(e.kind in IMMEDIATE_KINDS for e in pending)
                logger.info(
                    "member scheduler: %s has %d pending envelope(s) at start; resuming",
                    slug,
                    len(pending),
                )
                self.notify(slug, immediate=immediate)

    async def stop(self) -> None:
        self._stopped = True
        tasks: list[asyncio.Task] = []
        for lane in self._lanes.values():
            for t in (lane.pending_task, lane.timer_task):
                if t is not None and not t.done():
                    t.cancel()
                    tasks.append(t)
            lane.pending_task = None
            lane.timer_task = None
        for t in tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass

    # ------------------------------------------------------------------- triggers
    def notify(self, slug: str, *, immediate: bool = False) -> None:
        """An envelope landed for *slug*: schedule a wake.

        Thread-safe: producers that wrote the envelope on a worker thread
        (``asyncio.to_thread``) hop back to the loop. The flag and the cadence
        are config reads (disk), so they are resolved in the lane task -- off the
        loop -- not here; an unflagged member's wake is a no-op that leaves the
        envelope on disk for when it is flagged.
        """
        if self._stopped:
            return
        loop = self._loop
        if loop is None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                logger.warning("member scheduler: notify(%s) before start(); dropped", slug)
                return
            self._loop = loop
        try:
            on_loop = asyncio.get_running_loop() is loop
        except RuntimeError:
            on_loop = False
        if on_loop:
            self._notify_on_loop(slug, immediate)
        else:
            loop.call_soon_threadsafe(self._notify_on_loop, slug, immediate)

    def _notify_on_loop(self, slug: str, immediate: bool) -> None:
        if self._stopped:
            return
        lane = self._lane(slug)
        if lane.running:
            lane.dirty = True
            return
        delay = 0.0 if immediate else self._coalesce
        if lane.pending_task is not None and not lane.pending_task.done():
            if not immediate:
                return  # already scheduled inside the window
            lane.pending_task.cancel()
        lane.pending_task = asyncio.get_running_loop().create_task(self._run_after(lane, delay))

    def is_running(self, slug: str) -> bool:
        lane = self._lanes.get(slug)
        return bool(lane and lane.running)

    # -------------------------------------------------------------------- internal
    def _lane(self, slug: str) -> _Lane:
        lane = self._lanes.get(slug)
        if lane is None:
            lane = _Lane(slug)
            self._lanes[slug] = lane
        return lane

    async def _run_after(self, lane: _Lane, delay: float) -> None:
        if delay > 0:
            await asyncio.sleep(delay)
        # Config reads happen HERE, on a worker thread: the flag decides whether
        # the wake runs at all, and a member flagged after boot gets its cadence
        # armed on its first envelope instead of waiting for a restart.
        if not await asyncio.to_thread(inbox_model_enabled, lane.slug):
            lane.pending_task = None
            return
        interval = await asyncio.to_thread(self._interval_for, lane.slug)
        self._arm_timer(lane, interval)
        await self._run_lane(lane)

    async def _run_lane(self, lane: _Lane) -> None:
        if lane.running:
            lane.dirty = True
            return
        lane.running = True
        try:
            while True:
                lane.dirty = False
                lane.wakes += 1
                lane.last_wake_ts = time.time()
                try:
                    await self._wake_fn(lane.slug)
                except asyncio.CancelledError:
                    raise
                except (
                    Exception
                ):  # noqa: BLE001 - a wake failure is logged, never fatal to the lane
                    logger.exception("member scheduler: wake for %s raised", lane.slug)
                if self._stopped:
                    break
                if lane.dirty:
                    continue
                # A wake that ended without acking (provider failure, budget) leaves
                # its envelopes pending, and nothing else notifies the lane -- so
                # without this they would sit until the next unrelated trigger or a
                # restart. Schedule a bounded retry; the attempt ceiling ends it.
                if await asyncio.to_thread(self._has_pending, lane.slug):
                    loop = asyncio.get_running_loop()
                    lane.pending_task = loop.create_task(self._run_after(lane, RETRY_DELAY_SECS))
                    lane.running = False
                    return
                break
        finally:
            lane.running = False
            if lane.pending_task is not None and lane.pending_task.done():
                lane.pending_task = None

    @staticmethod
    def _has_pending(slug: str) -> bool:
        try:
            return bool(InboxStore(slug).pending())
        except Exception:  # noqa: BLE001 - an unreadable inbox is not grounds to spin
            return False

    def _arm_timer(self, lane: _Lane, interval: float | None) -> None:
        if not interval or interval <= 0:
            return
        if interval < MIN_WAKE_INTERVAL_SECS:
            logger.warning(
                "member scheduler: %s wake_interval_secs=%s below the %ss floor; clamping",
                lane.slug,
                interval,
                MIN_WAKE_INTERVAL_SECS,
            )
            interval = MIN_WAKE_INTERVAL_SECS
        if lane.timer_task is not None and not lane.timer_task.done():
            return
        lane.timer_task = asyncio.get_running_loop().create_task(
            self._timer_loop(lane, float(interval))
        )

    async def _timer_loop(self, lane: _Lane, interval: float) -> None:
        while not self._stopped:
            await asyncio.sleep(interval)
            if self._stopped or not await asyncio.to_thread(inbox_model_enabled, lane.slug):
                return
            try:
                minted = await asyncio.to_thread(_mint_wake_timer, lane.slug, interval)
                if not minted:
                    continue  # coalesce: the last tick has not been consumed yet
            except Exception:  # noqa: BLE001 - a bad tick must not end the cadence
                logger.warning(
                    "member scheduler: could not mint wake_timer for %s", lane.slug, exc_info=True
                )
                continue
            self.notify(lane.slug)


def _mint_wake_timer(slug: str, interval: float) -> bool:
    """Append one ``wake_timer`` unless one is still pending. Filesystem work."""
    store = InboxStore(slug)
    if store.has_pending_kind("wake_timer"):
        return False
    store.append(
        make_envelope(
            to_slug=slug,
            kind="wake_timer",
            body=f"Scheduled wake (every {int(interval)}s).",
            from_="system",
        )
    )
    return True


def _configured_interval(slug: str) -> float | None:
    """``members.<slug>.wake_interval_secs`` as a float >= the floor, else ``None``.

    Coerced defensively: a hand-edited knob must never raise into the scheduler.
    """
    raw = member_inbox_setting(slug, "wake_interval_secs", None)
    if raw is None:
        return None
    try:
        value = float(raw) if not isinstance(raw, bool) else float("nan")
    except (TypeError, ValueError):
        value = float("nan")
    if value != value or value < MIN_WAKE_INTERVAL_SECS:
        logger.warning(
            "member scheduler: ignoring wake_interval_secs=%r for %s (not a number >= %s)",
            raw,
            slug,
            MIN_WAKE_INTERVAL_SECS,
        )
        return None
    return value


# --------------------------------------------------------------- process singleton

_scheduler: MemberScheduler | None = None


def set_scheduler(scheduler: MemberScheduler | None) -> None:
    global _scheduler
    _scheduler = scheduler


def member_wake_running(slug: str) -> bool:
    """Whether a wake for *slug* is in flight right now (``False`` with no scheduler)."""
    sched = _scheduler
    return bool(sched is not None and sched.is_running(slug))


def notify_member(slug: str, *, kind: str) -> None:
    """Convenience for producers: wake *slug* with the right urgency for *kind*."""
    sched = _scheduler
    if sched is None:
        return
    sched.notify(slug, immediate=kind in IMMEDIATE_KINDS)
