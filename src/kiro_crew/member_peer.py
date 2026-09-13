"""``peer_send`` admission: one member messages another (member inbox model, M0).

The full admission sequence of ``rfc-member-inbox-model.md`` *Peer DM as
envelopes*, in order, each refusal a typed :class:`PeerSendError`:

1. target resolves to a member identity WITH a DM binding — ``peer_dm_target_unknown``
2. global switch ``member_peer_dm.enabled`` — ``peer_dm_disabled``;
   receiver ``members.<slug>.peer_dm.accept`` / sender ``.send`` — ``peer_dm_opted_out``
3. (workspace mismatch is not reachable in M0: members are global crews)
4. causal hop — ``peer_dm_hop_limit``
5. per-member unattended budget — ``peer_dm_budget_exhausted``
6. per-pair rate limit — ``peer_dm_rate_limited``
7. body checks — ``message_empty`` / ``message_too_long``
8. receiver inbox envelope + sender outbox mirror, one call -- JOURNALED: the
   mirror is written first with ``delivery: pending``, stamped ``delivered``
   after the receiver's append; :func:`reconcile_peer_sends` completes or
   fails any send a crash left pending, at scheduler start
9. SEL on allow and on every refusal

Bounds are HUMAN-GATED: the hop chain restarts only when a wake was not
triggered by an inbound ``peer_dm`` (the wake runner records the max inbound
hop on the wake slot), and the budget window is anchored on the newest
``user_dm`` the PERSON typed in the sender's own inbox. Nothing time-based
resets either; see the RFC for why a quiet-window reset is a rate in disguise.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from typing import Any

from kiro_crew.member_inbox import (
    MAX_BODY_CHARS,
    Envelope,
    InboxError,
    InboxStore,
    OutboxStore,
    inbox_model_enabled,
    make_envelope,
    member_inbox_setting,
    member_slug_from_key,
    strip_reserved_refs,
)
from kiro_crew.members import DM_SLOT_KEY_PREFIX, read_dm_binding, validate_slug
from kiro_crew.sel import sel

logger = logging.getLogger(__name__)

#: ``refs.delivery`` states of a ``peer_dm`` mirror row (the send journal).
DELIVERY_PENDING = "pending"
DELIVERY_DELIVERED = "delivered"
DELIVERY_FAILED = "failed"

#: The bounds. Constants, not config: no operator has asked for other values,
#: a config key is honoured forever, and the RFC's open decision on their size
#: is to be settled from SEL data first. ``member_peer_dm.enabled`` (the kill
#: switch) is the one configurable input.
DEFAULT_MAX_HOPS = 6
DEFAULT_MAX_UNATTENDED_SENDS = 12
DEFAULT_RATE_PER_WINDOW = 12
RATE_WINDOW_SECS = 600.0


class PeerSendError(Exception):
    def __init__(
        self, message: str, *, code: str, status: int = 403, retry_after: float | None = None
    ) -> None:
        super().__init__(message)
        self.code = code
        self.status = status
        self.retry_after = retry_after


class _Degraded(Exception):
    """The config could not be read, or a peer-DM key is malformed: deny."""


def peer_dm_enabled() -> bool:
    """``member_peer_dm.enabled`` (default ``True``); raises :class:`_Degraded`
    when the config cannot be read, the whole file / a section was discarded on
    load, or the key is present with a non-boolean value. A malformed switch
    must not fall back to the permissive default -- that is the fail-open the
    admission exists to prevent."""
    try:
        from kiro_crew.config.loader import KiroCrewConfig
        from kiro_crew.config.resolution import DEGRADED_WHOLE_CONFIG

        cfg = KiroCrewConfig.load()
    except Exception as exc:  # noqa: BLE001
        raise _Degraded("config read failed") from exc
    if DEGRADED_WHOLE_CONFIG in getattr(cfg, "degraded_sections", set()):
        raise _Degraded("config degraded")
    section = getattr(cfg, "member_peer_dm", None)
    if section is None:
        return True
    if not isinstance(section, dict):
        raise _Degraded("member_peer_dm is not a mapping")
    if "enabled" not in section:
        return True
    value = section["enabled"]
    if not isinstance(value, bool):
        raise _Degraded("member_peer_dm.enabled is not a boolean")
    return value


#: One lock per sender: the budget CHECK (count mirror rows since the anchor)
#: and the mirror WRITE that spends the budget must not interleave across two
#: concurrent sends from the same member, or both would pass with one slot left.
_sender_locks: dict[str, threading.Lock] = {}
_sender_locks_guard = threading.Lock()


def _sender_lock(slug: str) -> threading.Lock:
    with _sender_locks_guard:
        lock = _sender_locks.get(slug)
        if lock is None:
            lock = _sender_locks[slug] = threading.Lock()
        return lock


def _member_opt(slug: str, key: str) -> bool:
    """``members.<slug>.peer_dm.<key>``; anything present but not a boolean
    reads as ``False`` (deny) rather than as the permissive default."""
    raw = member_inbox_setting(slug, "peer_dm", None)
    if raw is None:
        return True
    if not isinstance(raw, dict):
        return False
    value = raw.get(key, True)
    return value is True


def pair_id(a: str, b: str) -> str:
    return "|".join(sorted((a, b)))


class _PairRate:
    """Sliding-window counter per unordered pair, process-local.

    Burst cost only; termination is the hop and budget bounds, which are on
    disk. Losing this on restart is acceptable — the window is ten minutes.
    """

    def __init__(self) -> None:
        self._hits: dict[str, deque[float]] = {}
        # The pair is UNORDERED but the callers hold per-SENDER locks, so A->B
        # and B->A reach the same deque under different locks; this lock is what
        # makes the boundary test and the append one step.
        self._lock = threading.Lock()

    def check_and_add(
        self, pid: str, limit: int, window: float, now: float | None = None
    ) -> float | None:
        now = time.monotonic() if now is None else now
        with self._lock:
            q = self._hits.setdefault(pid, deque())
            while q and now - q[0] >= window:
                q.popleft()
            if len(q) >= limit:
                return window - (now - q[0])
            q.append(now)
            return None

    def reset(self) -> None:
        with self._lock:
            self._hits.clear()


_rate = _PairRate()


def resolve_target_slug(target: str) -> str:
    """``member-<slug>`` / ``<slug>`` -> validated slug, else ``peer_dm_target_unknown``."""
    t = (target or "").strip()
    slug = member_slug_from_key(t) if t.startswith((DM_SLOT_KEY_PREFIX, "dashboard")) else None
    if slug is None:
        try:
            slug = validate_slug(t)
        except Exception as exc:  # noqa: BLE001
            raise PeerSendError(
                f"no crew member matches {target!r}", code="peer_dm_target_unknown", status=404
            ) from exc
    return slug


def _audit(
    *, caller_key: str, outcome: str, code: str = "", detail: dict[str, Any] | None = None
) -> None:
    try:
        if outcome == "allowed":
            sel().log_tool_invocation(
                session_key=caller_key,
                tool_name="peer_send",
                tool_kind="command",
                source="mcp",
                outcome="allowed",
                resources=f"target={(detail or {}).get('to', '')}",
                metadata=dict(detail or {}),
            )
        else:
            sel().log_api_access(
                caller=caller_key,
                operation="peer_send",
                outcome="denied",
                source="mcp",
                resources=f"target={(detail or {}).get('to', '')}:{code}",
                error=code,
            )
    except Exception:  # noqa: BLE001 - audit must never turn a refusal into a 500
        logger.warning("peer_send: SEL write failed", exc_info=True)


def peer_send(
    *,
    caller_key: str,
    sender_slug: str,
    target: str,
    body: str,
    inbound_hop: int = 0,
    refs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run the admission and, on success, write both sides. Returns a receipt.

    *inbound_hop* is the max ``hop`` among the ``peer_dm`` envelopes the calling
    wake drained (0 for a wake with no inbound peer message). *caller_key* is
    only for the audit row.
    """
    to_slug = resolve_target_slug(target)
    sender_slug = validate_slug(sender_slug)
    detail: dict[str, Any] = {"from": sender_slug, "to": to_slug}

    def deny(
        msg: str, code: str, status: int = 403, retry_after: float | None = None
    ) -> PeerSendError:
        _audit(caller_key=caller_key, outcome="denied", code=code, detail=detail)
        return PeerSendError(msg, code=code, status=status, retry_after=retry_after)

    if to_slug == sender_slug:
        raise deny("a member cannot message itself", "peer_dm_self_target", 400)
    try:
        enabled = peer_dm_enabled()
    except _Degraded as exc:
        raise deny(f"member-to-member DM refused: {exc}", "peer_dm_config_degraded", 503) from exc
    if not enabled:
        raise deny("member-to-member DM is disabled (member_peer_dm.enabled)", "peer_dm_disabled")
    if not inbox_model_enabled(sender_slug):
        # A member still on the session model keeps today's reach exactly: the
        # creator fence, not the peer admission. Otherwise an unflagged member
        # could reach a flagged one through the shim while nothing about ITS
        # own wake, budget anchor or projection exists yet.
        raise deny(
            f"member {sender_slug!r} is not on the inbox model (members.{sender_slug}.inbox_model)",
            "peer_dm_sender_not_flagged",
            409,
        )
    if not inbox_model_enabled(to_slug):
        raise deny(
            f"member {to_slug!r} is not on the inbox model (members.{to_slug}.inbox_model)",
            "peer_dm_target_not_flagged",
            409,
        )
    # The flag alone is a config line; the member is REACHABLE only with a DM
    # binding, which is what a wake needs to run (agent, generation). A slug
    # whose binding is gone -- a partial delete, a hand edit -- would otherwise
    # take the envelope, report delivery, and leave it pending forever.
    binding = read_dm_binding(to_slug)
    if binding is None or not binding.get("member"):
        raise deny(
            f"member {to_slug!r} has no DM thread binding; it cannot be woken",
            "peer_dm_target_unknown",
            404,
        )
    if not _member_opt(to_slug, "accept"):
        raise deny(
            f"member {to_slug!r} does not accept messages from other members", "peer_dm_opted_out"
        )
    if not _member_opt(sender_slug, "send"):
        raise deny(
            f"member {sender_slug!r} is not allowed to message other members", "peer_dm_opted_out"
        )

    cap, budget, rate = DEFAULT_MAX_HOPS, DEFAULT_MAX_UNATTENDED_SENDS, DEFAULT_RATE_PER_WINDOW
    hop = max(0, int(inbound_hop)) + 1
    detail["hop"] = hop
    if hop > cap:
        raise deny(
            f"this thread of member messages is {cap} hops long without the owner weighing in; "
            "report to the owner rather than continuing",
            "peer_dm_hop_limit",
            429,
        )

    # Budget check and the mirror write that spends it are one critical section
    # per sender (see _sender_lock). The receiver-side append is inside it too so
    # the pair rate is charged in the same order the mirrors land.
    with _sender_lock(sender_slug):
        sender_inbox = InboxStore(sender_slug)
        sender_outbox = OutboxStore(sender_slug)
        since = sender_inbox.last_user_dm_at()
        sent = sender_outbox.peer_sends_since(since)
        detail["budget_used"] = sent
        if sent >= budget:
            raise deny(
                f"{budget} member messages sent since the owner last wrote to you; "
                "report to the owner rather than continuing",
                "peer_dm_budget_exhausted",
                429,
            )

        # Body checks come BEFORE the rate charge and use the SANITIZED text:
        # redaction can expand a body past the cap, and a refusal after the
        # charge would spend a rate slot on a message that never landed.
        if not isinstance(body, str) or not body.strip():
            raise deny("message is empty", "message_empty", 400)
        from kiro_crew.dashboard.chat_delivery import sanitize_outbound

        clean = sanitize_outbound(body)
        if len(clean) > MAX_BODY_CHARS:
            raise deny(
                f"message exceeds {MAX_BODY_CHARS} characters after sanitization",
                "message_too_long",
                400,
            )

        pid = pair_id(sender_slug, to_slug)
        detail["pair_id"] = pid
        retry = _rate.check_and_add(pid, rate, RATE_WINDOW_SECS)
        if retry is not None:
            raise deny(
                f"too many messages between {sender_slug} and {to_slug} in the last 10 minutes",
                "peer_dm_rate_limited",
                429,
                retry_after=retry,
            )

        # The model's refs minus the keys the journal and the stores own.
        env_refs = {**strip_reserved_refs(refs), "pair_id": pid}
        # Two files, one logical send, JOURNALED through the mirror. The mirror
        # goes first because it is the budget record, and it carries everything
        # the receiver's envelope needs (id, body, hop, pair) plus a `delivery`
        # state: `pending` until the receiver's append returns, then `delivered`.
        # A crash between the two writes therefore leaves a mirror that SAYS it
        # is unfinished, and `reconcile_peer_sends` (scheduler start) completes
        # the send from the journal or marks it failed -- never a row the
        # projection shows as sent while the receiver has nothing.
        env = make_envelope(
            to_slug=to_slug,
            kind="peer_dm",
            body=clean,
            from_=f"member:{sender_slug}",
            hop=hop,
            refs=env_refs,
        )
        try:
            mirror = sender_outbox.append(
                kind="peer_dm",
                body=clean,
                hop=hop,
                refs={
                    **env_refs,
                    "to": f"member:{to_slug}",
                    "envelope_id": env.id,
                    "delivery": DELIVERY_PENDING,
                },
            )
        except (InboxError, OSError) as exc:
            raise deny(f"could not write the message: {exc}", "peer_dm_write_failed", 500) from exc
        try:
            InboxStore(to_slug).append(env)
        except (InboxError, OSError) as exc:
            _stamp(
                sender_outbox, mirror.id, delivery=DELIVERY_FAILED, delivery_error=str(exc)[:200]
            )
            raise deny(
                f"could not deliver the message: {exc}", "peer_dm_write_failed", 500
            ) from exc
        # Losing this stamp to a crash is the recoverable case: the receiver has
        # the envelope, so reconciliation finds the trace and stamps it then.
        _stamp(sender_outbox, mirror.id, delivery=DELIVERY_DELIVERED)
    detail["envelope_id"] = env.id
    _audit(caller_key=caller_key, outcome="allowed", detail=detail)
    from kiro_crew.member_scheduler import notify_member

    notify_member(to_slug, kind="peer_dm")
    return {"ok": True, "to": to_slug, "envelope_id": env.id, "mirror_id": mirror.id, "hop": hop}


def _stamp(outbox: OutboxStore, mirror_id: str, **refs: Any) -> None:
    try:
        outbox.mark(mirror_id, **refs)
    except OSError:
        logger.warning("peer_send: could not stamp %s on %s", refs, mirror_id, exc_info=True)


def reconcile_peer_sends(sender_slug: str) -> dict[str, int]:
    """Finish every journaled send of *sender_slug* a crash left ``pending``.

    For each ``peer_dm`` mirror still marked ``delivery: pending``:

    * the receiver has a trace of the envelope id (pending, acked or
      dead-lettered) -> only the ``delivered`` stamp was lost; stamp it;
    * no trace -> the crash hit between the two writes; rebuild the envelope
      from the mirror (same id, body, hop, pair) and append it, then stamp
      ``delivered`` with ``recovered: true`` and notify the receiver's lane;
    * the receiver is not a flagged member (any more), or the append fails ->
      stamp ``failed`` so the projection stops claiming the message was sent.

    Budget and rate were charged at send time and are not charged again.
    Filesystem work: call off the loop. Returns counts for logs and tests.
    """
    out = {"stamped": 0, "recovered": 0, "failed": 0}
    try:
        outbox = OutboxStore(sender_slug)
        rows = outbox.rows()
    except Exception:  # noqa: BLE001 - an unreadable outbox is logged, never fatal at boot
        logger.warning("peer_send: cannot scan outbox of %s for reconciliation", sender_slug)
        return out
    for row in rows:
        if row.kind != "peer_dm" or row.refs.get("delivery") != DELIVERY_PENDING:
            continue
        env_id = str(row.refs.get("envelope_id") or "")
        to = str(row.refs.get("to") or "")
        try:
            to_slug = validate_slug(to.removeprefix("member:"))
            receiver = InboxStore(to_slug)
            if receiver.has_record(env_id):
                _stamp(outbox, row.id, delivery=DELIVERY_DELIVERED)
                out["stamped"] += 1
                continue
            if not inbox_model_enabled(to_slug):
                raise InboxError(f"member {to_slug!r} is not on the inbox model")
            # Same reachability rule as the live send: without a DM binding no
            # wake can run for the receiver, so an envelope appended here would
            # sit pending forever behind a mirror stamped `delivered`. A binding
            # that vanished in the crash window marks the send failed instead
            # (the person sees it on the sender's thread and can resend once the
            # receiver's thread is opened again).
            binding = read_dm_binding(to_slug)
            if binding is None or not binding.get("member"):
                raise InboxError(f"member {to_slug!r} has no DM thread binding; it cannot be woken")
            # The receiver's refs are rebuilt from the journal exactly as the
            # live path builds them: the sender's custom refs (everything the
            # mirror carries minus the keys the journal and the stores own)
            # plus the pair. Keeping only `pair_id` here would make a crash
            # between the two appends silently drop the sender's refs from the
            # one delivery reconciliation exists to complete.
            receiver.append(
                Envelope(
                    id=env_id,
                    from_=row.from_,
                    to=to,
                    kind="peer_dm",
                    body=row.body,
                    hop=row.hop,
                    created_at=row.created_at,
                    refs={**strip_reserved_refs(row.refs), "pair_id": row.refs.get("pair_id", "")},
                )
            )
        except (InboxError, OSError, ValueError) as exc:
            _stamp(outbox, row.id, delivery=DELIVERY_FAILED, delivery_error=str(exc)[:200])
            out["failed"] += 1
            continue
        _stamp(outbox, row.id, delivery=DELIVERY_DELIVERED, recovered=True)
        out["recovered"] += 1
        from kiro_crew.member_scheduler import notify_member

        notify_member(to_slug, kind="peer_dm")
    if any(out.values()):
        logger.info("peer_send: reconciled journaled sends of %s: %s", sender_slug, out)
    return out
