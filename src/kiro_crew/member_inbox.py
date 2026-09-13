"""Per-member inbox / outbox of typed envelopes (member inbox model, M0).

Implements the on-disk half of the member inbox model RFC (``rfc-member-inbox-model``):

* an **inbox** — append-only, one JSON file per envelope under
  ``member_dir(slug)/inbox/``, fsync'd on write so a restart cannot lose a
  message that was acknowledged to its sender;
* an **outbox** — the member's own replies and mirrored peer sends, same shape,
  under ``member_dir(slug)/outbox/``;
* a **dead-letter** directory for envelopes that crashed every wake.

Envelopes are immutable once written. Acknowledgement and attempt counting are
recorded by rewriting the envelope into its new place (``inbox/acked/`` or
``dead-letter/``) and unlinking the original, so a scan of ``inbox/*.json`` is
always exactly the pending set — no index file to fall out of sync with the
directory.

Nothing here schedules or runs anything: :mod:`kiro_crew.member_scheduler`
watches these directories and :mod:`kiro_crew.dashboard.member_wake` drains them.
The module is import-light on purpose (stdlib + ``members`` + ``atomic_write``)
so the shim in ``session_control`` and the chat handler can reach it without
pulling the scheduler in.
"""

from __future__ import annotations

import itertools
import json
import logging
import os
import re
import secrets
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from kiro_crew.atomic_write import atomic_write, fsync_dir
from kiro_crew.config.paths import data_home
from kiro_crew.members import DM_SLOT_KEY_PREFIX
from kiro_crew.members import WAKE_SLOT_MODE as _WAKE_SLOT_MODE
from kiro_crew.members import MemberSlugError, validate_slug

logger = logging.getLogger(__name__)

INBOX_DIR_NAME = "inbox"
ACKED_DIR_NAME = "acked"
OUTBOX_DIR_NAME = "outbox"
DEAD_LETTER_DIR_NAME = "dead-letter"
#: Root of every member's inbox / outbox / dead-letter state: a TOP-LEVEL leaf of
#: the crew home, bind-masked from every sandbox mode (``sandbox._CREW_HIDDEN_LEAVES``)
#: and fenced from agent file tools (``security.paths._CREW_SECRET_LEAVES``). Not the
#: member's own directory (agent-writable), and not a child of ``trust/`` (which is
#: writable in-sandbox for SEL appends, so a nested mask could be renamed around):
#: an envelope forged on disk would bypass the peer admission and the
#: ``from == "user"`` provenance the budgets rest on. Only the gateway reads or
#: writes here, through :class:`InboxStore` / :class:`OutboxStore`.
STORE_DIR_NAME = "member-inbox"

#: The exact shape :func:`new_envelope_id` mints. Every path built from an id is
#: gated on it, so an id read back from disk or a request can only ever name a
#: file inside the member's own inbox -- never ``../`` into another's.
_ENVELOPE_ID_RE = re.compile(r"^env_\d{13}_[0-9a-f]{8}$")

#: Re-exported from ``members`` so the wake runner and the slot registry read
#: one constant; see :func:`kiro_crew.members.is_member_mode`.
WAKE_SLOT_MODE = _WAKE_SLOT_MODE
#: Suffix appended to the member's canonical slot key to mint a wake slot key:
#: ``member-<slug>.wake-<n>``. Keeps the ``member-`` prefix (so provider-side
#: member detection and the dashboard server mount still apply) while
#: :func:`member_owner_key` folds it back to the member for ownership.
WAKE_KEY_INFIX = ".wake-"

#: The kinds M0 PRODUCES. ``user_dm`` is the person typing in the thread;
#: ``session_dm`` is another session's ``session_send`` translated by the shim
#: (its own kind, so "not the owner" is a property of the kind and not of a
#: field); ``peer_dm`` is another member's ``peer_send``; ``wake_timer`` is the
#: member's own cadence; ``system`` is a gateway notice. Kinds arrive with the
#: milestone that writes them (``make_envelope`` refuses an unknown kind, so no
#: file can carry one before its producer exists).
ENVELOPE_KINDS: tuple[str, ...] = (
    "user_dm",
    "session_dm",
    "peer_dm",
    "wake_timer",
    "system",
)
#: Kinds that wake the member without waiting for the coalescing window.
IMMEDIATE_KINDS: frozenset[str] = frozenset({"user_dm"})
#: Kind of an outbox row the member wrote as a reply into its projection.
OUTBOX_REPLY_KIND = "reply"

#: ``refs`` keys the GATEWAY writes and reads back as state -- the redelivery
#: idempotency stamp, the send journal, the batch a reply answers, the pair a
#: send belongs to. A model's free-form ``refs`` (``outbox_send`` / ``peer_send``)
#: is stripped of these before the merge (:func:`strip_reserved_refs`): a wake
#: steered by an untrusted envelope must not be able to write ``completed: true``
#: onto an "on it..." row and have the owner's message acked without a turn.
RESERVED_REF_KEYS: frozenset[str] = frozenset(
    {
        "completed",
        "wake_key",
        "in_reply_to",
        "delivery",
        "delivery_error",
        "recovered",
        "envelope_id",
        "to",
        "pair_id",
        "dead_letter_reason",
        "dead_lettered_at",
        "view",
    }
)

#: ``refs.view`` states of a durable record whose THREAD ROW (the conversational
#: view) is not known to be on disk. The store is the record and the transcript
#: is the view, but a view that silently diverges from the record is a message
#: the person never sees: a failed transcript save stamps the record
#: ``view=pending``, and ``member_wake.reconcile_thread_view`` re-appends the
#: row before the next intake or wake processes anything, clearing the stamp
#: only after a confirmed write.
VIEW_REF = "view"
VIEW_PENDING = "pending"
VIEW_RESTORED = "restored"


def strip_reserved_refs(refs: Any) -> dict[str, Any]:
    """The model's ``refs`` minus every key in :data:`RESERVED_REF_KEYS`."""
    if not isinstance(refs, dict):
        return {}
    return {str(k): v for k, v in refs.items() if str(k) not in RESERVED_REF_KEYS}


#: ``from`` value for a ``user_dm`` -- the person typed it. Only a ``user_dm``
#: resets the peer-DM budgets; a ``session_dm`` never does.
FROM_USER = "user"

#: Ceiling on redelivery before an envelope is dead-lettered, and the wall clock
#: one wake may run. Constants, not per-member config: no operator has asked for
#: per-member variance, and a config key is honoured forever.
MAX_ATTEMPTS = 3
WAKE_WALL_SECS = 600.0
MAX_BODY_CHARS = 50_000
#: Retention: how many acked / outbox / dead-letter rows a member keeps. The
#: budget anchor (``last_user_dm_at``) and ``peer_sends_since`` scan these
#: directories on every ``peer_send``, and ``recent_exchange`` on every wake, so
#: the store must not grow without bound. ``compact`` runs after each wake and
#: always keeps the newest ``user_dm`` so the budget anchor survives pruning.
RETAIN_ACKED = 500
RETAIN_OUTBOX = 500
RETAIN_DEAD_LETTERS = 100


class InboxError(Exception):
    """A store operation that could not be honoured."""


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def validate_envelope_id(env_id: str) -> str:
    """Return *env_id* if it has the minted shape, else raise :class:`InboxError`."""
    if not isinstance(env_id, str) or not _ENVELOPE_ID_RE.match(env_id):
        raise InboxError("malformed envelope id")
    return env_id


def member_store_dir(slug: str) -> Path:
    """``<data_home>/member-inbox/<slug>``, containment-checked like
    :func:`kiro_crew.members.dm_binding_path`."""
    validate_slug(slug)
    root = (data_home() / STORE_DIR_NAME).resolve()
    target = (root / slug).resolve()
    if target.parent != root and root not in target.parents:
        raise MemberSlugError(f"member slug {slug!r} escapes {root}")
    return target


_id_seq = itertools.count()
_id_seq_lock = threading.Lock()


def new_envelope_id() -> str:
    """Time-ordered, collision-resistant id: ``env_<ms since epoch>_<seq4><rand4>``.

    Lexicographic order of ids is delivery order, which is what lets the
    pending set be ``sorted(inbox/*.json)`` with no index. Within one
    millisecond the order is fixed by a process-wide, lock-protected sequence
    (four hex digits, wrapping), not by the random tail -- so two envelopes
    minted together are read in the order they were appended. The random tail
    keeps ids from two processes (a restart mid-millisecond) distinct.
    """
    with _id_seq_lock:
        seq = next(_id_seq) & 0xFFFF
    return f"env_{int(time.time() * 1000):013d}_{seq:04x}{secrets.token_hex(2)}"


@dataclass
class Envelope:
    id: str
    from_: str
    to: str
    kind: str
    body: str
    hop: int = 0
    created_at: str = field(default_factory=now_iso)
    refs: dict[str, Any] = field(default_factory=dict)
    attempts: int = 0
    acked_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["from"] = d.pop("from_")
        return d

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "Envelope":
        data = dict(raw)
        data["from_"] = str(data.pop("from", ""))
        refs = data.get("refs")
        data["refs"] = dict(refs) if isinstance(refs, dict) else {}
        try:
            data["hop"] = int(data.get("hop", 0) or 0)
            data["attempts"] = int(data.get("attempts", 0) or 0)
        except (TypeError, ValueError):
            data["hop"], data["attempts"] = 0, 0
        known = {f for f in cls.__dataclass_fields__}  # type: ignore[attr-defined]
        return cls(**{k: v for k, v in data.items() if k in known})


def make_envelope(
    *,
    to_slug: str,
    kind: str,
    body: str,
    from_: str,
    hop: int = 0,
    refs: dict[str, Any] | None = None,
) -> Envelope:
    if kind not in ENVELOPE_KINDS:
        raise InboxError(f"unknown envelope kind {kind!r}")
    if not isinstance(body, str) or not body.strip():
        raise InboxError("envelope body is empty")
    if len(body) > MAX_BODY_CHARS:
        raise InboxError(f"envelope body exceeds {MAX_BODY_CHARS} characters")
    return Envelope(
        id=new_envelope_id(),
        from_=from_,
        to=f"member:{validate_slug(to_slug)}",
        kind=kind,
        body=body,
        hop=max(0, int(hop)),
        refs=dict(refs or {}),
    )


# --------------------------------------------------------------------------- keys


def member_owner_key(key: str) -> str:
    """Fold a wake slot key (``member-<slug>.wake-<n>``) to the member's own key.

    Identity for ownership and ledger purposes: a worker a wake created must be
    controllable by the member's NEXT wake, and every wake must read the same
    ledger, so the ephemeral suffix is never the identity. Non-member keys and
    keys without the infix return unchanged, so the function is safe to apply
    to every caller key.
    """
    if not key or not key.startswith(DM_SLOT_KEY_PREFIX):
        return key
    idx = key.find(WAKE_KEY_INFIX)
    if idx == -1:
        return key
    tail = key[idx + len(WAKE_KEY_INFIX) :]
    if not tail.isdigit():
        return key
    return key[:idx]


def member_slug_from_key(key: str | None) -> str | None:
    """``member-<slug>[.memory-<store>][.wake-<n>]`` -> ``<slug>``, else ``None``.

    Accepts the ``dashboard_`` / ``dashboard:`` spellings like
    :func:`kiro_crew.members.is_member_session_key`.
    """
    if not key:
        return None
    k = key
    for prefix in ("dashboard_", "dashboard:"):
        if k.startswith(prefix):
            k = k[len(prefix) :]
            break
    k = member_owner_key(k)
    if not k.startswith(DM_SLOT_KEY_PREFIX):
        return None
    rest = k[len(DM_SLOT_KEY_PREFIX) :]
    slug = rest.split(".memory-", 1)[0]
    try:
        return validate_slug(slug)
    except Exception:  # noqa: BLE001 - a malformed key is simply not a member key
        return None


def wake_slot_key_for(member_key: str, seq: int) -> str:
    """Mint ``<member_key>.wake-<ms since epoch><seq:03>`` -- unique across restarts.

    *member_key* is the binding's ``slot_key`` -- ``member-<slug>`` or, for a
    member on a private V2 memory generation, ``member-<slug>.memory-<store>``.
    Minting on it (not on the bare slug) is what keeps a wake in the same
    generation as its thread: :func:`member_owner_key` folds the wake back to
    exactly that key, so ownership, ledger and the mirror target never cross
    from a private generation to the legacy one.

    A per-process counter alone restarts at 1 with the gateway, and a key that
    matches a persisted earlier wake would make ``get_or_create_slot`` REUSE that
    slot and its transcript. The millisecond stamp plus a three-digit sequence
    keeps the tail numeric (what :func:`member_owner_key` folds on) and distinct.
    """
    if not member_key.startswith(DM_SLOT_KEY_PREFIX) or WAKE_KEY_INFIX in member_key:
        raise InboxError(f"not a member thread key: {member_key!r}")
    stamp = int(time.time() * 1000)
    return f"{member_key}{WAKE_KEY_INFIX}{stamp}{int(seq) % 1000:03d}"


def member_thread_key(slug: str, binding: dict[str, Any] | None) -> str:
    """The thread key a member's wakes act as: the binding's ``slot_key`` when
    present (it carries the memory generation), else the bare derivation."""
    from kiro_crew.members import member_slot_key

    key = str((binding or {}).get("slot_key") or "")
    return key if key.startswith(DM_SLOT_KEY_PREFIX) else member_slot_key(validate_slug(slug))


# ------------------------------------------------------------------------- config


def inbox_model_enabled(slug: str) -> bool:
    """Whether ``members.<slug>.inbox_model`` is ``true`` in config.

    ``members`` is a modelled top-level section (a plain mapping, like
    ``hooks``); its shape is validated HERE, at the point of use. Fails CLOSED on
    any read problem: a member whose flag cannot be read behaves as today
    (session model).
    """
    try:
        from kiro_crew.config.loader import KiroCrewConfig

        section = getattr(KiroCrewConfig.load(), "members", None)
    except Exception:  # noqa: BLE001 - fail closed on any config read failure
        logger.warning("member inbox: config read failed; treating %s as session model", slug)
        return False
    if not isinstance(section, dict):
        return False
    entry = section.get(slug)
    return isinstance(entry, dict) and entry.get("inbox_model") is True


def member_inbox_setting(slug: str, key: str, default: Any) -> Any:
    """One optional per-member knob (``members.<slug>.<key>``), with a default."""
    try:
        from kiro_crew.config.loader import KiroCrewConfig

        section = getattr(KiroCrewConfig.load(), "members", None)
    except Exception:  # noqa: BLE001
        return default
    if not isinstance(section, dict):
        return default
    entry = section.get(slug)
    if not isinstance(entry, dict) or key not in entry:
        return default
    return entry[key]


def flagged_member_slugs() -> list[str]:
    """Every slug with ``inbox_model: true``, for the scheduler's startup scan."""
    try:
        from kiro_crew.config.loader import KiroCrewConfig

        section = getattr(KiroCrewConfig.load(), "members", None)
    except Exception:  # noqa: BLE001
        return []
    if not isinstance(section, dict):
        return []
    out: list[str] = []
    for slug, entry in section.items():
        if isinstance(entry, dict) and entry.get("inbox_model") is True:
            try:
                out.append(validate_slug(str(slug)))
            except Exception:  # noqa: BLE001 - skip a malformed slug, keep the rest
                logger.warning("member inbox: ignoring malformed member slug %r in config", slug)
    return out


# -------------------------------------------------------------------------- store


def _write_envelope(path: Path, env: Envelope) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write(path, json.dumps(env.to_dict(), ensure_ascii=False), fsync=True)
    fsync_dir(path.parent, best_effort=True)


def _read_envelope(path: Path) -> Envelope | None:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        logger.warning("member inbox: unreadable envelope %s", path.name, exc_info=True)
        return None
    if not isinstance(raw, dict) or not raw.get("id"):
        return None
    try:
        return Envelope.from_dict(raw)
    except TypeError:
        return None


def _unlink_quiet(path: Path) -> int:
    try:
        os.unlink(path)
    except FileNotFoundError:
        return 0
    except OSError:
        logger.warning("member inbox: could not prune %s", path.name, exc_info=True)
        return 0
    return 1


def compact_member(slug: str) -> int:
    """Apply retention to one member's inbox and outbox. Filesystem work.

    The outbox keeps every ``peer_dm`` mirror newer than the inbox's budget
    anchor (the newest ``user_dm``): those rows ARE the unattended-send count,
    and pruning them would refill a budget only the owner may refill.
    """
    inbox = InboxStore(slug)
    return inbox.compact() + OutboxStore(slug).compact(keep_peer_since=inbox.last_user_dm_at())


def _list_envelopes(dir_path: Path) -> list[Envelope]:
    if not dir_path.is_dir():
        return []
    out: list[Envelope] = []
    for p in sorted(dir_path.glob("env_*.json")):
        env = _read_envelope(p)
        if env is not None:
            out.append(env)
    return out


class InboxStore:
    """The pending / acked / dead-letter directories of one member."""

    def __init__(self, slug: str) -> None:
        self.slug = validate_slug(slug)
        self.root = member_store_dir(self.slug) / INBOX_DIR_NAME
        self.acked_dir = self.root / ACKED_DIR_NAME
        self.dead_dir = member_store_dir(self.slug) / DEAD_LETTER_DIR_NAME

    # -- paths
    def _pending_path(self, env_id: str) -> Path:
        return self.root / f"{validate_envelope_id(env_id)}.json"

    # -- writes
    def append(self, env: Envelope) -> Envelope:
        """Durably add *env* to the pending set. Returns it unchanged."""
        if env.to != f"member:{self.slug}":
            raise InboxError(f"envelope addressed to {env.to!r}, not member:{self.slug}")
        _write_envelope(self._pending_path(env.id), env)
        return env

    def mark_attempt(self, env_ids: list[str]) -> list[Envelope]:
        """Increment ``attempts`` on each pending envelope; return the new state.

        Called at the START of a wake, before the model runs, so a crash mid-wake
        leaves the count already raised — the redelivery bound cannot be
        evaded by dying before the bookkeeping.
        """
        out: list[Envelope] = []
        for env_id in env_ids:
            try:
                path = self._pending_path(env_id)
            except InboxError:
                continue
            env = _read_envelope(path)
            if env is None:
                continue
            env.attempts += 1
            _write_envelope(path, env)
            out.append(env)
        return out

    def ack(self, env_ids: list[str]) -> int:
        """Move envelopes out of the pending set. Returns how many were pending."""
        moved = 0
        self.acked_dir.mkdir(parents=True, exist_ok=True)
        for env_id in env_ids:
            try:
                path = self._pending_path(env_id)
            except InboxError:
                continue
            if not path.exists():
                continue
            # The TRANSITION is one rename -- atomic on the same filesystem, so a
            # crash leaves the envelope either pending or acked, never both (a
            # copy-then-unlink would redeliver work already acknowledged). The
            # ``acked_at`` stamp is metadata written after the fact; losing it to
            # a crash costs a timestamp, not a duplicate delivery.
            target = self.acked_dir / path.name
            os.replace(path, target)
            fsync_dir(self.root, best_effort=True)
            fsync_dir(self.acked_dir, best_effort=True)
            env = _read_envelope(target)
            if env is not None and env.acked_at is None:
                env.acked_at = now_iso()
                _write_envelope(target, env)
            moved += 1
        return moved

    def mark(self, env_id: str, **refs: Any) -> None:
        """Stamp extra ``refs`` onto an envelope in ANY state (metadata only)."""
        name = f"{validate_envelope_id(env_id)}.json"
        for d in (self.root, self.acked_dir, self.dead_dir):
            path = d / name
            env = _read_envelope(path) if path.exists() else None
            if env is not None:
                env.refs = {**env.refs, **refs}
                _write_envelope(path, env)
                return

    def view_pending(self) -> list[Envelope]:
        """Envelopes (pending or acked) whose thread row is not confirmed on disk."""
        return [e for e in self.pending() + self.acked() if e.refs.get(VIEW_REF) == VIEW_PENDING]

    def dead_letter(self, env_id: str, reason: str) -> Envelope | None:
        try:
            path = self._pending_path(env_id)
        except InboxError:
            return None
        if not path.exists():
            return None
        # Same discipline as ``ack``: rename is the transition, the reason is
        # metadata stamped afterwards.
        self.dead_dir.mkdir(parents=True, exist_ok=True)
        target = self.dead_dir / path.name
        os.replace(path, target)
        fsync_dir(self.root, best_effort=True)
        fsync_dir(self.dead_dir, best_effort=True)
        env = _read_envelope(target)
        if env is None:
            return None
        env.refs = {**env.refs, "dead_letter_reason": reason[:500], "dead_lettered_at": now_iso()}
        _write_envelope(target, env)
        return env

    # -- reads
    def has_record(self, env_id: str) -> bool:
        """Whether *env_id* exists in ANY state (pending, acked, dead-lettered).

        The delivery check :func:`kiro_crew.member_peer.reconcile_peer_sends`
        runs on a journaled send: a trace in any directory means the receiver
        got it, so the sender's mirror can be stamped delivered without a
        second append.
        """
        try:
            name = f"{validate_envelope_id(env_id)}.json"
        except InboxError:
            return False
        return any((d / name).exists() for d in (self.root, self.acked_dir, self.dead_dir))

    def pending(self) -> list[Envelope]:
        return _list_envelopes(self.root)

    def acked(self) -> list[Envelope]:
        return _list_envelopes(self.acked_dir)

    def dead_letters(self) -> list[Envelope]:
        return _list_envelopes(self.dead_dir)

    def has_pending_kind(self, kind: str) -> bool:
        return any(e.kind == kind for e in self.pending())

    def compact(self) -> int:
        """Prune acked and dead-letter rows beyond the retention caps.

        Oldest first (ids are time-ordered). The newest ``user_dm`` is always kept:
        it anchors the peer-DM budget, and losing it would make the budget count
        every retained send -- conservative, but a surprise. Pending rows are
        never touched. Returns how many files were removed.
        """
        removed = 0
        acked = self.acked()
        anchor = max(
            (e for e in acked if e.kind == "user_dm"), key=lambda e: e.created_at, default=None
        )
        for env in acked[: max(0, len(acked) - RETAIN_ACKED)]:
            if anchor is not None and env.id == anchor.id:
                continue
            removed += _unlink_quiet(self.acked_dir / f"{env.id}.json")
        dead = self.dead_letters()
        for env in dead[: max(0, len(dead) - RETAIN_DEAD_LETTERS)]:
            removed += _unlink_quiet(self.dead_dir / f"{env.id}.json")
        return removed

    def last_user_dm_at(self) -> str:
        """``created_at`` of the newest ``user_dm`` (the person typed), or ``""``.

        The reset anchor for the peer-DM budget (see :mod:`member_peer`). A
        ``session_dm`` -- another session's ``session_send`` -- is a different
        kind and never refills a member's budget.
        """
        newest = ""
        for env in self.pending() + self.acked():
            if env.kind == "user_dm" and env.created_at > newest:
                newest = env.created_at
        return newest


class OutboxStore:
    """The member's own rows: replies into the projection and mirrored sends."""

    def __init__(self, slug: str) -> None:
        self.slug = validate_slug(slug)
        self.root = member_store_dir(self.slug) / OUTBOX_DIR_NAME

    def append(
        self, *, kind: str, body: str, refs: dict[str, Any] | None = None, hop: int = 0
    ) -> Envelope:
        if not isinstance(body, str) or not body.strip():
            raise InboxError("outbox body is empty")
        if len(body) > MAX_BODY_CHARS:
            raise InboxError(f"outbox body exceeds {MAX_BODY_CHARS} characters")
        env = Envelope(
            id=new_envelope_id(),
            from_=f"member:{self.slug}",
            to=(
                f"member:{self.slug}"
                if kind == OUTBOX_REPLY_KIND
                else str((refs or {}).get("to", ""))
            ),
            kind=kind,
            body=body,
            hop=max(0, int(hop)),
            refs=dict(refs or {}),
        )
        _write_envelope(self.root / f"{env.id}.json", env)
        return env

    def rows(self) -> list[Envelope]:
        return _list_envelopes(self.root)

    def answered(self) -> set[str]:
        """Envelope ids a COMPLETED wake's ``reply`` row names in ``in_reply_to``.

        A reply row carries ``in_reply_to`` from the moment it is written, but it
        only counts here once the wake that wrote it ended cleanly and stamped it
        ``completed`` (:meth:`mark_completed`). The runner checks this set on
        redelivery: an envelope answered by a completed wake that then crashed
        before its ack is acked WITHOUT a second turn (the owner never reads the
        same answer twice), while an EARLY reply from a wake that then failed
        ("on it...") does not count -- that batch redelivers and runs again, with
        the early reply visible in the recent-exchange window.
        """
        out: set[str] = set()
        for env in self.rows():
            if env.kind != OUTBOX_REPLY_KIND or env.refs.get("completed") is not True:
                continue
            ids = env.refs.get("in_reply_to")
            if isinstance(ids, list):
                out.update(str(i) for i in ids)
        return out

    def mark_completed(self, wake_key: str) -> int:
        """Stamp ``completed: true`` on every ``reply`` row *wake_key* wrote.

        Called by the runner after the wake's turn ended cleanly and before the
        ack, so the completion record precedes the ack the same way the reply
        does. Returns how many rows were stamped.
        """
        n = 0
        for env in self.rows():
            if env.kind == OUTBOX_REPLY_KIND and env.refs.get("wake_key") == wake_key:
                if env.refs.get("completed") is not True:
                    self.mark(env.id, completed=True)
                n += 1
        return n

    def mark(self, env_id: str, **refs: Any) -> None:
        """Stamp extra ``refs`` onto an existing outbox row (metadata only)."""
        path = self.root / f"{validate_envelope_id(env_id)}.json"
        env = _read_envelope(path)
        if env is None:
            return
        env.refs = {**env.refs, **refs}
        _write_envelope(path, env)

    def view_pending(self) -> list[Envelope]:
        """Reply rows whose thread row is not confirmed on disk."""
        return [
            e
            for e in self.rows()
            if e.kind == OUTBOX_REPLY_KIND and e.refs.get(VIEW_REF) == VIEW_PENDING
        ]

    def peer_sends_since(self, since_iso: str) -> int:
        """How many ``peer_dm`` rows this member has sent after *since_iso*."""
        return sum(1 for e in self.rows() if e.kind == "peer_dm" and e.created_at > since_iso)

    def compact(self, *, keep_peer_since: str = "") -> int:
        """Prune outbox rows beyond ``RETAIN_OUTBOX``, oldest first.

        ``peer_dm`` rows newer than *keep_peer_since* (the member's budget
        anchor, see :func:`compact_member`) are never pruned: ``peer_sends_since``
        counts them, so evicting them under a run of newer ``reply`` rows would
        reset the unattended-send budget without the owner having written.
        """
        rows = self.rows()
        excess = max(0, len(rows) - RETAIN_OUTBOX)
        removed = 0
        for env in rows:
            if excess <= 0:
                break
            if env.kind == "peer_dm" and env.created_at > keep_peer_since:
                continue
            removed += _unlink_quiet(self.root / f"{env.id}.json")
            excess -= 1
        return removed


def recent_exchange(slug: str, limit: int = 6) -> list[Envelope]:
    """The last *limit* conversational rows: acked ``user_dm`` / ``peer_dm`` in,
    ``reply`` / ``peer_dm`` out -- what a wake needs to understand a follow-up
    ("yes, do that") whose antecedent was a previous wake's reply. Bounded so
    continuity does not become the transcript-as-context the model replaces."""
    inbox = InboxStore(slug)
    rows = [e for e in inbox.acked() if e.kind in ("user_dm", "peer_dm")]
    rows += [e for e in OutboxStore(slug).rows() if e.kind in (OUTBOX_REPLY_KIND, "peer_dm")]
    rows.sort(key=lambda e: (e.created_at, e.id))
    return rows[-limit:] if limit > 0 else rows
