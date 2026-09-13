"""Member inbox model, M0 + M1: store, peer admission, scheduler, wake runner, shim,
read marker / unread, worker_report production, member-mode predicate.

``KIROCREW_HOME`` is pinned to a per-test tmp dir by the autouse conftest
fixture, so every member directory here resolves under tmp. Config is stubbed
at the module seams (``inbox_model_enabled`` and friends) rather than by
writing a config file, because the flag lives in the loader's preserved
``members`` section and the loader is not what is under test.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from kiro_crew import member_inbox as mi
from kiro_crew import member_peer as mp
from kiro_crew import member_scheduler as ms
from kiro_crew.member_inbox import (
    Envelope,
    InboxStore,
    OutboxStore,
    make_envelope,
    member_owner_key,
    member_slug_from_key,
    projection,
    wake_slot_key_for,
)

FLAGGED = {"radar", "fixer", "scout"}

# The real config readers, captured before the autouse seam stubs replace them
# on the modules (TestConfigSections exercises the readers themselves).
_REAL_READERS = {
    "enabled": mi.inbox_model_enabled,
    "setting": mi.member_inbox_setting,
    "flagged": mi.flagged_member_slugs,
    "global": mp.peer_dm_enabled,
}


@pytest.fixture(autouse=True)
def _flag_members(monkeypatch):
    """Every module that reads the flag sees the same stub; peer knobs default."""
    enabled = lambda slug: slug in FLAGGED  # noqa: E731
    monkeypatch.setattr(mi, "inbox_model_enabled", enabled)
    monkeypatch.setattr(mp, "inbox_model_enabled", enabled)
    monkeypatch.setattr(ms, "inbox_model_enabled", enabled)
    monkeypatch.setattr(mi, "member_inbox_setting", lambda slug, key, default: default)
    monkeypatch.setattr(mp, "member_inbox_setting", lambda slug, key, default: default)
    monkeypatch.setattr(ms, "member_inbox_setting", lambda slug, key, default: default)
    monkeypatch.setattr(mp, "peer_dm_enabled", lambda: True)
    monkeypatch.setattr(
        mp,
        "read_dm_binding",
        lambda slug: {"member": f"{slug}-agent", "slug": slug} if slug in FLAGGED else None,
    )
    monkeypatch.setattr(mi, "flagged_member_slugs", lambda: sorted(FLAGGED))
    monkeypatch.setattr(ms, "flagged_member_slugs", lambda: sorted(FLAGGED))
    mp._rate.reset()
    ms.set_scheduler(None)
    yield
    ms.set_scheduler(None)


def _env(slug: str, kind: str = "user_dm", body: str = "hi", **kw: Any) -> Envelope:
    return make_envelope(
        to_slug=slug, kind=kind, body=body, from_=kw.pop("from_", mi.FROM_USER), **kw
    )


# ------------------------------------------------------------------------ keys


class TestKeys:
    @pytest.mark.parametrize(
        "key, owner",
        [
            ("member-radar", "member-radar"),
            ("member-radar.wake-7", "member-radar"),
            ("member-radar.memory-v2-abc.wake-3", "member-radar.memory-v2-abc"),
            ("member-radar.wake-x", "member-radar.wake-x"),
            ("chat-1-123", "chat-1-123"),
            ("", ""),
        ],
    )
    def test_owner_key_folds_only_a_numeric_wake_suffix(self, key, owner):
        assert member_owner_key(key) == owner

    @pytest.mark.parametrize(
        "key, slug",
        [
            ("member-radar", "radar"),
            ("dashboard_member-radar.wake-2", "radar"),
            ("dashboard:member-radar.memory-v2-abc", "radar"),
            ("chat-9", None),
            ("member-", None),
            (None, None),
        ],
    )
    def test_slug_from_key(self, key, slug):
        assert member_slug_from_key(key) == slug

    def test_wake_key_for_a_private_generation_stays_in_it(self):
        from kiro_crew.member_inbox import member_thread_key, wake_slot_key_for

        v2 = "member-radar.memory-v2-abc"
        k = wake_slot_key_for(v2, 3)
        assert k.startswith(v2 + ".wake-") and member_owner_key(k) == v2
        assert member_slug_from_key(k) == "radar"
        assert member_thread_key("radar", {"slot_key": v2}) == v2
        assert member_thread_key("radar", {"slot_key": ""}) == "member-radar"
        assert member_thread_key("radar", None) == "member-radar"
        with pytest.raises(mi.InboxError):
            wake_slot_key_for("chat-1-2", 1)

    def test_wake_key_round_trips_and_is_unique_across_restarts(self):
        k1 = wake_slot_key_for("member-radar", 1)
        assert member_owner_key(k1) == "member-radar" and member_slug_from_key(k1) == "radar"
        # a second process restarting its counter at 1 must not mint the same key
        import time

        time.sleep(0.002)
        assert wake_slot_key_for("member-radar", 1) != k1

    def test_ledger_key_folds_a_wake_to_its_member(self):
        from kiro_crew.session_ledger import ledger_key

        assert ledger_key("dashboard_member-radar.wake-4") == "member-radar"
        assert ledger_key("dashboard_chat-1-5") == "chat-1-5"


# ----------------------------------------------------------------------- store


class TestInboxStore:
    def test_ids_minted_in_one_millisecond_keep_append_order(self, monkeypatch):
        monkeypatch.setattr(mi.time, "time", lambda: 1_700_000_000.000)
        ids = [mi.new_envelope_id() for _ in range(50)]
        assert ids == sorted(ids) and len(set(ids)) == 50
        assert all(mi._ENVELOPE_ID_RE.match(i) for i in ids)

    def test_append_is_durable_and_ordered(self):
        store = InboxStore("radar")
        a = store.append(_env("radar", body="first"))
        b = store.append(_env("radar", kind="system", body="second", from_="system"))
        pending = store.pending()
        assert [e.id for e in pending] == [a.id, b.id]
        assert (store.root / f"{a.id}.json").exists()
        raw = json.loads((store.root / f"{a.id}.json").read_text())
        assert raw["from"] == "user" and raw["to"] == "member:radar" and raw["kind"] == "user_dm"

    def test_refuses_an_envelope_for_another_member(self):
        with pytest.raises(mi.InboxError):
            InboxStore("radar").append(_env("fixer"))

    def test_make_envelope_validates(self):
        with pytest.raises(mi.InboxError):
            make_envelope(to_slug="radar", kind="nope", body="x", from_="user")
        with pytest.raises(mi.InboxError):
            make_envelope(to_slug="radar", kind="user_dm", body="   ", from_="user")
        with pytest.raises(mi.InboxError):
            make_envelope(
                to_slug="radar", kind="user_dm", body="x" * (mi.MAX_BODY_CHARS + 1), from_="user"
            )

    def test_ack_moves_out_of_pending_and_records_time(self):
        store = InboxStore("radar")
        a = store.append(_env("radar"))
        assert store.ack([a.id]) == 1
        assert store.pending() == []
        acked = store.acked()
        assert [e.id for e in acked] == [a.id] and acked[0].acked_at
        assert store.ack([a.id]) == 0  # idempotent

    def test_ack_is_a_single_rename_not_copy_then_unlink(self, monkeypatch):
        """A crash between two writes must never leave an envelope both pending
        and acked; the transition is one os.replace."""
        store = InboxStore("radar")
        a = store.append(_env("radar"))
        moves: list[tuple[str, str]] = []
        real_replace = mi.os.replace

        def spy(src, dst):
            moves.append((str(src), str(dst)))
            return real_replace(src, dst)

        monkeypatch.setattr(mi.os, "replace", spy)
        monkeypatch.setattr(
            mi.os, "unlink", lambda *_: pytest.fail("unlink used for a state transition")
        )
        store.ack([a.id])
        # exactly ONE move of the pending file itself (atomic_write's own temp->final
        # replace for the acked_at stamp is a different source path)
        pending_path = str(store.root / f"{a.id}.json")
        assert [m for m in moves if m[0] == pending_path] == [
            (pending_path, str(store.acked_dir / f"{a.id}.json"))
        ]
        assert store.pending() == [] and len(store.acked()) == 1

    def test_mark_attempt_raises_count_before_the_model_runs(self):
        store = InboxStore("radar")
        a = store.append(_env("radar"))
        assert store.mark_attempt([a.id])[0].attempts == 1
        assert store.mark_attempt([a.id])[0].attempts == 2
        assert store.pending()[0].attempts == 2  # persisted, survives a re-read

    def test_dead_letter_moves_with_reason(self):
        store = InboxStore("radar")
        a = store.append(_env("radar"))
        moved = store.dead_letter(a.id, "crashed 3 wake(s)")
        assert moved is not None and moved.refs["dead_letter_reason"] == "crashed 3 wake(s)"
        assert store.pending() == [] and [e.id for e in store.dead_letters()] == [a.id]

    def test_store_is_a_top_level_masked_leaf_not_the_member_dir(self):
        from kiro_crew import sandbox
        from kiro_crew.config.paths import data_home
        from kiro_crew.members import member_dir
        from kiro_crew.security import paths as secpaths

        store = InboxStore("radar")
        leaf = (data_home() / mi.STORE_DIR_NAME).resolve()
        assert leaf.parent == data_home().resolve()  # top level: no agent-writable ancestor
        assert leaf in store.root.parents and leaf in OutboxStore("radar").root.parents
        assert member_dir("radar").resolve() not in store.root.parents
        assert (data_home() / "trust").resolve() not in store.root.parents
        # bind-masked in every sandbox mode AND fenced from agent file tools
        assert mi.STORE_DIR_NAME in sandbox._CREW_HIDDEN_LEAVES
        assert mi.STORE_DIR_NAME in secpaths._CREW_SECRET_LEAVES
        # materialised before every namespace spawn: the store is created lazily, and
        # a mask can only bind a path that exists when the sandbox starts
        assert mi.STORE_DIR_NAME in sandbox._CREW_PRECREATE_HIDDEN_DIR_LEAVES

    @pytest.mark.parametrize(
        "bad",
        [
            "env_/../../fixer/inbox/env_0000000000000_deadbeef",
            "env_x",
            "../x",
            "env_0000000000000_DEADBEEF",
            "",
        ],
    )
    def test_malformed_ids_never_touch_the_filesystem(self, bad):
        store = InboxStore("radar")
        a = store.append(_env("radar"))
        with pytest.raises(mi.InboxError):
            mi.validate_envelope_id(bad)
        assert (
            store.ack([bad]) == 0
            and store.mark_attempt([bad]) == []
            and store.dead_letter(bad, "x") is None
        )
        assert [e.id for e in store.pending()] == [a.id]

    def test_compaction_prunes_oldest_but_keeps_the_budget_anchor(self, monkeypatch):
        monkeypatch.setattr(mi, "RETAIN_ACKED", 3)
        monkeypatch.setattr(mi, "RETAIN_OUTBOX", 2)
        monkeypatch.setattr(mi, "RETAIN_DEAD_LETTERS", 1)
        store = InboxStore("radar")
        anchor = store.append(_env("radar", body="anchor"))
        others = [
            store.append(_env("radar", kind="system", body=f"s{i}", from_="system"))
            for i in range(5)
        ]
        store.ack([anchor.id] + [e.id for e in others])
        for i in range(3):
            d = store.append(_env("radar", kind="system", body=f"d{i}", from_="system"))
            store.dead_letter(d.id, "x")
        out = OutboxStore("radar")
        for i in range(4):
            out.append(kind=mi.OUTBOX_REPLY_KIND, body=f"r{i}")
        removed = mi.compact_member("radar")
        acked = store.acked()
        assert anchor.id in {e.id for e in acked}  # the oldest row, kept as the budget anchor
        # 6 acked, keep 3 -> the 3 oldest are candidates, the anchor among them is spared
        assert len(acked) == 4 and store.last_user_dm_at() == anchor.created_at
        assert len(store.dead_letters()) == 1 and len(out.rows()) == 2
        assert removed == 2 + 2 + 2
        assert store.pending() == []  # never touched

    def test_compaction_never_prunes_peer_sends_that_count_against_the_budget(self, monkeypatch):
        """Twelve peer sends followed by a run of replies: the replies are what
        compaction removes; the mirrors newer than the budget anchor stay, so
        `peer_sends_since` still counts them and the budget does not refill."""
        monkeypatch.setattr(mi, "RETAIN_OUTBOX", 5)
        inbox = InboxStore("radar")
        anchor = inbox.append(_env("radar", body="owner spoke"))
        inbox.ack([anchor.id])
        out = OutboxStore("radar")
        for i in range(4):
            out.append(kind="peer_dm", body=f"p{i}", refs={"to": "member:fixer"})
        for i in range(8):
            out.append(kind=mi.OUTBOX_REPLY_KIND, body=f"r{i}")
        removed = mi.compact_member("radar")
        rows = out.rows()
        assert removed == 7 and len(rows) == 5
        assert out.peer_sends_since(inbox.last_user_dm_at()) == 4  # budget records intact
        assert [e.body for e in rows if e.kind == mi.OUTBOX_REPLY_KIND] == ["r7"]
        # peer sends OLDER than the anchor are ordinary rows again: prunable
        inbox.ack([inbox.append(_env("radar", body="owner spoke again")).id])
        for i in range(3):
            out.append(kind=mi.OUTBOX_REPLY_KIND, body=f"n{i}")
        mi.compact_member("radar")
        assert out.peer_sends_since(inbox.last_user_dm_at()) == 0
        assert len(out.rows()) == 5

    def test_unreadable_file_is_skipped_not_fatal(self):
        store = InboxStore("radar")
        store.append(_env("radar"))
        store.root.joinpath("env_9999999999999_bad.json").write_text("{not json")
        assert len(store.pending()) == 1

    def test_last_user_dm_at_counts_only_the_person(self):
        store = InboxStore("radar")
        assert store.last_user_dm_at() == ""
        s = store.append(_env("radar", kind="session_dm", from_="session:chat-1-9"))
        assert store.last_user_dm_at() == ""  # another session's send does not anchor
        u = store.append(_env("radar"))
        assert store.last_user_dm_at() == u.created_at
        store.ack([u.id, s.id])
        assert store.last_user_dm_at() == u.created_at  # acked still anchors


class TestOutboxAndProjection:
    def test_peer_sends_since(self):
        out = OutboxStore("radar")
        first = out.append(kind="peer_dm", body="x", refs={"to": "member:fixer"})
        out.append(kind=mi.OUTBOX_REPLY_KIND, body="reply")
        out.append(kind="peer_dm", body="y", refs={"to": "member:fixer"})
        assert out.peer_sends_since("") == 2
        assert out.peer_sends_since(first.created_at) == 1

    def test_projection_marks_a_failed_peer_mirror_undeliverable(self):
        out = OutboxStore("radar")
        ok = out.append(
            kind="peer_dm", body="x", refs={"to": "member:fixer", "delivery": "delivered"}
        )
        bad = out.append(
            kind="peer_dm", body="y", refs={"to": "member:fixer", "delivery": "failed"}
        )
        reply = out.append(kind=mi.OUTBOX_REPLY_KIND, body="r")
        state = {r["id"]: r["state"] for r in projection("radar")}
        assert (state[ok.id], state[bad.id], state[reply.id]) == ("sent", "dead", "sent")


# ------------------------------------------------------------ read marker (M1)


class TestReadMarkerAndUnread:
    def test_unread_counts_only_outbox_rows_newer_than_the_marker(self):
        InboxStore("radar").append(_env("radar", body="hello"))
        out = OutboxStore("radar")
        r1 = out.append(kind=mi.OUTBOX_REPLY_KIND, body="first reply")
        assert mi.read_marker("radar") == {"last_read_id": "", "last_read_at": ""}
        assert mi.unread_count("radar") == 1  # the user_dm is not news to its author
        rows = projection("radar")
        marker = mi.write_read_marker("radar", rows[-1])
        assert marker["last_read_id"] == r1.id
        assert mi.unread_count("radar") == 0
        r2 = out.append(kind="peer_dm", body="to fixer", refs={"to": "member:fixer"})
        InboxStore("radar").append(_env("radar", kind="system", body="notice", from_="system"))
        assert mi.unread_count("radar") == 1  # the mirrored send counts, the system row does not
        assert (
            mi.write_read_marker("radar", projection("radar")[-1])["last_read_at"] >= r2.created_at
        )

    def test_marker_is_monotone_and_ignores_an_empty_projection(self):
        out = OutboxStore("radar")
        a = out.append(kind=mi.OUTBOX_REPLY_KIND, body="a")
        b = out.append(kind=mi.OUTBOX_REPLY_KIND, body="b")
        rows = projection("radar")
        newest = mi.write_read_marker("radar", rows[-1])
        assert newest["last_read_id"] == b.id
        older = mi.write_read_marker("radar", rows[0])
        assert older == newest, "moving the marker backwards is refused"
        assert mi.write_read_marker("radar", None) == newest
        assert a.id != b.id

    def test_malformed_marker_reads_as_never_read(self):
        path = mi.member_store_dir("radar") / mi.READ_MARKER_FILE_NAME
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{not json")
        assert mi.read_marker("radar") == {"last_read_id": "", "last_read_at": ""}
        OutboxStore("radar").append(kind=mi.OUTBOX_REPLY_KIND, body="x")
        assert mi.unread_count("radar") == 1

    def test_concurrent_marker_writes_never_move_it_backwards(self):
        import threading

        out = OutboxStore("radar")
        rows_env = [out.append(kind=mi.OUTBOX_REPLY_KIND, body=f"r{i}") for i in range(5)]
        by_id = {r["id"]: r for r in projection("radar")}
        order = [4, 0, 2, 1, 3] * 6
        threads = [
            threading.Thread(target=mi.write_read_marker, args=("radar", by_id[rows_env[i].id]))
            for i in order
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert mi.read_marker("radar")["last_read_id"] == rows_env[4].id
        assert mi.unread_count("radar") == 0

    def test_served_window_never_starts_after_the_first_unread_row(self):
        out = OutboxStore("radar")
        rows_env = [out.append(kind=mi.OUTBOX_REPLY_KIND, body=f"r{i}") for i in range(6)]
        rows = projection("radar")
        no_marker = {"last_read_id": "", "last_read_at": ""}
        # Nothing read: the window is everything, whatever the limit.
        assert mi.served_window(rows, no_marker, 2) == rows
        # Read up to r1: the window starts at r2 even though the limit is 2.
        marker = {"last_read_id": rows_env[1].id, "last_read_at": rows_env[1].created_at}
        win = mi.served_window(rows, marker, 2)
        assert [r["id"] for r in win] == [e.id for e in rows_env[2:]]
        # Everything read: plain newest-N window.
        marker = {"last_read_id": rows_env[5].id, "last_read_at": rows_env[5].created_at}
        assert [r["id"] for r in mi.served_window(rows, marker, 2)] == [
            rows_env[4].id,
            rows_env[5].id,
        ]
        assert mi.served_window(rows, marker, 0) == rows


# ------------------------------------------------------- projection endpoint (M1)


def _projection_app() -> Any:
    from types import SimpleNamespace

    from aiohttp import web

    from kiro_crew.dashboard.handlers.members import api_member_projection, api_member_read

    @web.middleware
    async def _owner(request, handler):
        # The standalone-local owner subject the owner gate accepts when no
        # owner_id is configured; `app == ""` is the dashboard (non-app) caller.
        request["app"] = ""
        request["user"] = "local-app"
        return await handler(request)

    app = web.Application(middlewares=[_owner])
    app["state"] = SimpleNamespace(owner_id="")
    app.router.add_get("/api/members/{slug}/projection", api_member_projection)
    app.router.add_post("/api/members/{slug}/read", api_member_read)
    return app


class TestProjectionEndpoint:
    @pytest.mark.asyncio
    async def test_successful_reads_and_marker_writes_are_audited(self, monkeypatch):
        """Refusals on these routes are audited by the gates; a successful owner
        read of the projection and a marker write must leave an `allowed` SEL
        row too, or the owner's trail shows only what was denied."""
        from types import SimpleNamespace

        from aiohttp.test_utils import TestClient, TestServer

        import kiro_crew.dashboard.handlers as handlers_pkg

        rows: list[dict[str, Any]] = []
        fake = SimpleNamespace(log_api_access=lambda **kw: rows.append(kw))
        monkeypatch.setattr(handlers_pkg, "sel", lambda: fake)
        inbox = InboxStore("radar")
        a = inbox.append(_env("radar", body="hello"))
        inbox.ack([a.id])
        async with TestClient(TestServer(_projection_app())) as client:
            assert (await client.get("/api/members/radar/projection")).status == 200
            resp = await client.post("/api/members/radar/read", json={"last_read_id": a.id})
            assert resp.status == 200
        allowed = [(r["operation"], r["outcome"], r["resources"]) for r in rows]
        assert ("members.projection", "allowed", "slug=radar") in allowed
        assert ("members.read", "allowed", "slug=radar") in allowed

    @pytest.mark.asyncio
    async def test_every_string_in_a_row_is_redacted_refs_included(self):
        """`refs` is the model's, stored verbatim; it crosses the same boundary as `body`.

        A worker that puts a credential in a metadata field (``refs``) must
        get the same treatment as one that puts it in the body -- the
        projection response is what the dashboard renders, and the redaction
        chain runs recursively over the whole row, not over ``body`` alone.
        """
        from aiohttp.test_utils import TestClient, TestServer

        secret = "AKIAIOSFODNN7EXAMPLE"
        OutboxStore("radar").append(
            kind="peer_dm",
            body=f"token {secret} in body",
            refs={
                "to": "member:fixer",
                "note": f"key={secret}",
                "nested": [f"{secret}", {f"k-{secret}": "v"}],
                f"key-{secret}": "a credential used as a KEY, not a value",
            },
        )
        async with TestClient(TestServer(_projection_app())) as client:
            resp = await client.get("/api/members/radar/projection")
            assert resp.status == 200
            data = await resp.json()
        assert data["inbox_model"] is True and len(data["rows"]) == 1
        assert "AKIA" not in json.dumps(data["rows"]), data["rows"]
        row = data["rows"][0]
        assert row["refs"]["to"] == "member:fixer"  # non-secret metadata survives
        assert "key=" in row["refs"]["note"] and len(row["refs"]["nested"]) == 2
        # keys are model text too: the credential-bearing keys are redacted
        assert all("AKIA" not in k for k in row["refs"]) and len(row["refs"]) == 4
        assert all("AKIA" not in k for k in row["refs"]["nested"][1])

    @pytest.mark.asyncio
    async def test_read_without_a_named_row_is_refused_never_everything_shown(self):
        """The read marker only moves forward: a body that is missing, empty, not a
        JSON object, or without `last_read_id` must be a 400, never "mark the
        newest served row read" -- that row may have raced in after the client's
        last render, and a marker past it clears an unread reply for good."""
        from aiohttp.test_utils import TestClient, TestServer

        from kiro_crew.member_inbox import read_marker

        inbox = InboxStore("radar")
        row = inbox.append(_env("radar", body="unread"))
        inbox.ack([row.id])
        async with TestClient(TestServer(_projection_app())) as client:
            deep = (
                b"[" * 30_000 + b"]" * 30_000
            )  # within the byte cap, blows the parser stack: 400 not 500
            for raw in (b"not json", b"[1, 2]", b'"env_x"', b"42", deep):
                resp = await client.post(
                    "/api/members/radar/read",
                    data=raw,
                    headers={"Content-Type": "application/json"},
                )
                assert resp.status == 400, raw[:20]
                assert (await resp.json())["code"] in ("invalid_json", "body_not_object")
            # no body, an empty object, a wrong-typed id: refused, the marker untouched
            assert (await client.post("/api/members/radar/read")).status == 400
            resp = await client.post("/api/members/radar/read", json={})
            assert resp.status == 400 and (await resp.json())["code"] == "invalid_marker"
            resp = await client.post("/api/members/radar/read", json={"last_read_id": 7})
            assert resp.status == 400 and (await resp.json())["code"] == "invalid_marker"
            assert read_marker("radar")["last_read_id"] == ""  # nothing was marked read
            # the row the client rendered last, by id, is what moves the marker
            resp = await client.post("/api/members/radar/read", json={"last_read_id": row.id})
            assert resp.status == 200
        assert read_marker("radar")["last_read_id"] == row.id

    @pytest.mark.asyncio
    async def test_an_unflagged_member_answers_inbox_model_false_not_an_error(self):
        from aiohttp.test_utils import TestClient, TestServer

        async with TestClient(TestServer(_projection_app())) as client:
            resp = await client.get("/api/members/nobody/projection")
            assert resp.status == 200
            data = await resp.json()
        assert data == {
            "slug": "nobody",
            "inbox_model": False,
            "rows": [],
            "unread": 0,
            "marker": None,
        }


# ------------------------------------------------------------ worker_report (M1)


class _WorkerSlot:
    def __init__(self, key: str, created_by: str, mode: str = "", reply: str = "done") -> None:
        self.key = key
        self._created_by = created_by
        self.mode = mode
        self.title = "Triage #42"
        self.messages = [{"role": "user", "content": "go"}]
        if reply:
            self.messages.append({"role": "assistant", "content": reply})


class TestWorkerReport:
    @pytest.fixture(autouse=True)
    def _wake_module(self, monkeypatch):
        from kiro_crew.dashboard import member_wake as mw

        monkeypatch.setattr(mw, "inbox_model_enabled", lambda slug: slug in FLAGGED)
        self.mw = mw

    def _report(self, slot) -> str | None:
        """The two halves the queue-cycle end runs: snapshot on the loop, write off it."""
        turn = self.mw.snapshot_worker_turn(slot)
        return None if turn is None else self.mw.report_worker_turn(object(), turn)

    def test_a_worker_created_by_a_wake_reports_to_the_member(self):
        notified: list[tuple[str, str]] = []
        sched = type(
            "S",
            (),
            {"notify": lambda self, slug, immediate=False: notified.append((slug, immediate))},
        )()
        ms.set_scheduler(sched)
        slot = _WorkerSlot("chat-7-1", created_by="member-radar.wake-3", reply="PR opened.")
        env_id = self._report(slot)
        pending = InboxStore("radar").pending()
        assert env_id and [e.id for e in pending] == [env_id]
        env = pending[0]
        assert env.kind == "worker_report" and env.from_ == "session:chat-7-1"
        assert env.body == "[Triage #42]\nPR opened."
        assert env.refs["session_key"] == "chat-7-1"
        assert notified == [("radar", False)]  # coalesced, not immediate

    @pytest.mark.parametrize(
        "slot",
        [
            _WorkerSlot("member-radar.wake-2", created_by="member-radar", mode="member-wake"),
            _WorkerSlot("member-radar", created_by="member-radar", mode="member"),
            _WorkerSlot("chat-1-1", created_by="chat-0-0"),
            _WorkerSlot("chat-1-2", created_by=""),
        ],
        ids=["own-wake", "own-thread", "human-created", "no-creator"],
    )
    def test_nothing_is_written_when_the_slot_reports_to_nobody(self, slot):
        assert self.mw.snapshot_worker_turn(slot) is None
        assert InboxStore("radar").pending() == []

    def test_unflagged_creator_is_decided_off_loop_and_gets_no_report(self, monkeypatch):
        """The snapshot (on the loop) does not read config; the writer does, and an
        unflagged creator gets no file. The on-loop half must stay I/O-free."""
        slot = _WorkerSlot("chat-1-3", created_by="member-nobody")
        turn = self.mw.snapshot_worker_turn(slot)
        assert turn is not None and turn.slug == "nobody"
        assert self.mw.report_worker_turn(object(), turn) is None
        assert not (mi.member_store_dir("nobody") / "inbox").exists()

        def blow(slug):
            raise AssertionError("config read on the loop")

        monkeypatch.setattr(self.mw, "inbox_model_enabled", blow)
        assert self.mw.snapshot_worker_turn(slot) is not None  # no config read here

    def test_reply_tail_is_bounded(self):
        slot = _WorkerSlot("chat-9-9", created_by="member-fixer", reply="x" * 10_000)
        self._report(slot)
        (env,) = InboxStore("fixer").pending()
        assert len(env.body) <= self.mw.WORKER_REPORT_MAX_CHARS + len("[Triage #42]\n")
        assert env.body.endswith("x") and env.refs["outcome"] == "ok"

    def test_a_tool_using_turn_reports_every_assistant_segment(self):
        """Text, then a tool call, then more text: each segment is its own
        assistant row, and the report carries all of them in order -- not only
        the last one, which would silently drop what the worker said first."""
        slot = _WorkerSlot("chat-8-0", created_by="member-radar", reply="Reading the failing test.")
        slot.messages += [
            {"role": "tool", "content": "pytest ... 1 failed"},
            {"role": "assistant", "content": "The fixture is stale; patching it."},
            {"role": "tool", "content": "edit applied"},
            {"role": "assistant", "content": "Fixed and pushed as abc123."},
        ]
        self._report(slot)
        (env,) = InboxStore("radar").pending()
        assert env.refs["outcome"] == "ok"
        assert env.body.index("Reading the failing test.") < env.body.index("fixture is stale")
        assert env.body.endswith("Fixed and pushed as abc123.")

    def test_a_failed_turn_reports_the_failure_not_the_previous_reply(self):
        """Prior turn succeeded, this turn raised an error row before replying."""
        slot = _WorkerSlot("chat-8-1", created_by="member-radar", reply="first reply")
        slot.messages += [
            {"role": "user", "content": "again"},
            {"role": "error", "content": "model timed out"},
            {"role": "done", "content": ""},
        ]
        env_id = self._report(slot)
        (env,) = [e for e in InboxStore("radar").pending() if e.id == env_id]
        assert env.refs["outcome"] == "failed"
        assert "model timed out" in env.body and "turn failed" in env.body
        assert "first reply" not in env.body

    @pytest.mark.parametrize("opener", ["nudge", "subagent"])
    def test_a_turn_opened_by_a_nudge_or_subagent_row_reports_only_its_own_reply(self, opener):
        """Auto-nudge cycles and subagent completions open turns without a `user`
        row; the report must start at THAT boundary, or the previous turn's reply
        rides along in this turn's `worker_report`."""
        slot = _WorkerSlot("chat-8-3", created_by="member-radar", reply="previous reply")
        slot.messages += [
            {"role": opener, "content": "[cycle 2]" if opener == "nudge" else "[Subagent done]"},
            {"role": "assistant", "content": "this turn only"},
            {"role": "done", "content": ""},
        ]
        env_id = self._report(slot)
        (env,) = [e for e in InboxStore("radar").pending() if e.id == env_id]
        assert env.refs["outcome"] == "ok"
        assert env.body.endswith("this turn only") and "previous reply" not in env.body
        # and the same boundary bounds a FAILED turn's report
        slot.messages += [
            {"role": opener, "content": "[cycle 3]"},
            {"role": "error", "content": "model timed out"},
        ]
        env_id2 = self._report(slot)
        (env2,) = [e for e in InboxStore("radar").pending() if e.id == env_id2]
        assert env2.refs["outcome"] == "failed" and "this turn only" not in env2.body

    def test_a_turn_with_no_new_assistant_text_is_reported_as_failed(self):
        slot = _WorkerSlot("chat-8-2", created_by="member-radar", reply="old reply")
        slot.messages += [{"role": "user", "content": "again"}, {"role": "done", "content": ""}]
        env_id = self._report(slot)
        (env,) = [e for e in InboxStore("radar").pending() if e.id == env_id]
        assert env.refs["outcome"] == "failed" and "without a reply" in env.body
        assert "old reply" not in env.body
        # And a fresh turn with a fresh reply reports THAT reply, nothing older.
        slot.messages += [
            {"role": "user", "content": "once more"},
            {"role": "assistant", "content": "new"},
        ]
        env_id2 = self._report(slot)
        (env2,) = [e for e in InboxStore("radar").pending() if e.id == env_id2]
        assert env2.refs["outcome"] == "ok" and env2.body.endswith("new")

    def test_the_report_describes_the_turn_that_ended_not_the_prompt_that_followed(self):
        """The snapshot is taken while the slot is busy; the write happens later.

        Between ``chat_done`` and the writer thread's read, the next prompt can
        land on ``slot.messages``. The snapshot the writer receives is immutable,
        so a ``user`` row (or a whole failed second turn) appended after it does
        not turn the finished turn's "ok" into "failed" or swap its reply.
        """
        slot = _WorkerSlot("chat-r-1", created_by="member-radar", reply="PR opened.")
        turn = self.mw.snapshot_worker_turn(slot)
        assert turn is not None and turn.outcome == "ok"
        slot.messages += [
            {"role": "user", "content": "next prompt lands first"},
            {"role": "error", "content": "and fails"},
        ]
        env_id = self.mw.report_worker_turn(object(), turn)
        (env,) = [e for e in InboxStore("radar").pending() if e.id == env_id]
        assert env.refs["outcome"] == "ok" and env.body.endswith("PR opened.")
        assert "next prompt" not in env.body and "and fails" not in env.body
        # The snapshot is frozen: nothing downstream can edit what was decided.
        from dataclasses import FrozenInstanceError

        with pytest.raises(FrozenInstanceError):
            turn.outcome = "failed"  # type: ignore[misc]

    def test_worker_report_wakes_are_budgeted_until_the_owner_speaks(self):
        notified: list[str] = []
        ms.set_scheduler(
            type("S", (), {"notify": lambda self, slug, **kw: notified.append(slug)})()
        )
        budget = self.mw.WORKER_REPORT_BUDGET
        for i in range(budget):
            self._report(_WorkerSlot(f"chat-b-{i}", created_by="member-radar"))
        assert len(notified) == budget
        inbox = InboxStore("radar")
        assert not any(e.kind == "system" for e in inbox.pending())
        # The report past the budget is written, does not wake, and the member
        # is told exactly once.
        over = self._report(_WorkerSlot("chat-b-over", created_by="member-radar"))
        assert over is not None and len(notified) == budget
        notices = [e for e in inbox.pending() if e.kind == "system"]
        assert len(notices) == 1 and notices[0].refs["reason"] == "worker_report_budget"
        self._report(_WorkerSlot("chat-b-over2", created_by="member-radar"))
        assert len(notified) == budget
        assert sum(1 for e in inbox.pending() if e.kind == "system") == 1
        # Only the person refills it: a user_dm resets the count, a system or
        # peer envelope does not.
        inbox.append(_env("radar", kind="system", body="notice", from_="system"))
        self._report(_WorkerSlot("chat-b-over3", created_by="member-radar"))
        assert len(notified) == budget
        inbox.append(_env("radar", body="owner here"))
        self._report(_WorkerSlot("chat-b-after", created_by="member-radar"))
        assert len(notified) == budget + 1

    def test_concurrent_reports_count_each_other_once_at_the_budget_boundary(self):
        """Two workers finishing together at budget-1 must not both slip under it.

        The append, the count and the one-time notice run under the member's
        report lock: exactly ``WORKER_REPORT_BUDGET`` reports wake the member,
        the one past it does not, and the notice is written once -- whatever
        order the two threads land in.
        """
        import threading

        notified: list[str] = []
        ms.set_scheduler(
            type("S", (), {"notify": lambda self, slug, **kw: notified.append(slug)})()
        )
        budget = self.mw.WORKER_REPORT_BUDGET
        for i in range(budget - 1):
            self._report(_WorkerSlot(f"chat-c-{i}", created_by="member-radar"))
        assert len(notified) == budget - 1
        turns = [
            self.mw.snapshot_worker_turn(_WorkerSlot(f"chat-c-race-{i}", created_by="member-radar"))
            for i in range(2)
        ]
        gate = threading.Barrier(2)

        def _go(turn):
            gate.wait()
            self.mw.report_worker_turn(object(), turn)

        threads = [threading.Thread(target=_go, args=(t,)) for t in turns]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        inbox = InboxStore("radar")
        reports = [e for e in inbox.pending() if e.kind == "worker_report"]
        assert len(reports) == budget + 1  # every report is written
        assert len(notified) == budget  # exactly one of the two got no wake
        notices = [e for e in inbox.pending() if e.kind == "system"]
        assert len(notices) == 1 and notices[0].refs["reason"] == "worker_report_budget"

    @pytest.mark.asyncio
    async def test_a_queued_successor_still_yields_a_report_for_the_finished_turn(self, tmp_path):
        """The report hook runs at every turn end, before the queue drain.

        ``_finish_queue_cycle`` is skipped when a queued successor starts; the
        snapshot must not live there. Two real ``_run_chat`` turns on a worker
        slot -- the second dequeued by the first's teardown -- yield two
        reports, and the first names the first turn's reply, not the second's.
        """
        from unittest.mock import AsyncMock, MagicMock

        from chat_test_helpers import _make_state

        from kiro_crew.dashboard.chat_runner import _run_chat
        from kiro_crew.providers.base import EVENT_COMPLETE, EVENT_TEXT_CHUNK, LLMEvent

        async def stream(stream_message: str):
            yield LLMEvent(kind=EVENT_TEXT_CHUNK, text=f"response to {stream_message}")
            yield LLMEvent(kind=EVENT_COMPLETE)

        client = MagicMock()
        client.stream = stream
        client.stream_command = stream
        client.context_usage_pct = MagicMock(return_value=1.0)
        state = _make_state(tmp_path)
        state.broadcast_ws = MagicMock()
        state.push_slots_update = MagicMock()
        state.context_builder = None
        state.consolidator = None
        state._hook_store = None
        state._yolo = False
        state._background_tasks = set()
        state.sessions.get_or_create = AsyncMock(return_value=(client, True, False))
        slot = state.get_or_create_slot("chat-q-1")
        slot._titled = True
        slot._created_by = "member-radar"
        slot.queue_append("second message")

        await _run_chat(state, slot, "first message")
        assert slot.task is not None
        await slot.task  # the dequeued successor
        assert slot._queue == []
        # The reports are fire-and-forget off-loop writes; let them land.
        await asyncio.gather(*list(state._background_tasks), return_exceptions=True)

        reports = sorted(
            (e for e in InboxStore("radar").pending() if e.kind == "worker_report"),
            key=lambda e: (e.created_at, e.id),
        )
        assert [e.refs["outcome"] for e in reports] == ["ok", "ok"]
        assert reports[0].body.endswith("response to first message")
        assert "second message" in reports[1].body
        assert "second message" not in reports[0].body

    def test_a_turn_awaiting_a_synthetic_recovery_is_not_reported_yet(self, monkeypatch):
        """An empty or transient-failed response queues a runner-authored recovery
        nudge; that turn is not terminal, so no "failed" report (and no wake) may be
        filed before the recovery runs. The recovery turn reports its own outcome."""
        from kiro_crew.dashboard import chat_runner as cr
        from kiro_crew.dashboard.chat_utils import SYNTHETIC_RECOVERY_KIND

        reported: list[Any] = []
        monkeypatch.setattr(self.mw, "snapshot_worker_turn", lambda slot: reported.append(slot))
        slot = _WorkerSlot("chat-r-1", created_by="member-radar", reply="")
        slot._queue = [{"content": "continue", "kind": SYNTHETIC_RECOVERY_KIND}]
        cr._report_worker_turn_end(object(), slot)
        assert reported == []  # deferred: the recovery turn reports
        slot._queue = [{"content": "a real follow-up", "kind": ""}]
        cr._report_worker_turn_end(object(), slot)
        assert reported == [slot]  # an ordinary queued successor does not defer


# ------------------------------------------------------------ member modes (M1)


class TestMemberModePredicate:
    def test_both_member_modes_and_nothing_else(self):
        from kiro_crew.members import DM_SLOT_MODE, WAKE_SLOT_MODE, is_member_mode

        assert (DM_SLOT_MODE, WAKE_SLOT_MODE) == ("member", "member-wake")
        assert is_member_mode("member") and is_member_mode("member-wake")
        assert not is_member_mode("") and not is_member_mode(None) and not is_member_mode("crew")

    def test_external_arm_is_refused_on_a_wake_slot(self):
        from kiro_crew.autonudge_authz import _EXTERNAL_ARM_REFUSED_MODES

        assert "member-wake" in _EXTERNAL_ARM_REFUSED_MODES


# ------------------------------------------------------------------ peer_send


class TestPeerSend:
    def _send(self, frm="radar", to="fixer", hop=0, body="hello", **kw):
        return mp.peer_send(
            caller_key=f"member-{frm}.wake-1",
            sender_slug=frm,
            target=to,
            body=body,
            inbound_hop=hop,
            **kw,
        )

    def test_writes_receiver_inbox_and_sender_mirror(self):
        r = self._send()
        assert r["ok"] and r["hop"] == 1
        inbound = InboxStore("fixer").pending()
        assert (
            len(inbound) == 1
            and inbound[0].kind == "peer_dm"
            and inbound[0].from_ == "member:radar"
        )
        assert inbound[0].hop == 1 and inbound[0].refs["pair_id"] == "fixer|radar"
        mirror = OutboxStore("radar").rows()
        assert (
            len(mirror) == 1
            and mirror[0].kind == "peer_dm"
            and mirror[0].refs["envelope_id"] == inbound[0].id
        )

    def test_model_refs_cannot_carry_reserved_keys(self):
        """`refs` is the model's, but the journal / idempotency keys are the gateway's:
        a send that tries to preset `delivery`, `completed` or `pair_id` gets them
        stripped, and the stores' own values land."""
        self._send(
            refs={"note": "keep", "delivery": "delivered", "completed": True, "pair_id": "x|y"}
        )
        env = InboxStore("fixer").pending()[0]
        assert env.refs == {"note": "keep", "pair_id": "fixer|radar"}
        mirror = OutboxStore("radar").rows()[0]
        assert mirror.refs["delivery"] == mp.DELIVERY_DELIVERED and "completed" not in mirror.refs
        assert mi.strip_reserved_refs({"completed": True, "a": 1}) == {"a": 1}
        assert mi.strip_reserved_refs("not a mapping") == {}

    def test_accepts_member_key_spelling(self):
        assert self._send(to="member-fixer")["to"] == "fixer"

    @pytest.mark.parametrize("target", ["nobody-here!!", "", "no spaces here", "UPPER"])
    def test_unknown_target(self, target):
        with pytest.raises(mp.PeerSendError) as exc:
            self._send(to=target)
        assert exc.value.code == "peer_dm_target_unknown" and exc.value.status == 404

    def test_self_target(self):
        with pytest.raises(mp.PeerSendError) as exc:
            self._send(to="radar")
        assert exc.value.code == "peer_dm_self_target"

    def test_flagged_receiver_without_a_binding_is_unreachable(self, monkeypatch):
        """The flag is a config line; a wake needs the binding. A slug whose
        binding is gone must not take an envelope it can never drain."""
        monkeypatch.setattr(mp, "read_dm_binding", lambda slug: None)
        with pytest.raises(mp.PeerSendError) as exc:
            self._send()
        assert exc.value.code == "peer_dm_target_unknown" and exc.value.status == 404
        assert InboxStore("fixer").pending() == [] and OutboxStore("radar").rows() == []

    def test_unflagged_receiver(self):
        with pytest.raises(mp.PeerSendError) as exc:
            self._send(to="scribe")
        assert exc.value.code == "peer_dm_target_not_flagged" and exc.value.status == 409

    def test_unflagged_sender_keeps_the_creator_fence(self):
        """A member still on the session model has today's reach, not the peer admission."""
        with pytest.raises(mp.PeerSendError) as exc:
            self._send(frm="scribe", to="fixer")
        assert exc.value.code == "peer_dm_sender_not_flagged" and exc.value.status == 409
        assert InboxStore("fixer").pending() == []

    def test_global_switch(self, monkeypatch):
        monkeypatch.setattr(mp, "peer_dm_enabled", lambda: False)
        with pytest.raises(mp.PeerSendError) as exc:
            self._send()
        assert exc.value.code == "peer_dm_disabled"

    def test_opt_out_either_side(self, monkeypatch):
        def setting(slug, key, default):
            if key == "peer_dm" and slug == "fixer":
                return {"accept": False}
            return default

        monkeypatch.setattr(mp, "member_inbox_setting", setting)
        with pytest.raises(mp.PeerSendError) as exc:
            self._send()
        assert exc.value.code == "peer_dm_opted_out" and "fixer" in str(exc.value)

        def setting2(slug, key, default):
            if key == "peer_dm" and slug == "radar":
                return {"send": False}
            return default

        monkeypatch.setattr(mp, "member_inbox_setting", setting2)
        with pytest.raises(mp.PeerSendError) as exc:
            self._send()
        assert exc.value.code == "peer_dm_opted_out" and "radar" in str(exc.value)

    def test_causal_hop_is_inherited_across_a_relay(self):
        """A -> B (1), B -> C acting on it (2), C -> A (3): one chain, not three."""
        r1 = self._send("radar", "fixer")
        r2 = self._send("fixer", "scout", hop=InboxStore("fixer").pending()[0].hop)
        r3 = self._send("scout", "radar", hop=InboxStore("scout").pending()[0].hop)
        assert (r1["hop"], r2["hop"], r3["hop"]) == (1, 2, 3)

    def test_hop_cap_refuses_and_does_not_write(self):
        with pytest.raises(mp.PeerSendError) as exc:
            self._send(hop=mp.DEFAULT_MAX_HOPS)
        assert exc.value.code == "peer_dm_hop_limit" and exc.value.status == 429
        assert InboxStore("fixer").pending() == [] and OutboxStore("radar").rows() == []

    def test_hop_chain_restarts_on_a_wake_with_no_inbound_peer(self):
        assert self._send(hop=0)["hop"] == 1
        assert self._send(hop=mp.DEFAULT_MAX_HOPS - 1)["hop"] == mp.DEFAULT_MAX_HOPS

    def test_budget_exhausts_and_only_the_person_refills_it(self, monkeypatch):
        monkeypatch.setattr(mp, "RATE_WINDOW_SECS", 0.0)  # rate never binds here
        for _ in range(mp.DEFAULT_MAX_UNATTENDED_SENDS):
            self._send()
        with pytest.raises(mp.PeerSendError) as exc:
            self._send()
        assert exc.value.code == "peer_dm_budget_exhausted" and exc.value.status == 429
        # neither time, nor a timer, a peer, a system row, nor a session's user_dm refills
        InboxStore("radar").append(_env("radar", kind="wake_timer", body="t", from_="system"))
        InboxStore("radar").append(_env("radar", kind="system", body="s", from_="system"))
        InboxStore("radar").append(_env("radar", kind="session_dm", from_="session:chat-1-1"))
        with pytest.raises(mp.PeerSendError):
            self._send()
        # the person typing does
        InboxStore("radar").append(_env("radar"))
        assert self._send()["ok"]

    def test_pair_rate_limit_is_shared_both_ways(self, monkeypatch):
        monkeypatch.setattr(mp, "DEFAULT_MAX_UNATTENDED_SENDS", 10_000)
        monkeypatch.setattr(mp, "DEFAULT_RATE_PER_WINDOW", 2)
        self._send("radar", "fixer")
        self._send("fixer", "radar")
        with pytest.raises(mp.PeerSendError) as exc:
            self._send("radar", "fixer")
        assert exc.value.code == "peer_dm_rate_limited" and exc.value.retry_after is not None
        # a different pair is unaffected
        assert self._send("radar", "scout")["ok"]

    def test_pair_rate_is_one_step_across_opposite_direction_senders(self, monkeypatch):
        """A->B and B->A hold different SENDER locks but share one pair deque; the
        boundary test and the append are one step under the rate's own lock, so
        two opposite sends at the last slot admit exactly one."""
        import threading

        monkeypatch.setattr(mp, "DEFAULT_MAX_UNATTENDED_SENDS", 10_000)
        monkeypatch.setattr(mp, "DEFAULT_RATE_PER_WINDOW", 1)
        gate = threading.Barrier(2)
        results: list[str] = []

        def send(frm, to):
            gate.wait()
            try:
                self._send(frm, to)
                results.append("ok")
            except mp.PeerSendError as exc:
                results.append(exc.code)

        threads = [
            threading.Thread(target=send, args=("radar", "fixer")),
            threading.Thread(target=send, args=("fixer", "radar")),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert sorted(results) == ["ok", "peer_dm_rate_limited"]
        assert len(InboxStore("fixer").pending()) + len(InboxStore("radar").pending()) == 1

    def test_body_checks(self):
        with pytest.raises(mp.PeerSendError) as exc:
            self._send(body="   ")
        assert exc.value.code == "message_empty"
        with pytest.raises(mp.PeerSendError) as exc:
            self._send(body="x" * (mi.MAX_BODY_CHARS + 1))
        assert exc.value.code == "message_too_long"

    def test_redaction_expansion_is_a_typed_refusal_before_the_rate_charge(self, monkeypatch):
        import kiro_crew.dashboard.chat_delivery as cd

        monkeypatch.setattr(cd, "sanitize_outbound", lambda text: text + "x" * 100)
        body = "y" * (mi.MAX_BODY_CHARS - 10)  # valid raw length, over the cap once sanitized
        with pytest.raises(mp.PeerSendError) as exc:
            self._send(body=body)
        assert exc.value.code == "message_too_long" and exc.value.status == 400
        assert InboxStore("fixer").pending() == [] and OutboxStore("radar").rows() == []
        # the rate bucket was not charged: a full window of sends still fits
        monkeypatch.setattr(cd, "sanitize_outbound", lambda text: text)
        for _ in range(mp.DEFAULT_RATE_PER_WINDOW):
            self._send()

    def test_mirror_is_written_before_delivery_and_marked_on_failure(self, monkeypatch):
        """The mirror is the budget record: a crash after it and before the inbox
        write must count against the sender, never deliver uncounted."""
        order: list[str] = []
        real_out_append = mi.OutboxStore.append
        real_in_append = mi.InboxStore.append

        def out_append(self, **kw):
            order.append("mirror")
            return real_out_append(self, **kw)

        def in_append(self, env):
            order.append("inbox")
            raise OSError("disk full")

        monkeypatch.setattr(mi.OutboxStore, "append", out_append)
        monkeypatch.setattr(mi.InboxStore, "append", in_append)
        with pytest.raises(mp.PeerSendError) as exc:
            self._send()
        assert exc.value.code == "peer_dm_write_failed" and order == ["mirror", "inbox"]
        monkeypatch.setattr(mi.InboxStore, "append", real_in_append)
        rows = OutboxStore("radar").rows()
        assert len(rows) == 1 and rows[0].refs.get("delivery") == mp.DELIVERY_FAILED
        assert rows[0].kind == "peer_dm"  # and it still counts against the budget
        assert OutboxStore("radar").peer_sends_since("") == 1

    def test_send_is_journaled_through_the_mirror(self):
        self._send()
        mirror = OutboxStore("radar").rows()[0]
        assert mirror.refs["delivery"] == mp.DELIVERY_DELIVERED
        assert mp.reconcile_peer_sends("radar") == {"stamped": 0, "recovered": 0, "failed": 0}

    def test_reconcile_completes_a_send_that_died_between_the_two_writes(self, monkeypatch):
        """Mirror written, process gone before the receiver's append: the journal
        carries the whole envelope, so restart delivers it (once) and the
        projection never shows as sent a message nobody received."""
        real_in_append = mi.InboxStore.append

        def crash(self, env):
            raise KeyboardInterrupt  # the process died; peer_send's except does not catch it

        monkeypatch.setattr(mi.InboxStore, "append", crash)
        with pytest.raises(KeyboardInterrupt):
            # Custom refs ride the journal too; only the reserved keys the
            # stores own are the gateway's to write.
            self._send(body="take #12", refs={"issue": 12, "completed": True, "to": "x"})
        monkeypatch.setattr(mi.InboxStore, "append", real_in_append)
        mirror = OutboxStore("radar").rows()[0]
        assert (
            mirror.refs["delivery"] == mp.DELIVERY_PENDING and InboxStore("fixer").pending() == []
        )
        notified: list[str] = []
        monkeypatch.setattr(ms, "notify_member", lambda slug, kind: notified.append(slug))
        assert mp.reconcile_peer_sends("radar") == {"stamped": 0, "recovered": 1, "failed": 0}
        got = InboxStore("fixer").pending()
        assert len(got) == 1 and got[0].id == mirror.refs["envelope_id"]
        assert got[0].body == "take #12" and got[0].hop == 1 and got[0].from_ == "member:radar"
        assert got[0].refs["pair_id"] == "fixer|radar" and notified == ["fixer"]
        # The recovered envelope carries the same refs the live path delivers:
        # the sender's custom keys, none of the journal's or the stores' own.
        assert got[0].refs == {"issue": 12, "pair_id": "fixer|radar"}
        mirror = OutboxStore("radar").rows()[0]
        assert mirror.refs["delivery"] == mp.DELIVERY_DELIVERED and mirror.refs["recovered"] is True
        # idempotent: a second pass finds nothing pending and appends nothing
        assert mp.reconcile_peer_sends("radar")["recovered"] == 0
        assert len(InboxStore("fixer").pending()) == 1

    def test_reconcile_only_stamps_when_the_receiver_already_has_it(self, monkeypatch):
        """Crash AFTER the receiver's append but before the delivered stamp: any
        trace of the id (here: already acked) means no second append."""
        self._send()
        inbound = InboxStore("fixer").pending()[0]
        InboxStore("fixer").ack([inbound.id])
        OutboxStore("radar").mark(OutboxStore("radar").rows()[0].id, delivery=mp.DELIVERY_PENDING)
        assert mp.reconcile_peer_sends("radar") == {"stamped": 1, "recovered": 0, "failed": 0}
        assert InboxStore("fixer").pending() == [] and len(InboxStore("fixer").acked()) == 1
        assert OutboxStore("radar").rows()[0].refs["delivery"] == mp.DELIVERY_DELIVERED

    def test_reconcile_marks_failed_when_the_receiver_binding_is_gone(self, monkeypatch):
        """Flag still set, `dm.json` gone in the crash window: the live send
        would refuse (`peer_dm_target_unknown`), so reconcile must not complete
        it either -- an envelope nobody can be woken for is a stranded message
        behind a mirror that claims delivery."""
        self._send()
        outbox = OutboxStore("radar")
        outbox.mark(outbox.rows()[0].id, delivery=mp.DELIVERY_PENDING)
        for p in InboxStore("fixer").root.glob("env_*.json"):
            p.unlink()
        monkeypatch.setattr(mp, "read_dm_binding", lambda slug: None)
        notified: list[str] = []
        monkeypatch.setattr(ms, "notify_member", lambda slug, kind: notified.append(slug))
        assert mp.reconcile_peer_sends("radar") == {"stamped": 0, "recovered": 0, "failed": 1}
        assert InboxStore("fixer").pending() == [] and notified == []
        row = outbox.rows()[0]
        assert (
            row.refs["delivery"] == mp.DELIVERY_FAILED and "binding" in row.refs["delivery_error"]
        )

    def test_reconcile_marks_failed_when_the_receiver_left_the_model(self, monkeypatch):
        self._send()
        outbox = OutboxStore("radar")
        outbox.mark(outbox.rows()[0].id, delivery=mp.DELIVERY_PENDING)
        for p in InboxStore("fixer").root.glob("env_*.json"):
            p.unlink()
        monkeypatch.setattr(mp, "inbox_model_enabled", lambda slug: slug == "radar")
        assert mp.reconcile_peer_sends("radar") == {"stamped": 0, "recovered": 0, "failed": 1}
        assert outbox.rows()[0].refs["delivery"] == mp.DELIVERY_FAILED
        assert InboxStore("fixer").pending() == []

    def test_degraded_or_malformed_config_denies(self, monkeypatch):
        def raising():
            raise mp._Degraded("member_peer_dm.enabled is not a boolean")

        monkeypatch.setattr(mp, "peer_dm_enabled", raising)
        with pytest.raises(mp.PeerSendError) as exc:
            self._send()
        assert exc.value.code == "peer_dm_config_degraded" and exc.value.status == 503
        assert InboxStore("fixer").pending() == []

    def test_malformed_member_opt_out_denies_not_defaults(self, monkeypatch):
        monkeypatch.setattr(
            mp,
            "member_inbox_setting",
            lambda slug, key, default: "yes" if key == "peer_dm" else default,
        )
        with pytest.raises(mp.PeerSendError) as exc:
            self._send()
        assert exc.value.code == "peer_dm_opted_out"

    def test_notifies_the_scheduler(self):
        seen: list[tuple[str, bool]] = []

        class _S:
            def notify(self, slug, *, immediate=False):
                seen.append((slug, immediate))

        ms.set_scheduler(_S())  # type: ignore[arg-type]
        self._send()
        assert seen == [("fixer", False)]


# ------------------------------------------------------------------ scheduler


class TestScheduler:
    @pytest.mark.asyncio
    async def test_serial_per_member_and_dirty_rerun(self):
        calls: list[str] = []
        gate = asyncio.Event()

        async def wake(slug: str) -> None:
            calls.append(slug)
            if len(calls) == 1:
                await gate.wait()

        sched = ms.MemberScheduler(wake, coalesce_secs=0.0, interval_for=lambda s: None)
        await sched.start()
        sched.notify("radar", immediate=True)
        for _ in range(100):  # the lane resolves the flag off-loop before it runs
            await asyncio.sleep(0.01)
            if sched.is_running("radar"):
                break
        assert sched.is_running("radar")
        sched.notify("radar", immediate=True)  # arrives mid-wake -> dirty
        sched.notify("radar", immediate=True)
        gate.set()
        for _ in range(50):
            await asyncio.sleep(0.01)
            if len(calls) == 2 and not sched.is_running("radar"):
                break
        assert calls == ["radar", "radar"]  # one follow-up wake, not one per notify
        await sched.stop()

    @pytest.mark.asyncio
    async def test_coalescing_window_and_immediate(self):
        calls: list[float] = []
        loop = asyncio.get_running_loop()

        async def wake(slug: str) -> None:
            calls.append(loop.time())

        sched = ms.MemberScheduler(wake, coalesce_secs=0.2, interval_for=lambda s: None)
        await sched.start()
        t0 = loop.time()
        sched.notify("radar")
        sched.notify("radar")
        await asyncio.sleep(0.05)
        assert calls == []  # inside the window
        sched.notify("radar", immediate=True)  # user_dm collapses the wait
        await asyncio.sleep(0.05)
        assert len(calls) == 1 and calls[0] - t0 < 0.15
        await sched.stop()

    @pytest.mark.asyncio
    async def test_start_reconciles_journaled_sends_before_scanning(self, monkeypatch):
        calls: list[str] = []
        monkeypatch.setattr(mp, "reconcile_peer_sends", lambda slug: calls.append(slug) or {})

        async def wake(slug: str) -> None:
            pass

        sched = ms.MemberScheduler(wake, coalesce_secs=0.0, interval_for=lambda s: None)
        await sched.start()
        assert calls == sorted(FLAGGED)
        await sched.stop()

    @pytest.mark.asyncio
    async def test_unflagged_member_is_ignored(self):
        calls: list[str] = []

        async def wake(slug: str) -> None:
            calls.append(slug)

        sched = ms.MemberScheduler(wake, coalesce_secs=0.0, interval_for=lambda s: None)
        await sched.start()
        sched.notify("scribe", immediate=True)
        await asyncio.sleep(0.02)
        assert calls == []
        await sched.stop()

    @pytest.mark.asyncio
    async def test_start_rebuilds_from_disk(self):
        """A restart resumes pending wakes with no persisted scheduler row."""
        InboxStore("radar").append(_env("radar"))
        InboxStore("fixer").append(_env("fixer", kind="system", body="s", from_="system"))
        calls: list[str] = []

        async def wake(slug: str) -> None:
            calls.append(slug)

        sched = ms.MemberScheduler(wake, coalesce_secs=0.0, interval_for=lambda s: None)
        await sched.start()
        await asyncio.sleep(0.05)
        assert sorted(calls) == ["fixer", "radar"]
        await sched.stop()

    @pytest.mark.asyncio
    async def test_wake_timer_is_minted_and_coalesced(self, monkeypatch):
        monkeypatch.setattr(ms, "MIN_WAKE_INTERVAL_SECS", 0.0)
        calls: list[str] = []
        block = asyncio.Event()

        async def wake(slug: str) -> None:
            calls.append(slug)
            await block.wait()

        sched = ms.MemberScheduler(
            wake, coalesce_secs=0.0, interval_for=lambda s: 0.03 if s == "radar" else None
        )
        await sched.start()
        await asyncio.sleep(0.2)
        timers = [e for e in InboxStore("radar").pending() if e.kind == "wake_timer"]
        assert len(timers) == 1  # ticks while one is pending do not stack
        assert calls == ["radar"]
        block.set()
        await sched.stop()

    @pytest.mark.asyncio
    async def test_notify_from_a_worker_thread_hops_to_the_loop(self):
        calls: list[str] = []

        async def wake(slug: str) -> None:
            calls.append(slug)

        sched = ms.MemberScheduler(wake, coalesce_secs=0.0, interval_for=lambda s: None)
        await sched.start()
        await asyncio.to_thread(sched.notify, "radar", immediate=True)
        await asyncio.sleep(0.05)
        assert calls == ["radar"]
        await sched.stop()

    @pytest.mark.asyncio
    async def test_a_wake_that_leaves_envelopes_pending_is_retried(self, monkeypatch):
        """A failed turn acks nothing and nothing else notifies the lane; the
        scheduler must come back on its own, bounded by the attempt ceiling."""
        monkeypatch.setattr(ms, "RETRY_DELAY_SECS", 0.02)
        store = InboxStore("radar")
        env = store.append(_env("radar"))
        calls: list[str] = []

        async def wake(slug: str) -> None:
            calls.append(slug)
            store.mark_attempt([env.id])  # as the runner does; no ack -> still pending
            if len(calls) >= 3:
                store.ack([env.id])

        sched = ms.MemberScheduler(wake, coalesce_secs=0.0, interval_for=lambda s: None)
        await sched.start()
        sched.notify("radar", immediate=True)
        for _ in range(100):
            await asyncio.sleep(0.02)
            if len(calls) >= 3 and not sched.is_running("radar"):
                break
        assert calls == ["radar", "radar", "radar"]  # retried until the inbox drained
        await asyncio.sleep(0.05)
        assert calls == ["radar", "radar", "radar"]  # and no retry once it is empty
        await sched.stop()

    @pytest.mark.asyncio
    async def test_a_raising_wake_does_not_kill_the_lane(self):
        calls: list[str] = []

        async def wake(slug: str) -> None:
            calls.append(slug)
            if len(calls) == 1:
                raise RuntimeError("boom")

        sched = ms.MemberScheduler(wake, coalesce_secs=0.0, interval_for=lambda s: None)
        await sched.start()
        sched.notify("radar", immediate=True)
        await asyncio.sleep(0.02)
        sched.notify("radar", immediate=True)
        await asyncio.sleep(0.02)
        assert calls == ["radar", "radar"]
        await sched.stop()


# ---------------------------------------------------------------- wake runner


class _Slot:
    def __init__(self, key: str) -> None:
        self.key = key
        self.messages: list[dict[str, Any]] = []
        self.task: asyncio.Task | None = None
        self.title = ""
        self._titled = False
        self.appended: list[tuple[str, str]] = []

    def append(self, role, content, cls="", *, meta=None, **_):
        self.appended.append((role, content))
        row = {"role": role, "content": content}
        if meta:
            row["meta"] = meta
        self.messages.append(row)


class _Log:
    def __init__(self) -> None:
        self.deleted: list[str] = []
        self.appended: list[tuple[str, str, str]] = []

    def delete_session(self, key: str, **_):
        self.deleted.append(key)
        return True

    def append(self, key, role, content, **_):
        self.appended.append((key, role, content))


class _State:
    def __init__(self, behaviour: str) -> None:
        self.behaviour = behaviour
        self.slots: dict[str, _Slot] = {}
        self.notified: list[str] = []
        self.thread = _Slot("member-radar")
        self.thread.workspace, self.thread.project = "radar-ws", "/proj/radar"
        self.conversation_log = _Log()

    def get_or_create_slot(self, key, agent="", mode="", workspace="default", **_):
        slot = _Slot(key)
        slot.agent, slot.mode, slot.workspace = agent, mode, workspace
        slot.project = ""
        self.slots[key] = slot
        return slot

    def broadcast_ws(self, kind, payload):
        self.notified.append(f"ws:{kind}:{payload.get('slot', '')}")

    def get_slot(self, key):
        if key == "member-radar":
            return self.thread
        return self.slots.get(key)

    def notify(self, kind, title, body, **_):
        self.notified.append(kind)


@pytest.fixture
def wake_env(monkeypatch):
    from kiro_crew.dashboard import member_wake as mw

    monkeypatch.setattr(mw, "read_dm_binding", lambda slug: {"member": "radar-agent", "slug": slug})
    monkeypatch.setattr(mw, "inbox_model_enabled", lambda slug: slug in FLAGGED)
    seen: dict[str, Any] = {}

    def enqueue(self_slot, prompt, run, state):
        seen["prompt"] = prompt
        behaviour = state.behaviour

        async def turn():
            if behaviour == "crash":
                raise RuntimeError("transport dropped")
            if behaviour == "hang":
                await asyncio.sleep(10)
            if behaviour == "error_row":
                # what _run_chat does on a provider failure: no exception, an error row
                self_slot.messages.append({"role": "error", "content": "Session stuck"})
                return
            if behaviour == "silent":
                return
            if behaviour == "provider_timeout":
                # what _run_chat records when the provider cut the turn short after
                # streaming partial text: no error row, a non-end_turn stop reason
                self_slot.messages.append({"role": "assistant", "content": "Half an answ"})
                self_slot._last_stop_reason = "timeout"
                return
            if behaviour == "outbox_sent":
                # what the outbox_send handler does mid-turn
                self_slot._wake_outbox_sent = True
            if behaviour == "continuation" and not getattr(self_slot, "_continued", False):
                # what _start_next_queued_turn does from the finishing turn's finally:
                # a promise-only first reply, then a SUCCESSOR task doing the real work
                self_slot._continued = True
                self_slot.messages.append({"role": "assistant", "content": "On it, one moment."})

                async def successor():
                    await asyncio.sleep(0.05)
                    self_slot.messages.append({"role": "assistant", "content": "Done: triaged it."})

                self_slot.task = asyncio.get_running_loop().create_task(successor())
                return
            self_slot.messages.append({"role": "assistant", "content": "Done: triaged it."})

        self_slot.task = asyncio.get_running_loop().create_task(turn())
        return True

    monkeypatch.setattr(_Slot, "enqueue_or_run_prompt", enqueue, raising=False)

    async def close_slot(state, slot, key):
        seen.setdefault("closed", []).append(key)

    async def stop_slot_turn(state, slot, **kw):
        slot.task.cancel()
        seen["stopped"] = True
        return {"info": "stopping"}

    import kiro_crew.dashboard.chat_handlers as ch
    import kiro_crew.dashboard.chat_runner as cr

    monkeypatch.setattr(ch, "close_slot", close_slot)
    monkeypatch.setattr(ch, "stop_slot_turn", stop_slot_turn)
    monkeypatch.setattr(cr, "_run_chat", lambda *a, **k: None)
    monkeypatch.setattr(
        "kiro_crew.session_ledger.render_snapshot", lambda key: "[work ledger]\nnext: keep going"
    )
    import kiro_crew.dashboard.chat_persistence as cp

    async def fake_save(state, slot, *args, **kw):
        seen.setdefault("persisted", []).append(slot.key)
        return True

    monkeypatch.setattr(cp, "save_slot_off_loop", fake_save)
    return mw, seen


class TestWakeRunner:
    @pytest.mark.asyncio
    async def test_clean_wake_acks_writes_outbox_and_mirrors(self, wake_env):
        mw, seen = wake_env
        inbox = InboxStore("radar")
        a = inbox.append(_env("radar", body="what did you triage?"))
        b = inbox.append(
            _env("radar", kind="peer_dm", body="take #12", from_="member:fixer", hop=2)
        )
        state = _State("ok")
        r = await mw.run_member_wake(state, "radar")
        assert r["ok"] and r["drained"] == 2
        assert inbox.pending() == [] and len(inbox.acked()) == 2
        assert "[INBOX — 2 envelope(s)]" in seen["prompt"]
        assert "kind=user_dm (from the owner) from=user" in seen["prompt"]
        assert "kind=peer_dm" in seen["prompt"] and "hop=2" in seen["prompt"]
        assert "[work ledger]" in seen["prompt"]
        wake = state.slots[r["wake"]]
        assert wake.mode == mi.WAKE_SLOT_MODE and wake.agent == "radar-agent"
        assert wake._created_by == "member-radar" and wake._wake_inbound_hop == 2
        # the wake works where the member's live thread works
        assert wake.workspace == "radar-ws" and wake.project == "/proj/radar"
        rows = OutboxStore("radar").rows()
        assert (
            len(rows) == 1
            and rows[0].kind == mi.OUTBOX_REPLY_KIND
            and rows[0].body == "Done: triaged it."
        )
        assert sorted(rows[0].refs["in_reply_to"]) == sorted([a.id, b.id])
        assert state.thread.appended == [("assistant", "Done: triaged it.")]
        # the mirror row is saved to the thread transcript: a restart keeps the view
        assert seen["persisted"] == ["member-radar"]
        assert seen["closed"] == [r["wake"]]
        # a clean wake leaves no transcript (deleted under its history key)
        assert [k.split(":", 1)[-1] for k in state.conversation_log.deleted] == [r["wake"]]

    @pytest.mark.asyncio
    async def test_turn_cut_short_by_the_provider_is_not_acked(self, wake_env):
        """Partial text + a `timeout` stop reason and no error row: the batch stays
        pending (redelivered), nothing is written to the outbox or the thread."""
        mw, seen = wake_env
        inbox = InboxStore("radar")
        inbox.append(_env("radar"))
        state = _State("provider_timeout")
        r = await mw.run_member_wake(state, "radar")
        assert not r["ok"] and r["reason"] == "turn_stopped: timeout" and r["drained"] == 0
        assert len(inbox.pending()) == 1 and inbox.pending()[0].attempts == 1
        assert OutboxStore("radar").rows() == [] and state.thread.appended == []
        # an explicit clean end (`end_turn`) and no reason at all both count as clean
        assert mw._turn_cut_short(type("S", (), {"_last_stop_reason": "end_turn"})()) == ""
        assert mw._turn_cut_short(type("S", (), {"_last_stop_reason": ""})()) == ""
        assert (
            mw._turn_cut_short(type("S", (), {"_last_stop_reason": "cancelled"})()) == "cancelled"
        )

    @pytest.mark.asyncio
    async def test_reply_whose_mirror_failed_is_restored_by_the_next_wake(
        self, wake_env, monkeypatch
    ):
        """The outbox row is the record; a mirror save that fails must not leave the
        thread permanently without the reply. The row is stamped `view=pending`
        and the next wake re-appends it to the thread before its own batch."""
        import kiro_crew.dashboard.chat_persistence as cp

        mw, seen = wake_env
        outcome = {"ok": False}

        async def save(state, slot, *a, **kw):
            seen.setdefault("persisted", []).append(slot.key)
            if not outcome["ok"]:
                return False
            return True

        monkeypatch.setattr(cp, "save_slot_off_loop", save)
        inbox = InboxStore("radar")
        inbox.append(_env("radar", body="first"))
        state = _State("ok")
        r = await mw.run_member_wake(state, "radar")
        assert r["ok"] and inbox.pending() == []  # the batch is still acked: the record is safe
        row = OutboxStore("radar").rows()[0]
        assert row.refs[mi.VIEW_REF] == mi.VIEW_PENDING
        assert state.thread.appended == [("assistant", "Done: triaged it.")]

        # Restart: the live thread lost the row. The next wake restores it first.
        outcome["ok"] = True
        state.thread = _Slot("member-radar")
        inbox.append(_env("radar", body="second"))
        r = await mw.run_member_wake(state, "radar")
        assert r["ok"]
        assert state.thread.appended[0] == ("assistant", "Done: triaged it.")
        assert state.thread.messages[0]["meta"]["peer_dm"]["outbox_id"] == row.id
        rows = {e.id: e for e in OutboxStore("radar").rows()}
        assert rows[row.id].refs[mi.VIEW_REF] == mi.VIEW_RESTORED
        assert OutboxStore("radar").view_pending() == []

    @pytest.mark.asyncio
    async def test_reply_reaches_an_unloaded_thread_transcript(self, wake_env, monkeypatch):
        """The person does not have the thread open: the reply is appended to the
        thread's transcript on disk anyway, so it is in the conversation they open
        later -- not only in the outbox store."""
        from types import SimpleNamespace

        mw, seen = wake_env
        cfg = SimpleNamespace(
            agents={}, workspaces={"default": object()}, default_workspace="default"
        )
        monkeypatch.setattr(
            "kiro_crew.config.loader.KiroCrewConfig.load", classmethod(lambda cls, *a, **k: cfg)
        )
        monkeypatch.setattr("kiro_crew.config.loader.default_project_dir", lambda ws=None: "/p")
        InboxStore("radar").append(_env("radar"))
        state = _State("ok")
        state.thread = None
        r = await mw.run_member_wake(state, "radar")
        assert r["ok"]
        assert state.conversation_log.appended == [
            ("dashboard:member-radar", "assistant", "Done: triaged it.")
        ]

    @pytest.mark.asyncio
    async def test_wake_without_a_live_thread_resolves_the_member_workspace(
        self, wake_env, monkeypatch
    ):
        """No thread slot loaded: the wake resolves the member's configured
        workspace and that workspace's project the way the thread opener does,
        never the bare default."""
        from types import SimpleNamespace

        mw, seen = wake_env
        cfg = SimpleNamespace(
            agents={"radar-agent": SimpleNamespace(workspace="ops")},
            workspaces={"ops": object(), "default": object()},
            default_workspace="default",
        )
        monkeypatch.setattr(
            "kiro_crew.config.loader.KiroCrewConfig.load", classmethod(lambda cls, *a, **k: cfg)
        )
        monkeypatch.setattr(
            "kiro_crew.config.loader.default_project_dir", lambda ws=None: f"/projects/{ws}"
        )
        inbox = InboxStore("radar")
        inbox.append(_env("radar"))
        state = _State("ok")
        state.thread = None  # the DM thread is not loaded
        r = await mw.run_member_wake(state, "radar")
        wake = state.slots[r["wake"]]
        assert wake.workspace == "ops" and wake.project == "/projects/ops"

    @pytest.mark.asyncio
    async def test_wake_publishes_its_batch_for_outbox_send(self, wake_env):
        mw, seen = wake_env
        inbox = InboxStore("radar")
        a = inbox.append(_env("radar"))
        state = _State("ok")
        r = await mw.run_member_wake(state, "radar")
        assert state.slots[r["wake"]]._wake_batch_ids == [a.id]

    @pytest.mark.asyncio
    async def test_redelivery_of_an_answered_envelope_acks_without_a_second_turn(self, wake_env):
        """The previous wake wrote its reply and died before the ack (or the
        `outbox_send` route returned and the turn then errored): the envelope
        redelivers, but the reply row already names it -- ack, no new turn, so
        the owner never reads the same answer twice."""
        mw, seen = wake_env
        inbox = InboxStore("radar")
        a = inbox.append(_env("radar", body="what did you triage?"))
        inbox.mark_attempt([a.id])
        OutboxStore("radar").append(
            kind=mi.OUTBOX_REPLY_KIND,
            body="Done: triaged it.",
            refs={"wake_key": "member-radar.wake-1", "in_reply_to": [a.id], "completed": True},
        )
        state = _State("ok")
        r = await mw.run_member_wake(state, "radar")
        assert r == {"ok": True, "drained": 1, "dead_lettered": 0, "already_answered": 1}
        assert inbox.pending() == [] and len(inbox.acked()) == 1
        assert "prompt" not in seen and state.slots == {}  # no turn, no slot
        assert len(OutboxStore("radar").rows()) == 1  # no second reply

    @pytest.mark.asyncio
    async def test_early_reply_from_a_failed_wake_does_not_ack_the_batch(self, wake_env):
        """`outbox_send("on it...")` and then the turn failed: the reply row exists
        but no wake completed, so redelivery RUNS the batch again instead of acking
        it as done on the strength of the early reply."""
        mw, seen = wake_env
        inbox = InboxStore("radar")
        a = inbox.append(_env("radar", body="fix #12"))
        inbox.mark_attempt([a.id])
        OutboxStore("radar").append(
            kind=mi.OUTBOX_REPLY_KIND,
            body="On it.",
            refs={"wake_key": "member-radar.wake-1", "in_reply_to": [a.id]},  # not completed
        )
        state = _State("ok")
        r = await mw.run_member_wake(state, "radar")
        assert r["ok"] and r["drained"] == 1 and r["already_answered"] == 0
        assert "fix #12" in seen["prompt"].split("[INBOX", 1)[1]
        assert "reply you: On it." in seen["prompt"]  # the early reply is visible context
        rows = OutboxStore("radar").rows()
        assert [x.body for x in rows] == ["On it.", "Done: triaged it."]
        assert rows[1].refs["completed"] is True and rows[0].refs.get("completed") is None

    @pytest.mark.asyncio
    async def test_completion_stamp_failure_does_not_skip_the_ack(self, wake_env, monkeypatch):
        """The runner's own reply is written complete in one write, and a failing
        `mark_completed` (a transient I/O fault) must not skip the ack -- skipping
        it would redeliver and expose a second reply."""
        mw, seen = wake_env
        inbox = InboxStore("radar")
        a = inbox.append(_env("radar"))

        def boom(self, wake_key):
            raise OSError("stamp failed")

        monkeypatch.setattr(mi.OutboxStore, "mark_completed", boom)
        r = await mw.run_member_wake(_State("ok"), "radar")
        assert r["ok"] and r["drained"] == 1
        assert inbox.pending() == [] and len(inbox.acked()) == 1  # acked despite the fault
        rows = OutboxStore("radar").rows()
        assert len(rows) == 1 and rows[0].refs["completed"] is True  # single complete write
        assert a.id in OutboxStore("radar").answered()

    @pytest.mark.asyncio
    async def test_clean_wake_stamps_its_route_written_reply_completed(self, wake_env):
        """The `outbox_send` route writes the reply mid-turn; the runner stamps it
        completed after the clean end, before the ack."""
        mw, seen = wake_env
        inbox = InboxStore("radar")
        a = inbox.append(_env("radar"))
        real_enqueue = _Slot.enqueue_or_run_prompt

        def enqueue(self_slot, prompt, run, state):
            # what the outbox_send route does mid-turn, keyed by the wake's own key
            OutboxStore("radar").append(
                kind=mi.OUTBOX_REPLY_KIND,
                body="Done via tool.",
                refs={"wake_key": self_slot.key, "in_reply_to": list(self_slot._wake_batch_ids)},
            )
            return real_enqueue(self_slot, prompt, run, state)

        import pytest as _pt

        mp_ = _pt.MonkeyPatch()
        mp_.setattr(_Slot, "enqueue_or_run_prompt", enqueue, raising=False)
        try:
            r = await mw.run_member_wake(_State("outbox_sent"), "radar")
        finally:
            mp_.undo()
        assert r["ok"] and inbox.pending() == []
        rows = OutboxStore("radar").rows()
        assert len(rows) == 1 and rows[0].refs["completed"] is True
        assert rows[0].refs["wake_key"] == r["wake"] and rows[0].refs["in_reply_to"] == [a.id]
        assert a.id in OutboxStore("radar").answered()

    @pytest.mark.asyncio
    async def test_redelivery_mixes_answered_and_new_envelopes(self, wake_env):
        mw, seen = wake_env
        inbox = InboxStore("radar")
        old = inbox.append(_env("radar", body="old question"))
        OutboxStore("radar").append(
            kind=mi.OUTBOX_REPLY_KIND,
            body="old answer",
            refs={"in_reply_to": [old.id], "completed": True},
        )
        new = inbox.append(_env("radar", body="new question"))
        state = _State("ok")
        r = await mw.run_member_wake(state, "radar")
        assert r["ok"] and r["drained"] == 2 and r["already_answered"] == 1
        inbox_section = seen["prompt"].split("[INBOX", 1)[1]
        assert "new question" in inbox_section and "old question" not in inbox_section
        assert "[INBOX — 1 envelope(s)]" in seen["prompt"]
        assert inbox.pending() == []
        rows = OutboxStore("radar").rows()
        assert [r.body for r in rows] == ["old answer", "Done: triaged it."]
        assert rows[1].refs["in_reply_to"] == [new.id]
        assert state.slots[r["wake"]]._wake_batch_ids == [new.id]

    @pytest.mark.asyncio
    async def test_translated_session_send_is_labelled_not_the_owner(self, wake_env):
        mw, seen = wake_env
        InboxStore("radar").append(
            _env("radar", kind="session_dm", body="do X", from_="session:chat-1-7")
        )
        await mw.run_member_wake(_State("ok"), "radar")
        assert (
            "kind=session_dm (from another session, not the owner) from=session:chat-1-7"
            in seen["prompt"]
        )
        assert "(from the owner)" not in seen["prompt"]

    @pytest.mark.asyncio
    async def test_reply_is_sanitized_before_outbox_and_mirror(self, wake_env, monkeypatch):
        """The model's text goes through the same outbound chain every sibling
        delivery path applies, before it is stored or shown."""
        import kiro_crew.dashboard.chat_delivery as cd

        mw, _ = wake_env
        monkeypatch.setattr(
            cd, "sanitize_outbound", lambda text: text.replace("SECRET", "[redacted]")
        )
        monkeypatch.setattr(mw, "_last_assistant_text", lambda slot: "token SECRET here")
        InboxStore("radar").append(_env("radar"))
        state = _State("ok")
        await mw.run_member_wake(state, "radar")
        rows = OutboxStore("radar").rows()
        assert rows and rows[0].body == "token [redacted] here"
        assert state.thread.appended == [("assistant", "token [redacted] here")]

    @pytest.mark.asyncio
    async def test_wake_slot_counts_as_member_mode_for_the_context_gate(self, wake_env):
        """The identity block, rules and briefing are keyed on member mode; a wake
        must not lose them by running in its own mode."""
        from kiro_crew.members import is_member_mode

        mw, _ = wake_env
        InboxStore("radar").append(_env("radar"))
        state = _State("ok")
        r = await mw.run_member_wake(state, "radar")
        assert is_member_mode(state.slots[r["wake"]].mode)
        assert (
            is_member_mode("member")
            and not is_member_mode("")
            and not is_member_mode("orchestrator")
        )

    @pytest.mark.asyncio
    async def test_wake_waits_for_the_whole_cycle_not_the_first_task(self, wake_env):
        """A promise-only reply followed by an auto-continued successor must be
        awaited to the end: the reply recorded is the successor's, and nothing is
        acked or closed while the successor still runs."""
        mw, _ = wake_env
        InboxStore("radar").append(_env("radar"))
        state = _State("continuation")
        r = await mw.run_member_wake(state, "radar")
        assert r["ok"] and r["drained"] == 1
        rows = OutboxStore("radar").rows()
        assert rows[0].body == "Done: triaged it."  # the successor's text, not the promise
        assert state.slots[r["wake"]].task.done()

    @pytest.mark.asyncio
    async def test_error_row_is_not_success(self, wake_env):
        """_run_chat swallows provider failures into an error row; the task
        resolving is not evidence the turn ran, so nothing is acked."""
        mw, _ = wake_env
        inbox = InboxStore("radar")
        inbox.append(_env("radar"))
        r = await mw.run_member_wake(_State("error_row"), "radar")
        assert not r["ok"] and r["reason"] == "turn_error_row"
        assert "Session stuck" not in r["reason"]  # row text never leaves the slot
        assert len(inbox.pending()) == 1 and OutboxStore("radar").rows() == []

    @pytest.mark.asyncio
    async def test_silent_turn_is_not_success(self, wake_env):
        mw, _ = wake_env
        InboxStore("radar").append(_env("radar"))
        r = await mw.run_member_wake(_State("silent"), "radar")
        assert not r["ok"] and r["reason"] == "turn_produced_nothing"
        assert len(InboxStore("radar").pending()) == 1

    @pytest.mark.asyncio
    async def test_recent_exchange_gives_the_next_wake_its_own_reply(self, wake_env):
        mw, seen = wake_env
        inbox = InboxStore("radar")
        inbox.append(_env("radar", body="should we close #12?"))
        await mw.run_member_wake(_State("ok"), "radar")
        inbox.append(_env("radar", body="yes, do that"))
        await mw.run_member_wake(_State("ok"), "radar")
        assert "[RECENT EXCHANGE" in seen["prompt"]
        assert "user_dm user: should we close #12?" in seen["prompt"]
        assert "reply you: Done: triaged it." in seen["prompt"]

    @pytest.mark.asyncio
    async def test_private_generation_member_wakes_in_its_own_generation(
        self, wake_env, monkeypatch
    ):
        """A V2 binding's slot key carries the memory generation; mirror, ledger
        and ownership must all use it, never the bare slug."""
        mw, seen = wake_env
        v2 = "member-radar.memory-v2-abc"
        monkeypatch.setattr(
            mw,
            "read_dm_binding",
            lambda slug: {
                "member": "radar-agent",
                "slug": slug,
                "slot_key": v2,
                "memory_store": "v2-abc",
            },
        )
        snapshots: list[str] = []
        monkeypatch.setattr(
            "kiro_crew.session_ledger.render_snapshot", lambda key: snapshots.append(key) or ""
        )
        InboxStore("radar").append(_env("radar"))
        state = _State("ok")
        state.thread.key = v2
        state.get_slot = lambda key, _s=state: _s.thread if key == v2 else _s.slots.get(key)  # type: ignore[method-assign]
        r = await mw.run_member_wake(state, "radar")
        wake = state.slots[r["wake"]]
        assert r["wake"].startswith(v2 + ".wake-") and wake._created_by == v2
        assert wake.memory_store == "v2-abc"
        assert snapshots == [v2]
        assert state.thread.appended == [("assistant", "Done: triaged it.")]

    @pytest.mark.asyncio
    async def test_crashed_wake_leaves_envelopes_pending_with_attempt_raised(self, wake_env):
        mw, seen = wake_env
        inbox = InboxStore("radar")
        inbox.append(_env("radar"))
        state = _State("crash")
        r = await mw.run_member_wake(state, "radar")
        assert not r["ok"] and r["reason"].startswith("turn_failed")
        pending = inbox.pending()
        assert len(pending) == 1 and pending[0].attempts == 1
        assert OutboxStore("radar").rows() == []
        assert seen["closed"]  # the ephemeral slot is closed on every path
        assert len(state.conversation_log.deleted) == 1  # a failed wake's transcript goes too

    @pytest.mark.asyncio
    async def test_poison_envelope_is_dead_lettered_and_escalated(self, wake_env):
        mw, _ = wake_env
        inbox = InboxStore("radar")
        a = inbox.append(_env("radar", body="poison"))
        state = _State("crash")
        for _ in range(mi.MAX_ATTEMPTS):
            await mw.run_member_wake(state, "radar")
        assert inbox.pending()[0].attempts == mi.MAX_ATTEMPTS
        # a FAILED wake's transcript is discarded too: a timer-driven member under a
        # persistent outage would otherwise grow history by one file per interval
        assert len(state.conversation_log.deleted) == mi.MAX_ATTEMPTS
        state.behaviour = "ok"
        r = await mw.run_member_wake(state, "radar")
        assert r["dead_lettered"] == 1
        assert [e.id for e in inbox.dead_letters()] == [a.id]
        notices = [e for e in inbox.pending() if e.kind == "system"]
        assert len(notices) == 1 and "dead-letter" in notices[0].body
        assert notices[0].refs["dead_letter_notice"] is True
        assert state.notified == ["member_dead_letter"]

    @pytest.mark.asyncio
    async def test_a_dead_lettered_notice_does_not_spawn_another_notice(self, wake_env):
        """Provider down for the whole ceiling: the poison envelope dead-letters and
        its notice is written; the NOTICE then dead-letters too -- and must not
        spawn a replacement, or the outage becomes a notice-of-a-notice loop that
        grows the dead-letter directory for as long as it lasts."""
        mw, _ = wake_env
        inbox = InboxStore("radar")
        inbox.append(_env("radar", body="poison"))
        state = _State("crash")
        for _ in range(mi.MAX_ATTEMPTS + 1):
            await mw.run_member_wake(state, "radar")  # last one dead-letters + notices
        assert len(inbox.dead_letters()) == 1 and len(inbox.pending()) == 1
        for _ in range(mi.MAX_ATTEMPTS + 1):
            await mw.run_member_wake(state, "radar")  # the notice runs out of attempts too
        assert len(inbox.dead_letters()) == 2  # poison + its notice
        assert inbox.pending() == []  # no second notice: the loop is closed
        assert state.notified == ["member_dead_letter"]  # the owner was told once
        # and the lane stays quiet afterwards
        assert (await mw.run_member_wake(state, "radar")) == {"ok": True, "drained": 0}

    @pytest.mark.asyncio
    async def test_wall_clock_budget_stops_the_turn_and_redelivers(self, wake_env, monkeypatch):
        mw, seen = wake_env
        monkeypatch.setattr(mw, "WAKE_WALL_SECS", 0.05)
        inbox = InboxStore("radar")
        inbox.append(_env("radar"))
        r = await mw.run_member_wake(_State("hang"), "radar")
        assert not r["ok"] and r["reason"] == "wall_clock_budget" and seen["stopped"]
        assert len(inbox.pending()) == 1

    @pytest.mark.asyncio
    async def test_no_binding_leaves_envelopes_and_runs_nothing(self, wake_env, monkeypatch):
        mw, seen = wake_env
        monkeypatch.setattr(mw, "read_dm_binding", lambda slug: None)
        InboxStore("radar").append(_env("radar"))
        r = await mw.run_member_wake(_State("ok"), "radar")
        assert r["reason"] == "binding_missing" and "prompt" not in seen
        assert len(InboxStore("radar").pending()) == 1

    @pytest.mark.asyncio
    async def test_model_outbox_send_suppresses_the_runner_reply(self, wake_env):
        mw, _ = wake_env
        InboxStore("radar").append(_env("radar"))
        r = await mw.run_member_wake(_State("outbox_sent"), "radar")
        assert r["ok"] and OutboxStore("radar").rows() == []

    @pytest.mark.asyncio
    async def test_unflagged_member_is_not_woken(self, wake_env):
        mw, seen = wake_env
        InboxStore("scribe").append(_env("scribe"))
        assert (await mw.run_member_wake(_State("ok"), "scribe"))["reason"] == "not_flagged"
        assert "prompt" not in seen


# ----------------------------------------------------------- session_send shim


def _routes_app(state: Any) -> Any:
    from aiohttp import web

    from kiro_crew.dashboard.handlers import member_inbox as routes

    app = web.Application()
    app["state"] = state
    app.router.add_post("/api/member-inbox/outbox", routes.api_member_inbox_outbox)
    return app


class TestInboxRoutes:
    """The `outbox_send` route behind the member tool, called as
    the wake `member-radar.wake-9` (internal auth and slot lookup stubbed)."""

    @pytest.fixture(autouse=True)
    def _stub(self, monkeypatch):
        from kiro_crew.dashboard import session_control as sc
        from kiro_crew.dashboard.handlers import member_inbox as routes

        async def _ok(request):
            return None

        monkeypatch.setattr(routes, "_require_internal", _ok)
        monkeypatch.setattr(sc, "caller_slot_key", lambda st, key: key)

        async def _no_persist(state, thread, key):
            return True  # persisted (the stub skips the disk)

        monkeypatch.setattr("kiro_crew.dashboard.member_wake._persist_thread_row", _no_persist)

    def _state(self, batch_ids):
        wake = _Slot("member-radar.wake-9")
        wake._wake_slug = "radar"
        wake._wake_batch_ids = list(batch_ids)
        wake._wake_outbox_sent = False
        state = _State("ok")
        state.slots[wake.key] = wake
        return state, wake

    @pytest.mark.asyncio
    async def test_outbox_send_strips_reserved_refs_and_stamps_the_batch(self):
        """A model cannot preset `completed` (the redelivery idempotency stamp) or
        forge `in_reply_to` / `wake_key`: the runner's values win and the reserved
        keys are dropped, so an "on it..." row can never ack the owner's message."""
        from aiohttp.test_utils import TestClient, TestServer

        inbox = InboxStore("radar")
        a = inbox.append(_env("radar"))
        state, wake = self._state([a.id])
        async with TestClient(TestServer(_routes_app(state))) as client:
            resp = await client.post(
                "/api/member-inbox/outbox",
                json={
                    "body": "on it...",
                    "refs": {"completed": True, "in_reply_to": ["env_x"], "wake_key": "k", "n": 1},
                },
                headers={"X-Session-Key": wake.key},
            )
            assert resp.status == 200 and (await resp.json())["ok"] is True
        rows = OutboxStore("radar").rows()
        assert len(rows) == 1 and rows[0].body == "on it..."
        assert rows[0].refs == {"n": 1, "wake_key": wake.key, "in_reply_to": [a.id]}
        assert OutboxStore("radar").answered() == set()  # not completed: redelivery runs
        assert wake._wake_outbox_sent is True
        assert state.thread.appended == [("assistant", "on it...")]


class TestSessionSendShim:
    """The compatibility shim in ``send_to_target`` for flagged member targets."""

    def _state(self, caller_key: str, target_key: str, target_mode: str = "member"):
        class Slot:
            def __init__(self, key, mode):
                self.key, self.mode = key, mode
                self._wake_inbound_hop = 3

        slots = {caller_key: Slot(caller_key, ""), target_key: Slot(target_key, target_mode)}

        class State:
            def get_slot(self, key):
                return slots.get(key)

        return State(), slots

    @pytest.mark.asyncio
    async def test_member_caller_goes_through_peer_admission(self, monkeypatch):
        from kiro_crew.dashboard import session_control as sc

        state, slots = self._state("member-radar.wake-2", "member-fixer")
        monkeypatch.setattr(sc, "_resolve_slot", lambda st, target: slots.get(target))
        monkeypatch.setattr(sc, "caller_slot_key", lambda st, key: key)
        r = await sc._inbox_model_shim(
            state, caller_session_key="member-radar.wake-2", target="member-fixer", body="take it"
        )
        assert r is not None and r["kind"] == "peer_dm" and r["started"] is False
        env = InboxStore("fixer").pending()[0]
        assert env.from_ == "member:radar" and env.hop == 4 and env.refs["via"] == "session_send"

    @pytest.mark.asyncio
    async def test_member_caller_refusal_maps_to_session_control_error(self, monkeypatch):
        from kiro_crew.dashboard import session_control as sc

        state, slots = self._state("member-radar.wake-2", "member-fixer")
        slots["member-radar.wake-2"]._wake_inbound_hop = mp.DEFAULT_MAX_HOPS
        monkeypatch.setattr(sc, "_resolve_slot", lambda st, target: slots.get(target))
        monkeypatch.setattr(sc, "caller_slot_key", lambda st, key: key)
        with pytest.raises(sc.SessionControlError) as exc:
            await sc._inbox_model_shim(
                state, caller_session_key="member-radar.wake-2", target="member-fixer", body="x"
            )
        assert exc.value.code == "peer_dm_hop_limit"

    @pytest.mark.asyncio
    async def test_non_member_target_falls_through(self, monkeypatch):
        from kiro_crew.dashboard import session_control as sc

        state, slots = self._state("chat-1-1", "chat-1-2", target_mode="")
        monkeypatch.setattr(sc, "_resolve_slot", lambda st, target: slots.get(target))
        assert (
            await sc._inbox_model_shim(
                state, caller_session_key="chat-1-1", target="chat-1-2", body="x"
            )
            is None
        )

    @pytest.mark.asyncio
    async def test_unflagged_member_target_falls_through(self, monkeypatch):
        from kiro_crew.dashboard import session_control as sc

        state, slots = self._state("chat-1-1", "member-scribe")
        monkeypatch.setattr(sc, "_resolve_slot", lambda st, target: slots.get(target))
        assert (
            await sc._inbox_model_shim(
                state, caller_session_key="chat-1-1", target="member-scribe", body="x"
            )
            is None
        )

    @pytest.mark.asyncio
    async def test_ordinary_caller_is_authorized_then_translated_to_user_dm(self, monkeypatch):
        from kiro_crew.dashboard import session_control as sc

        state, slots = self._state("chat-1-1", "member-fixer")
        monkeypatch.setattr(sc, "_resolve_slot", lambda st, target: slots.get(target))
        monkeypatch.setattr(sc, "caller_slot_key", lambda st, key: key)
        authorized: list[str] = []
        monkeypatch.setattr(
            sc,
            "authorize_target",
            lambda st, **kw: authorized.append((kw["operation"], kw["target"]))
            or slots["member-fixer"],
        )
        monkeypatch.setattr(sc, "_audit", lambda **kw: None)
        r = await sc._inbox_model_shim(
            state, caller_session_key="chat-1-1", target="member-fixer", body="please look"
        )
        # authorized on the resolved slot KEY, not the raw (title-resolvable) target
        assert authorized == [("send", "member-fixer")] and r is not None
        assert r["kind"] == "session_dm"
        env = InboxStore("fixer").pending()[0]
        assert env.kind == "session_dm" and env.from_ == "session:chat-1-1"
        assert InboxStore("fixer").last_user_dm_at() == ""  # does not refill the budget

    @pytest.mark.asyncio
    async def test_target_replaced_between_resolution_and_authorization_is_refused(
        self, monkeypatch
    ):
        """A title rename in the await window must not authorize slot B while the
        envelope lands in A's inbox: the authorized slot has to be the object
        resolved first, or the send is refused and nothing is written."""
        from kiro_crew.dashboard import session_control as sc

        state, slots = self._state("chat-1-1", "member-fixer")
        other = type(slots["member-fixer"])("member-scout", "member")
        monkeypatch.setattr(sc, "_resolve_slot", lambda st, target: slots.get(target))
        monkeypatch.setattr(sc, "caller_slot_key", lambda st, key: key)
        monkeypatch.setattr(sc, "authorize_target", lambda st, **kw: other)
        monkeypatch.setattr(sc, "_audit", lambda **kw: None)
        with pytest.raises(sc.SessionControlError) as exc:
            await sc._inbox_model_shim(
                state, caller_session_key="chat-1-1", target="member-fixer", body="please look"
            )
        assert exc.value.code == "target_replaced" and exc.value.status == 409
        assert InboxStore("fixer").pending() == [] and InboxStore("scout").pending() == []


_fold_state = object()  # save_slot_off_loop is stubbed, so the state is opaque here


@pytest.fixture(autouse=True)
def _stub_persist(monkeypatch, request):
    """The fold persists through the real history layer; these tests stub it and
    record the call so the ack-after-persist ordering is what is asserted."""
    if "Rollback" not in request.node.nodeid:
        return
    import kiro_crew.dashboard.chat_persistence as cp

    calls: list[tuple[Any, bool]] = []

    async def fake_save(state, slot, *args, **kw):
        calls.append((slot, kw.get("best_effort", True)))
        return True

    monkeypatch.setattr(cp, "save_slot_off_loop", fake_save)
    request.node._persist_calls = calls  # type: ignore[attr-defined]


class TestInboxIntake:
    @pytest.mark.asyncio
    async def test_intake_appends_the_row_mints_the_envelope_and_ends_the_local_turn(
        self, monkeypatch
    ):
        """No turn runs on the thread slot, but the client started an optimistic
        local turn when it sent and only a `chat_done` for the slot ends it: the
        intake must broadcast one, or the composer stays "running" forever."""
        from kiro_crew.dashboard.chat_handlers import _member_inbox_intake

        async def _no_persist(state, thread, key):
            return True  # persisted (the stub skips the disk)

        monkeypatch.setattr("kiro_crew.dashboard.member_wake._persist_thread_row", _no_persist)
        notified: list[str] = []
        monkeypatch.setattr(ms, "notify_member", lambda slug, kind: notified.append(kind))
        slot = _Slot("member-radar")
        slot.mode = "member"
        state = _State("ok")
        resp = await _member_inbox_intake(state, slot, "triage #42", {"mid": "m1"})
        assert resp is not None and resp.status == 200
        env = InboxStore("radar").pending()[0]
        assert env.kind == "user_dm" and env.from_ == "user" and env.body == "triage #42"
        assert slot.appended == [("user", "triage #42")]
        assert slot.messages[0]["meta"]["peer_dm"] == {"envelope_id": env.id, "kind": "user_dm"}
        assert notified == ["user_dm"]
        assert state.notified == ["ws:chat_done:member-radar"]  # the local turn ends

    @pytest.mark.asyncio
    async def test_failed_thread_save_is_stamped_and_restored_before_the_next_intake(
        self, monkeypatch
    ):
        """The envelope is the record and the thread row the view -- but a view that
        silently diverges is a message nobody sees. A save that fails stamps the
        envelope `view=pending`; the next intake restores the row (re-saving the
        live window when it still holds the row, appending when it does not) and
        clears the stamp only after a confirmed write."""
        import kiro_crew.dashboard.chat_persistence as cp
        from kiro_crew.dashboard.chat_handlers import _member_inbox_intake

        monkeypatch.setattr(ms, "notify_member", lambda slug, kind: None)
        saves: list[str] = []
        outcome = {"ok": False}

        async def save(state, slot, *a, **kw):
            saves.append(slot.key)
            if not outcome["ok"]:
                raise OSError("disk")
            return True

        monkeypatch.setattr(cp, "save_slot_off_loop", save)
        slot = _Slot("member-radar")
        slot.mode = "member"
        state = _State("ok")
        state.thread = slot
        assert (await _member_inbox_intake(state, slot, "first", None)).status == 200
        first = InboxStore("radar").pending()[0]
        assert first.refs[mi.VIEW_REF] == mi.VIEW_PENDING and saves == ["member-radar"]

        # Disk is back; the live window still holds the row -> re-saved, not duplicated.
        outcome["ok"] = True
        assert (await _member_inbox_intake(state, slot, "second", None)).status == 200
        assert [m["content"] for m in slot.messages] == ["first", "second"]
        pend = {e.body: e for e in InboxStore("radar").pending()}
        assert pend["first"].refs[mi.VIEW_REF] == mi.VIEW_RESTORED
        assert mi.VIEW_REF not in pend["second"].refs  # its own save succeeded
        assert saves == ["member-radar", "member-radar", "member-radar"]  # restore + own

        # A window that LOST the row (restart) gets it appended again, in order.
        InboxStore("radar").mark(pend["first"].id, **{mi.VIEW_REF: mi.VIEW_PENDING})
        fresh = _Slot("member-radar")
        fresh.mode = "member"
        state.thread = fresh
        assert (await _member_inbox_intake(state, fresh, "third", None)).status == 200
        assert [m["content"] for m in fresh.messages] == ["first", "third"]
        assert fresh.messages[0]["meta"]["peer_dm"]["envelope_id"] == pend["first"].id
        assert InboxStore("radar").view_pending() == []

    @pytest.mark.asyncio
    async def test_intake_falls_through_for_an_unflagged_member(self, monkeypatch):
        from kiro_crew.dashboard.chat_handlers import _member_inbox_intake

        monkeypatch.setattr("kiro_crew.member_scheduler.member_wake_running", lambda slug: False)
        slot = _Slot("member-nobody")
        slot.mode = "member"
        state = _State("ok")
        assert await _member_inbox_intake(state, slot, "hi", None) is None
        assert state.notified == [] and slot.appended == []


class TestRollbackFold:
    @pytest.mark.asyncio
    async def test_unflagged_member_with_pending_envelopes_folds_them_as_an_inject_row(self):
        from kiro_crew.dashboard.chat_handlers import _fold_stranded_envelopes

        store = InboxStore("scribe")  # not flagged
        store.append(_env("scribe", body="left over from before"))
        store.append(_env("scribe", kind="peer_dm", body="peer note", from_="member:radar", hop=1))
        slot = _Slot("member-scribe")
        assert await _fold_stranded_envelopes(_fold_state, slot, "scribe") == 2
        role, text = slot.appended[0]
        assert role == "inject"
        assert "2 envelope(s) arrived while this member was on the inbox model" in text
        assert "kind=user_dm from=user" in text and "left over from before" in text
        assert "kind=peer_dm from=member:radar" in text and "peer note" in text
        assert store.pending() == [] and len(store.acked()) == 2

    @pytest.mark.asyncio
    async def test_fold_carries_complete_multiline_bodies(self):
        from kiro_crew.dashboard.chat_handlers import _fold_stranded_envelopes

        store = InboxStore("scribe")
        body = "line one\nline two\n" + ("x" * 900)
        store.append(_env("scribe", body=body))
        slot = _Slot("member-scribe")
        await _fold_stranded_envelopes(_fold_state, slot, "scribe")
        _, text = slot.appended[0]
        assert "line one" in text and "line two" in text and "x" * 900 in text

    @pytest.mark.asyncio
    async def test_ack_follows_the_durable_row_never_precedes_it(self, monkeypatch, request):
        import kiro_crew.dashboard.chat_persistence as cp
        from kiro_crew.dashboard.chat_handlers import _fold_stranded_envelopes

        store = InboxStore("scribe")
        store.append(_env("scribe"))
        slot = _Slot("member-scribe")
        await _fold_stranded_envelopes(_fold_state, slot, "scribe")
        calls = request.node._persist_calls
        assert calls and calls[0][1] is False  # propagating save, not best-effort
        assert store.pending() == []

        # a save that reports failure, or raises, leaves the envelopes pending
        store.append(_env("scribe", body="second"))

        async def refused(state, slot, *a, **kw):
            return False

        monkeypatch.setattr(cp, "save_slot_off_loop", refused)
        slot2 = _Slot("member-scribe")
        slot2.messages.append({"role": "user", "content": "earlier"})
        with pytest.raises(RuntimeError):
            await _fold_stranded_envelopes(_fold_state, slot2, "scribe")
        assert len(store.pending()) == 1
        # the live row is withdrawn too: the next turn must not consume a delivery
        # the store still owes, or the envelopes would arrive twice
        assert [m["role"] for m in slot2.messages] == ["user"]

        async def raising(state, slot, *a, **kw):
            raise OSError("disk")

        monkeypatch.setattr(cp, "save_slot_off_loop", raising)
        slot3 = _Slot("member-scribe")
        with pytest.raises(OSError):
            await _fold_stranded_envelopes(_fold_state, slot3, "scribe")
        assert len(store.pending()) == 1 and slot3.messages == []

    @pytest.mark.asyncio
    async def test_fold_row_is_staged_and_announced_only_after_the_write(
        self, monkeypatch, request
    ):
        """`slot.append` broadcasts and books the row immediately; a save that then
        fails could withdraw the live row but not the push the client already
        rendered, and the same envelopes would fold again -- delivered twice. The
        row is appended WITHOUT a broadcast, announced after the confirmed write,
        and a failed write takes back the delivery queue and the count too."""
        import kiro_crew.dashboard.chat_persistence as cp
        from kiro_crew.dashboard.chat_handlers import _fold_stranded_envelopes

        class _BookedSlot(_Slot):
            def __init__(self, key):
                super().__init__(key)
                self._pending: list[dict] = []
                self.total_messages = 0
                self.pushed: list[dict] = []
                self._has_reader = False
                self._on_message = lambda key, row: self.pushed.append(row)
                self.broadcasts: list[bool] = []

            def append(self, role, content, cls="", *, meta=None, broadcast=True, **_):
                self.broadcasts.append(broadcast)
                row = super().append(role, content, cls, meta=meta)
                row = self.messages[-1]
                self._pending.append(row)
                self.total_messages += 1
                if broadcast:
                    self._on_message(self.key, row)
                return row

        store = InboxStore("scribe")
        store.append(_env("scribe"))
        slot = _BookedSlot("member-scribe")
        assert await _fold_stranded_envelopes(_fold_state, slot, "scribe") == 1
        assert slot.broadcasts == [False]  # staged
        assert len(slot.pushed) == 1 and slot.pushed[0] is slot.messages[0]  # announced after
        assert slot.total_messages == 1 and store.pending() == []

        store.append(_env("scribe", body="again"))

        async def refused(state, slot, *a, **kw):
            return False

        monkeypatch.setattr(cp, "save_slot_off_loop", refused)
        slot2 = _BookedSlot("member-scribe")
        with pytest.raises(RuntimeError):
            await _fold_stranded_envelopes(_fold_state, slot2, "scribe")
        assert slot2.messages == [] and slot2._pending == [] and slot2.total_messages == 0
        assert slot2.pushed == []  # nothing reached the client
        assert len(store.pending()) == 1

    @pytest.mark.asyncio
    async def test_fold_waits_out_an_in_flight_wake_entirely(self, monkeypatch):
        """A wake that started before the flag flipped may still CLAIM an envelope
        after any snapshot this fold could take, so filtering by attempts would
        race it and deliver twice. While a wake runs, the fold takes nothing;
        the next message folds what is still pending once it has ended."""
        from kiro_crew.dashboard.chat_handlers import _fold_stranded_envelopes

        store = InboxStore("scribe")
        drained = store.append(_env("scribe", body="being handled by a wake"))
        store.mark_attempt([drained.id])
        fresh = store.append(_env("scribe", body="never seen by a wake"))
        monkeypatch.setattr("kiro_crew.member_scheduler.member_wake_running", lambda slug: True)
        slot = _Slot("member-scribe")
        assert await _fold_stranded_envelopes(_fold_state, slot, "scribe") == 0
        assert slot.appended == [] and len(store.pending()) == 2
        # the wake ended (it acked its own); the next message folds the rest
        store.ack([drained.id])
        monkeypatch.setattr("kiro_crew.member_scheduler.member_wake_running", lambda slug: False)
        assert await _fold_stranded_envelopes(_fold_state, slot, "scribe") == 1
        assert "never seen by a wake" in slot.appended[0][1] and store.pending() == []
        assert fresh.id in {e.id for e in store.acked()}

    @pytest.mark.asyncio
    async def test_nothing_pending_folds_nothing(self):
        from kiro_crew.dashboard.chat_handlers import _fold_stranded_envelopes

        assert await _fold_stranded_envelopes(_fold_state, _Slot("member-scribe"), "scribe") == 0


class TestConfigSections:
    """``members`` / ``member_peer_dm`` are modelled sections: loaded without an
    unrecognized-key warning, round-tripped by ``to_dict``, read by the flag
    helpers from the field (not the unknown-section passthrough)."""

    def _load(self, tmp_path, monkeypatch, caplog, data):
        import json

        from kiro_crew.config import loader as L
        from kiro_crew.config.loader import KiroCrewConfig

        cfgp = tmp_path / "config.json"
        cfgp.write_text(json.dumps({"agent": {"provider": "acp"}, **data}))
        monkeypatch.setattr(L, "config_path", lambda: cfgp)
        monkeypatch.setattr(L, "config_dir", lambda: tmp_path)
        monkeypatch.setattr(L, "config_local_path", lambda: tmp_path / "config.local.json")
        with caplog.at_level("WARNING", logger="kiro_crew.config"):
            return KiroCrewConfig.load()

    def test_sections_load_round_trip_and_do_not_warn(self, tmp_path, monkeypatch, caplog):
        members = {"radar": {"inbox_model": True, "wake_interval_secs": 900}}
        cfg = self._load(
            tmp_path,
            monkeypatch,
            caplog,
            {"members": members, "member_peer_dm": {"enabled": False}},
        )
        assert cfg.members == members and cfg.member_peer_dm == {"enabled": False}
        assert "members" not in cfg._extra_sections and "member_peer_dm" not in cfg._extra_sections
        out = cfg.to_dict()
        assert out["members"] == members and out["member_peer_dm"] == {"enabled": False}
        assert "unrecognized top-level keys" not in caplog.text

    def test_flag_helpers_read_the_modelled_field(self, tmp_path, monkeypatch, caplog):
        cfg = self._load(
            tmp_path,
            monkeypatch,
            caplog,
            {
                "members": {"radar": {"inbox_model": True, "peer_dm": {"send": False}}, "x": {}},
                "member_peer_dm": {"enabled": False},
            },
        )
        from kiro_crew.config.loader import KiroCrewConfig

        monkeypatch.setattr(KiroCrewConfig, "load", classmethod(lambda cls, *a, **k: cfg))
        r = _REAL_READERS
        assert r["enabled"]("radar") is True and r["enabled"]("x") is False
        assert r["enabled"]("nope") is False
        assert r["setting"]("radar", "peer_dm", None) == {"send": False}
        assert r["flagged"]() == ["radar"]
        assert r["global"]() is False

    def test_malformed_section_still_fails_closed(self, tmp_path, monkeypatch, caplog):
        cfg = self._load(
            tmp_path, monkeypatch, caplog, {"members": ["radar"], "member_peer_dm": "yes"}
        )
        from kiro_crew.config.loader import KiroCrewConfig

        monkeypatch.setattr(KiroCrewConfig, "load", classmethod(lambda cls, *a, **k: cfg))
        r = _REAL_READERS
        assert r["enabled"]("radar") is False and r["flagged"]() == []
        with pytest.raises(mp._Degraded):
            r["global"]()


class TestMemberPinCoversWakes:
    """Every member-pin site keys on `is_member_mode`, so a wake slot (`member-wake`)
    is pinned to its crew exactly like the DM thread: a provider-side agent
    switch mid-wake is vetoed (test_members_dm_thread covers the runner path with a
    `member-wake` parametrization), and the HTTP pins refuse a re-bind."""

    def test_no_pin_site_compares_the_dm_mode_literal(self):
        import re
        from pathlib import Path

        from kiro_crew import dashboard

        root = Path(dashboard.__file__).parent
        offenders = []
        for path in root.rglob("*.py"):
            for n, line in enumerate(path.read_text().splitlines(), 1):
                code = line.split("#", 1)[0]
                if re.search(r'\.mode\s*==\s*"member"', code):
                    offenders.append(f"{path.relative_to(root)}:{n}")
        assert offenders == []


class TestActivityLog:
    def test_wake_slots_are_not_recorded_as_chat_activity(self):
        """Source contract: the one `record_activity(..., via="chat")` site in the
        chat runner excludes `member-wake` slots -- the scheduler minted them,
        nobody picked the member, and one row per wake grows with the timer."""
        import inspect

        from kiro_crew.dashboard import chat_runner

        src = inspect.getsource(chat_runner)
        i = src.index("record_activity,\n")
        guard = src[max(0, i - 600) : i]
        assert "slot.mode != WAKE_SLOT_MODE" in guard


class TestOwnershipFold:
    def test_fence_compares_against_the_member_key(self):
        from kiro_crew.dashboard import session_control as sc

        assert sc._member_owner_key("member-radar.wake-9") == "member-radar"
        assert sc._member_owner_key("cron-abc") == "cron-abc"
