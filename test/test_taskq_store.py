"""TaskStore: write-before-ack, journal mode selection, reads and the event log."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from overload_fakes import Clock, open_task_store

from kiro_crew.taskq import migrate, model
from kiro_crew.taskq.store import (
    TaskStore,
    TaskStoreUnavailable,
    detect_network_filesystem,
)


def _rec(task_id: str, **kw) -> model.TaskRecord:
    kw.setdefault("kind", model.KIND_SUBAGENT)
    kw.setdefault("params", {"task": f"task {task_id}"})
    return model.TaskRecord(id=task_id, **kw)


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def store(tmp_path: Path, clock: Clock) -> TaskStore:
    yield from open_task_store(tmp_path, clock, name="tasks/tasks.db", window=4)


# ── open / schema / journal ───────────────────────────────────────────────────


def test_open_creates_file_wal_and_schema(store: TaskStore, tmp_path: Path) -> None:
    assert (tmp_path / "tasks" / "tasks.db").exists()
    assert store.journal_mode == "wal"
    conn = sqlite3.connect(str(store.path))
    try:
        assert migrate.read_schema_version(conn) == migrate.SCHEMA_VERSION
        tables = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"  # wokeignore:rule=master
            )
        }
    finally:
        conn.close()
    assert {"tasks", "task_events", "meta"} <= tables
    assert store.warnings == []


def test_network_filesystem_uses_delete_journal_and_warns(tmp_path: Path) -> None:
    s = TaskStore(tmp_path / "t.db", network_fs=True).open()
    try:
        assert s.journal_mode == "delete"
        assert any("network filesystem" in w for w in s.warnings)
        assert any("journal_mode=DELETE" in w for w in s.warnings)
        # still fully functional: never refuses
        assert s.accept([_rec("a")]) == ["a"]
        assert any("journal_mode=delete" in line for line in s.doctor_lines())
    finally:
        s.close()


def test_detect_network_filesystem_on_tmp_is_not_true(tmp_path: Path) -> None:
    verdict = detect_network_filesystem(tmp_path)
    assert verdict in (False, None)


def test_detect_network_filesystem_recognizes_linux_mount_table(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from kiro_crew.taskq import store as store_mod

    monkeypatch.setattr(store_mod.sys, "platform", "linux")
    monkeypatch.setattr(store_mod, "_linux_mount_type", lambda p: "nfs4")
    assert detect_network_filesystem(tmp_path) is True
    monkeypatch.setattr(store_mod, "_linux_mount_type", lambda p: "ext4")
    assert detect_network_filesystem(tmp_path) is False
    monkeypatch.setattr(store_mod, "_linux_mount_type", lambda p: None)
    assert detect_network_filesystem(tmp_path) is None


def test_reopen_records_previous_incarnation(tmp_path: Path) -> None:
    path = tmp_path / "t.db"
    first = TaskStore(path, network_fs=False).open()
    first_id = first.incarnation
    first.close()
    second = TaskStore(path, network_fs=False).open()
    try:
        assert second.previous_incarnation == first_id
        assert second.incarnation != first_id
    finally:
        second.close()


def test_newer_schema_version_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "t.db"
    conn = sqlite3.connect(str(path))
    conn.execute("CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    conn.execute("INSERT INTO meta VALUES('schema_version', '99')")
    conn.commit()
    conn.close()
    with pytest.raises(TaskStoreUnavailable):
        TaskStore(path, network_fs=False).open()


def test_unopenable_path_raises_typed_error(tmp_path: Path) -> None:
    blocker = tmp_path / "file"
    blocker.write_text("x", encoding="utf-8")
    with pytest.raises(TaskStoreUnavailable):
        TaskStore(blocker / "tasks.db", network_fs=False).open()


# ── accept: write-before-ack ──────────────────────────────────────────────────


def test_accept_returns_ids_only_after_commit(store: TaskStore, clock: Clock) -> None:
    ids = store.accept([_rec("a"), _rec("b")])
    assert ids == ["a", "b"]
    assert store.count(state=model.QUEUED) == 2
    got = store.get("a")
    assert got is not None and got.created_at == clock.t and got.state == model.QUEUED
    assert [e.kind for e in store.events("a")] == ["accepted"]


class _FailingConn:
    """Forwards to a real connection, but the Nth ``INSERT INTO tasks`` fails."""

    def __init__(self, real: sqlite3.Connection, fail_on_insert: int) -> None:
        self._real = real
        self._fail_on = fail_on_insert
        self._inserts = 0

    def execute(self, sql: str, *args):  # type: ignore[no-untyped-def]
        if sql.lstrip().upper().startswith("INSERT INTO TASKS"):
            self._inserts += 1
            if self._inserts == self._fail_on:
                raise sqlite3.OperationalError("disk I/O error")
        return self._real.execute(sql, *args)

    def __getattr__(self, name: str):  # type: ignore[no-untyped-def]
        return getattr(self._real, name)


def test_accept_write_failure_raises_and_commits_nothing(store: TaskStore) -> None:
    real = store._conn
    assert real is not None
    store._conn = _FailingConn(real, fail_on_insert=2)  # type: ignore[assignment]
    with pytest.raises(TaskStoreUnavailable):
        store.accept([_rec("ok1"), _rec("boom")])
    store._conn = real
    # the whole batch rolled back: not even the first row is there
    assert store.count() == 0
    assert store.get("ok1") is None
    # and the store is still usable afterwards
    assert store.accept([_rec("after")]) == ["after"]


def test_accept_duplicate_id_is_a_refusal_not_an_ack(store: TaskStore) -> None:
    store.accept([_rec("dup")])
    with pytest.raises(TaskStoreUnavailable):
        store.accept([_rec("dup")])
    assert store.count() == 1


def test_accept_rejects_non_claimable_initial_state(store: TaskStore) -> None:
    with pytest.raises(TaskStoreUnavailable):
        store.accept([_rec("r", state=model.RUNNING)])
    assert store.count() == 0


def test_idempotency_key_is_unique_when_present(store: TaskStore) -> None:
    store.accept([_rec("k1", idempotency_key="key")])
    with pytest.raises(TaskStoreUnavailable):
        store.accept([_rec("k2", idempotency_key="key")])
    # NULL keys never collide
    store.accept([_rec("n1"), _rec("n2")])
    assert store.count() == 3


def test_insert_if_absent_is_idempotent(store: TaskStore) -> None:
    assert store.insert_if_absent(_rec("i", state=model.RECOVERING)) is True
    assert store.insert_if_absent(_rec("i", state=model.QUEUED)) is False
    got = store.get("i")
    assert got is not None and got.state == model.RECOVERING
    assert [e.kind for e in store.events("i")] == ["imported"]


def test_locked_database_is_reported_not_hung(tmp_path: Path) -> None:
    """A writer holding the lock past the busy timeout makes accept() refuse."""
    path = tmp_path / "t.db"
    s = TaskStore(path, network_fs=False, busy_timeout_secs=0.05).open()
    try:
        other = sqlite3.connect(str(path), isolation_level=None)
        other.execute("BEGIN IMMEDIATE")
        other.execute("INSERT INTO meta VALUES('hold', 'x')")
        with pytest.raises(TaskStoreUnavailable):
            s.accept([_rec("a")])
        other.execute("ROLLBACK")
        other.close()
        assert s.accept([_rec("a")]) == ["a"]
    finally:
        s.close()


# ── defer / reads / window helpers ────────────────────────────────────────────


def test_defer_keeps_row_queued_but_ineligible_until_clock_passes(
    store: TaskStore, clock: Clock
) -> None:
    store.accept([_rec("d")])
    assert store.defer("d", clock.t + 30, reason="low memory") is True
    assert store.state_of("d") == model.QUEUED
    assert store.fetch_dispatchable(model.KIND_SUBAGENT, limit=10) == []
    assert store.count_pending(model.KIND_SUBAGENT) == 1
    assert store.count_pending(model.KIND_SUBAGENT, eligible_only=True) == 0
    assert store.next_eligible_at(model.KIND_SUBAGENT) == clock.t + 30
    clock.t += 31
    assert [r.id for r in store.fetch_dispatchable(model.KIND_SUBAGENT, limit=10)] == ["d"]
    assert [e.kind for e in store.events("d")] == ["accepted", "deferred"]


def test_defer_on_terminal_row_is_a_noop(store: TaskStore, clock: Clock) -> None:
    store.accept([_rec("t")])
    assert store.cancel("t") == model.QUEUED
    assert store.defer("t", clock.t + 5, reason="x") is False


def test_fetch_dispatchable_is_fifo_and_excludes_window_ids(store: TaskStore, clock: Clock) -> None:
    for i in range(6):
        clock.t += 1
        store.accept([_rec(f"r{i}")])
    got = store.fetch_dispatchable(model.KIND_SUBAGENT, limit=3, exclude_ids=["r0", "r2"])
    assert [r.id for r in got] == ["r1", "r3", "r4"]
    assert store.count_pending(model.KIND_SUBAGENT, exclude_ids=["r0", "r2"]) == 4


def test_pending_by_batch_and_session_filters(store: TaskStore) -> None:
    store.accept(
        [
            _rec("b1", session_key="s1", params={"batch_id": "w"}),
            _rec("b2", session_key="s2", params={"batch_id": "w"}),
            _rec("b3", session_key="s1", params={"batch_id": "z"}),
        ]
    )
    assert {r.id for r in store.fetch_pending_by_batch(model.KIND_SUBAGENT, "w")} == {"b1", "b2"}
    assert [r.id for r in store.list_pending(model.KIND_SUBAGENT, session_key="s1")] == [
        "b1",
        "b3",
    ]
    assert store.count_pending(model.KIND_SUBAGENT, session_key="s2") == 1


def test_count_by_state_oldest_wait_and_doctor_lines(store: TaskStore, clock: Clock) -> None:
    store.accept([_rec("x")])
    clock.t += 42
    store.accept([_rec("y")])
    store.claim("y")
    assert store.count_by_state() == {model.QUEUED: 1, model.ADMITTED: 1}
    assert store.oldest_wait_secs() == 42.0
    lines = store.doctor_lines()
    assert "pending=1 active=1 oldest_wait=42s" in lines[1]


def test_record_progress_and_events_respect_generation(store: TaskStore) -> None:
    store.accept([_rec("p")])
    claimed = store.claim("p")
    assert claimed is not None
    assert store.record_progress("p", claimed.generation, {"step": 1}) is True
    assert store.record_progress("p", claimed.generation + 5, {"step": 2}) is False
    got = store.get("p")
    assert got is not None and got.progress == {"step": 1}
    store.append_event("p", "deliver", {"to": "dashboard:1"})
    assert store.events("p")[-1].kind == "deliver"


def test_closed_store_raises_typed_error(tmp_path: Path) -> None:
    s = TaskStore(tmp_path / "t.db", network_fs=False).open()
    s.close()
    with pytest.raises(TaskStoreUnavailable):
        s.accept([_rec("a")])
    assert s.is_open is False


def test_default_path_under_home(tmp_path: Path) -> None:
    assert TaskStore.default_path(tmp_path) == tmp_path / "tasks" / "tasks.db"


def test_diagnostic_open_does_not_record_incarnation(tmp_path: Path) -> None:
    path = tmp_path / "t.db"
    owner = TaskStore(path, network_fs=False).open()
    owner_id = owner.incarnation
    owner.close()
    reader = TaskStore(path, network_fs=False, diagnostic=True).open()
    try:
        assert reader.previous_incarnation is None  # never read, never written
        assert reader.doctor_lines()[1].strip().startswith("pending=0")
    finally:
        reader.close()
    again = TaskStore(path, network_fs=False).open()
    try:
        assert again.previous_incarnation == owner_id
    finally:
        again.close()


def test_doctor_task_store_is_silent_without_store_and_reports_with_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from kiro_crew import cli_doctor

    monkeypatch.setenv("KIROCREW_HOME", str(tmp_path))
    issues: list[str] = []
    cli_doctor._doctor_task_store(issues)
    assert capsys.readouterr().out == "" and issues == []
    s = TaskStore(tmp_path / "tasks" / "tasks.db", network_fs=False).open()
    s.accept([_rec("a")])
    s.close()
    from kiro_crew.taskq import store as store_mod

    monkeypatch.setattr(store_mod, "detect_network_filesystem", lambda p: True)
    cli_doctor._doctor_task_store(issues)
    out = capsys.readouterr().out
    assert "task store:" in out and "pending=1" in out
    assert any("network filesystem" in i for i in issues)


# ── a corrupt file is quarantined and recreated; a locked one still refuses ──


def _corrupt_db(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"this is not a sqlite database at all\n" * 64)
    (path.parent / (path.name + "-wal")).write_bytes(b"stale wal")


def test_corrupt_store_is_quarantined_and_recreated(tmp_path: Path) -> None:
    path = tmp_path / "tasks" / "tasks.db"
    _corrupt_db(path)
    store = TaskStore(path, window=8, network_fs=False).open()
    try:
        assert store.quarantined_to is not None
        quarantined = store.quarantined_to
        assert quarantined.exists() and quarantined.name.startswith("tasks.db.corrupt-")
        assert quarantined.read_bytes().startswith(b"this is not a sqlite database")
        # The stale WAL never survives beside the fresh store: SQLite drops an
        # invalid one on close, and whatever is left is moved with the file.
        wal = path.parent / "tasks.db-wal"
        assert not wal.exists() or wal.read_bytes() != b"stale wal"
        assert any("quarantined" in w for w in store.warnings)
        # A fresh, usable store: the schema is there and work is accepted.
        with sqlite3.connect(str(path)) as conn:
            names = {
                r[0]
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"  # wokeignore:rule=master
                )
            }
        assert {"tasks", "task_events"} <= names
        assert store.accept_one(_rec("fresh1", session_key="web-1")) == "fresh1"
        assert store.state_of("fresh1") == model.QUEUED
    finally:
        store.close()


def test_two_quarantines_in_the_same_second_keep_both_copies(tmp_path: Path, monkeypatch) -> None:
    """A crash loop quarantines repeatedly; every copy is evidence and none is
    overwritten, even with a frozen clock and one pid."""
    from kiro_crew.taskq import store as store_mod

    monkeypatch.setattr(store_mod.time, "time", lambda: 1_800_000_000.25)
    path = tmp_path / "tasks" / "tasks.db"
    copies = []
    for payload in (b"first corrupt copy\n" * 64, b"second corrupt copy\n" * 64):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        store = TaskStore(path, window=8, network_fs=False).open()
        try:
            assert store.quarantined_to is not None
            copies.append(store.quarantined_to)
        finally:
            store.close()
    assert len({c.name for c in copies}) == 2
    assert copies[0].read_bytes().startswith(b"first corrupt copy")
    assert copies[1].read_bytes().startswith(b"second corrupt copy")
    assert copies[1].name == f"{copies[0].name}-1"
    listed = [p.name for p in path.parent.glob("tasks.db.corrupt-*")]
    assert sorted(listed) == sorted(c.name for c in copies)


def test_stale_journal_is_quarantined_with_the_corrupt_file(tmp_path: Path) -> None:
    """A DELETE-mode rollback journal beside the corrupt file moves with it;
    left behind, SQLite would roll it into the recreated database. The move is
    exercised directly: SQLite itself deletes a journal it finds unreadable, so
    an open() round trip cannot tell the two removals apart."""
    path = tmp_path / "tasks" / "tasks.db"
    _corrupt_db(path)
    journal = path.parent / "tasks.db-journal"
    journal.write_bytes(b"\xd9\xd5\x05\xf9\x20\xa1\x63\xd7" + b"hot journal" * 16)
    store = TaskStore(path, window=8, network_fs=False)
    target = store._quarantine_corrupt_file("file is not a database")
    moved_journal = target.with_name(target.name + "-journal")
    assert moved_journal.exists() and moved_journal.read_bytes().endswith(b"hot journal")
    assert not journal.exists() and not path.exists()
    assert target.with_name(target.name + "-wal").read_bytes() == b"stale wal"
    assert target.read_bytes().startswith(b"this is not a sqlite database")
    assert any("-journal" in w for w in store.warnings)
    store.open()
    try:
        assert store.accept_one(_rec("fresh2", session_key="web-1")) == "fresh2"
        assert store.state_of("fresh2") == model.QUEUED
        with sqlite3.connect(str(path)) as conn:
            assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    finally:
        store.close()


def test_diagnostic_open_reports_corruption_without_moving_the_file(tmp_path: Path) -> None:
    path = tmp_path / "tasks" / "tasks.db"
    _corrupt_db(path)
    before = path.read_bytes()
    with pytest.raises(TaskStoreUnavailable, match="corrupt"):
        TaskStore(path, diagnostic=True).open()
    assert path.read_bytes() == before, "doctor never quarantines"
    assert not list(path.parent.glob("tasks.db.corrupt-*"))


def test_locked_store_still_refuses_instead_of_quarantining(tmp_path: Path, monkeypatch) -> None:
    from kiro_crew.taskq import store as store_mod

    path = tmp_path / "tasks" / "tasks.db"
    TaskStore(path, window=8, network_fs=False).open().close()  # a healthy file

    def _locked(*_a, **_k):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(store_mod.sqlite3, "connect", _locked)
    with pytest.raises(TaskStoreUnavailable, match="locked"):
        TaskStore(path, window=8, network_fs=False).open()
    assert path.exists() and not list(path.parent.glob("tasks.db.corrupt-*"))
    # Disk-full is a host condition too, never a quarantine.
    monkeypatch.setattr(
        store_mod.sqlite3,
        "connect",
        lambda *a, **k: (_ for _ in ()).throw(sqlite3.OperationalError("database or disk is full")),
    )
    with pytest.raises(TaskStoreUnavailable, match="full"):
        TaskStore(path, window=8, network_fs=False).open()
    assert not list(path.parent.glob("tasks.db.corrupt-*"))


def test_is_corruption_classifies_sqlite_errors() -> None:
    from kiro_crew.taskq.store import _is_corruption

    assert _is_corruption(sqlite3.DatabaseError("file is not a database"))
    assert _is_corruption(sqlite3.DatabaseError("database disk image is malformed"))
    assert _is_corruption(sqlite3.DatabaseError("malformed database schema (tasks)"))
    assert not _is_corruption(sqlite3.OperationalError("database is locked"))
    assert not _is_corruption(sqlite3.OperationalError("database is busy"))
    assert not _is_corruption(sqlite3.OperationalError("unable to open database file"))
    assert not _is_corruption(sqlite3.OperationalError("database or disk is full"))
    assert not _is_corruption(sqlite3.OperationalError("attempt to write a readonly database"))
    assert not _is_corruption(OSError("permission denied"))
    # A word that merely occurs in a message or a path is not a verdict.
    assert not _is_corruption(sqlite3.OperationalError("unable to open /var/corrupt-dir/tasks.db"))
    # A schema newer than this build is a refusal, never a quarantine (downgrade).
    assert not _is_corruption(
        sqlite3.DatabaseError("tasks.db schema version 99 is newer than this build supports")
    )
