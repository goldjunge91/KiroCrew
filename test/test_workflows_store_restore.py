"""Restart recovery accepts root aliases, never redirected run files."""

import json
import os

import pytest

from conftest import make_dir_link
from kiro_crew.workflow_memory import publish_binding
from kiro_crew.workflows.registry import RunHandle, RunRegistry
from kiro_crew.workflows.store import WorkflowRunStore


def _save(base, run_id="wf_000001"):
    store = WorkflowRunStore(base_dir=base)
    registry = RunRegistry(store=store)
    publish_binding(run_id, "", "dashboard:test")
    handle = RunHandle(run_id=run_id, name="restart", source="large source " * 1000)
    handle.execution_binding_version = 1
    registry.register(handle)
    registry.mark_terminal(run_id, "finished", result={"saved": True})
    return store


def test_restart_restores_public_run_through_symlink_ancestor(tmp_path, monkeypatch):
    actual = tmp_path / "actual"
    actual.mkdir()
    alias = tmp_path / "alias"
    make_dir_link(alias, actual)
    monkeypatch.setenv("KIROCREW_HOME", str(alias))
    _save(alias / "workflows")
    restored = RunRegistry(store=WorkflowRunStore(base_dir=alias / "workflows"))
    assert restored.load_persisted() == 1
    run = restored.get("wf_000001")
    assert run.result == {"saved": True}
    assert run.source == "large source " * 1000


@pytest.mark.parametrize("kind", ["hardlink", "leaf", "directory"])
def test_restart_refuses_linked_run_files(tmp_path, kind):
    store = _save(tmp_path / "workflows")
    path = store.runs_dir / "wf_000001.json"
    outside = tmp_path / "outside.json"
    path.rename(outside)
    if kind == "hardlink":
        os.link(outside, path)
    elif kind == "directory":
        make_dir_link(path, tmp_path)
    else:
        try:
            path.symlink_to(outside)
        except OSError as exc:
            if getattr(exc, "winerror", None) == 1314:
                pytest.skip("File symlink privilege unavailable; directory link covered separately")
            raise
    assert RunRegistry(store=store).load_persisted() == 0


def test_restart_refuses_ancestor_swap_outside_resolved_root(tmp_path, monkeypatch):
    from kiro_crew import platform_compat

    store = _save(tmp_path / "workflows")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "wf_000001.json").write_text(
        json.dumps({"run_id": "wf_000001", "status": "finished", "result": "foreign"})
    )
    real_open = platform_compat.open_file_no_reparse

    def swap_then_open(path, **kwargs):
        store.runs_dir.rename(tmp_path / "retired")
        make_dir_link(store.runs_dir, outside)
        return real_open(path, **kwargs)

    monkeypatch.setattr(platform_compat, "open_file_no_reparse", swap_then_open)
    assert RunRegistry(store=store).load_persisted() == 0


def test_restart_requires_surviving_protected_binding(tmp_path):
    from kiro_crew.workflow_memory import binding_path

    store = _save(tmp_path / "workflows")
    binding_path("wf_000001").unlink()
    assert RunRegistry(store=store).load_persisted() == 0


def test_restart_isolates_unresolvable_discovery_root(tmp_path, monkeypatch):
    from pathlib import Path

    store = _save(tmp_path / "workflows")
    real_resolve = Path.resolve

    def resolve(path, *args, **kwargs):
        if path == store.runs_dir:
            raise OSError("discovery root disappeared")
        return real_resolve(path, *args, **kwargs)

    monkeypatch.setattr(Path, "resolve", resolve)
    assert store.load_all() == []


def test_restart_keeps_public_rows_when_private_root_discovery_fails(tmp_path, monkeypatch):
    from kiro_crew.workflows import store as store_module

    store = _save(tmp_path / "workflows")
    original = store_module.private_payload_path

    def private_path(run_id):
        if run_id == "discovery":
            raise OSError("private discovery unavailable")
        return original(run_id)

    monkeypatch.setattr(store_module, "private_payload_path", private_path)
    rows = store.load_all()
    assert len(rows) == 1 and rows[0]["run_id"] == "wf_000001"


@pytest.mark.parametrize("failure", ["private-root", "root", "record"])
def test_discovery_diagnostics_never_log_private_text(tmp_path, monkeypatch, caplog, failure):
    import logging
    from pathlib import Path

    from kiro_crew.workflows import store as store_module

    store = _save(tmp_path / "workflows")
    sentinel = "PRIVATE_DISCOVERY_SECRET_SENTINEL"
    if failure == "private-root":

        def fail_private_path(_run_id):
            raise OSError(sentinel)

        monkeypatch.setattr(store_module, "private_payload_path", fail_private_path)
    elif failure == "root":
        real_resolve = Path.resolve

        def resolve(path, *args, **kwargs):
            if path == store.runs_dir:
                raise OSError(sentinel)
            return real_resolve(path, *args, **kwargs)

        monkeypatch.setattr(Path, "resolve", resolve)
    else:
        path = store.runs_dir / f"{sentinel}.json"
        path.write_text("broken", encoding="utf-8")
        real_open = store_module.platform_compat.open_file_no_reparse

        def open_record(candidate, **kwargs):
            if candidate == path:
                raise OSError(sentinel)
            return real_open(candidate, **kwargs)

        monkeypatch.setattr(store_module.platform_compat, "open_file_no_reparse", open_record)
    with caplog.at_level(logging.DEBUG, logger=store_module.__name__):
        rows = store.load_all()
    assert len(rows) == (0 if failure == "root" else 1)
    records = [record for record in caplog.records if record.name == store_module.__name__]
    assert records, "Recovery must retain a useful diagnostic"
    assert "OSError" in caplog.text
    assert sentinel not in caplog.text
    assert str(tmp_path) not in caplog.text
    assert all(record.exc_info is None and record.exc_text is None for record in records)


def test_unreadable_record_with_surrogate_name_does_not_abort_recovery(tmp_path, monkeypatch):
    from pathlib import Path

    from kiro_crew.workflows import store as store_module

    store = _save(tmp_path / "workflows")
    bad_path = store.runs_dir / "unreadable-\udcff.json"
    real_glob = Path.glob
    real_open = store_module.platform_compat.open_file_no_reparse

    def discover(path, pattern):
        yield from real_glob(path, pattern)
        if path == store.runs_dir:
            yield bad_path

    def open_record(path, **kwargs):
        if path == bad_path:
            raise OSError("unreadable record")
        return real_open(path, **kwargs)

    monkeypatch.setattr(Path, "glob", discover)
    monkeypatch.setattr(store_module.platform_compat, "open_file_no_reparse", open_record)
    rows = store.load_all()
    assert len(rows) == 1 and rows[0]["run_id"] == "wf_000001"
