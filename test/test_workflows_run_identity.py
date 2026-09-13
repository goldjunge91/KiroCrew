"""Workflow ID reservation is atomic; a reservation itself grants no scope."""

import asyncio
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from test_workflows_private_execution import SCRIPT, finished
from test_workflows_private_execution import world as _world

from kiro_crew.workflow_memory import WorkflowScope, read_binding, reserve_run_id
from kiro_crew.workflows.service import WorkflowService

world = _world


@pytest.mark.asyncio
@pytest.mark.parametrize("second", ["alice", "bob", "global"])
async def test_simultaneous_services_never_share_run_identity(world, second):
    services = [
        WorkflowService(sessions=world.sessions, context_builder=world.builder, persist=False)
        for _ in range(2)
    ]
    first, other = await asyncio.gather(
        services[0].start(SCRIPT, session_key="dashboard:alice"),
        services[1].start(SCRIPT, session_key=f"dashboard:{second}"),
    )
    assert "run_id" in first, first
    assert "run_id" in other, other
    assert first["run_id"] != other["run_id"]
    runs = await asyncio.gather(finished(services[0], first), finished(services[1], other))
    scopes = await asyncio.gather(*(WorkflowScope.restore(run.run_id) for run in runs))
    assert scopes[0].store == world.stores["alice"]
    assert scopes[1].store == world.stores.get(second, "")
    for run, scope in zip(runs, scopes):
        assert run.result == [[scope.store or "V1"] * 2] + [scope.store or "V1"] * 3


def test_thread_reservation_has_exactly_one_winner():
    import threading

    barrier = threading.Barrier(6, timeout=5)

    def claim(_):
        barrier.wait()
        return reserve_run_id("wf_000001")

    with ThreadPoolExecutor(max_workers=6) as pool:
        assert list(pool.map(claim, range(6))).count(True) == 1
    assert read_binding("wf_000001") is None
    assert not reserve_run_id("wf_000001")


def test_processes_reserve_distinct_ids_without_publishing_authority(tmp_path):
    source_root = Path(__file__).resolve().parents[1] / "src"
    script = (
        "import sys\n"
        "from kiro_crew.workflow_memory import reserve_run_id\n"
        "sys.stdin.readline()\n"
        "for index in range(1, 10):\n"
        " key = f'wf_{index:06d}'\n"
        " if reserve_run_id(key):\n"
        "  print(key, flush=True)\n"
        "  break\n"
    )
    environment = dict(os.environ, PYTHONPATH=str(source_root))
    children = []
    try:
        for _ in range(2):
            children.append(
                subprocess.Popen(
                    [sys.executable, "-c", script],
                    cwd=tmp_path,
                    env=environment,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    encoding="utf-8",
                )
            )
        for child in children:
            child.stdin.write("go\n")
            child.stdin.flush()
            child.stdin.close()
            child.stdin = None
        ids = []
        for child in children:
            out, error = child.communicate(timeout=20)
            assert child.returncode == 0, error
            ids.append(out.strip())
        assert set(ids) == {"wf_000001", "wf_000002"}
        assert all(read_binding(key) is None for key in ids)
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=5)


@pytest.mark.parametrize("unc", [False, True])
def test_windows_resolved_prefix_is_only_a_spelling(unc):
    from pathlib import PureWindowsPath

    from kiro_crew.workflow_memory import _plain_path

    root = r"\\server\share\Crew" if unc else r"C:\Users\Runner\Crew"
    plain = PureWindowsPath(root) / "workflows" / ".reserved" / "digest"
    extended = PureWindowsPath(("\\\\?\\UNC\\" + str(plain)[2:]) if unc else "\\\\?\\" + str(plain))
    assert extended != plain
    assert _plain_path(extended) == plain
    assert _plain_path(extended.parent / "foreign") != plain


def test_reservation_accepts_resolve_prefix_during_creation(monkeypatch):
    from kiro_crew.workflow_memory import binding_path

    reservation = binding_path("wf_prefix").parent.parent / ".reserved"
    real_resolve = Path.resolve

    def resolve(path, *args, **kwargs):
        resolved = real_resolve(path, *args, **kwargs)
        if path.parent == reservation:
            # Only the spelling returned by the OS is synthetic. mkdir,
            # collision detection and protected binding reads remain real.
            return Path("\\\\?\\" + str(resolved))
        return resolved

    monkeypatch.setattr(Path, "resolve", resolve)
    assert reserve_run_id("wf_prefix")
    assert not reserve_run_id("wf_prefix")
    assert read_binding("wf_prefix") is None


@pytest.mark.parametrize("target", ["reservation", "binding", "payload"])
def test_workflow_paths_still_refuse_real_directory_redirect(tmp_path, target):
    from conftest import make_dir_link

    from kiro_crew.workflow_memory import WorkflowMemoryError, binding_path, private_payload_path

    binding = binding_path("wf_redirect")
    path = {
        "reservation": binding.parent.parent / ".reserved",
        "binding": binding.parent,
        "payload": private_payload_path("wf_redirect").parent,
    }[target]
    path.parent.mkdir(parents=True, exist_ok=True)
    outside = tmp_path / "foreign"
    outside.mkdir()
    make_dir_link(path, outside)
    operation = private_payload_path if target == "payload" else reserve_run_id
    with pytest.raises(WorkflowMemoryError, match="redirected"):
        operation("wf_redirect")
    assert not list(outside.iterdir())
