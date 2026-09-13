"""Smoke run of the overload experiment driver at N=100 with every fault knob.

Drives ``scripts/experiments/overload_2000.py`` in-process (real admission,
real store, virtual clock; no kiro-cli, no sockets) and checks the invariants
the report leans on: nothing lost, nothing dispatched twice, in-flight within
the cap, the window bounded, every row terminal. Wall time is seconds; the
whole file must stay under a minute at ``-n 0``.
"""

from __future__ import annotations

import importlib.util
import json
import sys
import time
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]
_DRIVER = _REPO_ROOT / "scripts" / "experiments" / "overload_2000.py"

pytestmark = pytest.mark.usefixtures("healthy_host_memory")

N = 100


def _driver():
    spec = importlib.util.spec_from_file_location("overload_2000_smoke", _DRIVER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules.setdefault("overload_2000_smoke", module)
    spec.loader.exec_module(module)
    return module


async def _run(scenario: str, *, faults=None, user_max: int | None = None):
    drv = _driver()
    if faults is None:
        faults = drv.faults_for(scenario, N)
    if user_max is None:
        user_max = 2 if scenario == "tree" else 10
    t0 = time.monotonic()
    h = drv.Harness(scenario=scenario, n=N, faults=faults, user_max=user_max, seed=11)
    try:
        await h.drive(max_virtual_secs=2 * 3600.0)
    finally:
        h.close()
    wall = time.monotonic() - t0
    assert wall < 30.0, f"{scenario} took {wall:.1f}s of wall time at N={N}"
    return h.metrics


def _common(m) -> None:
    assert m.accepted >= N
    assert m.lost == 0
    assert m.remaining == 0
    assert m.duplicate_dispatches == 0
    assert m.max_in_flight <= m.user_max + 1
    assert m.peak_window <= 64
    assert m.invariants["host_procs_bounded"]
    assert m.manual_interventions == 0
    json.dumps(m.as_dict())  # the report writer must be able to serialise it


@pytest.mark.asyncio
async def test_baseline_two_sessions_and_system_lane_all_complete():
    m = await _run("baseline")
    _common(m)
    assert m.completed == N and m.failed == 0
    assert m.max_in_flight == 10
    # every lane got its first grant early even though sessions were submitted in blocks
    assert set(m.fairness_first_grant_index) == set(m.per_lane_accepted)
    assert max(m.fairness_first_grant_index.values()) < 40


@pytest.mark.asyncio
async def test_controller_decreases_under_injected_timeouts_and_recovers():
    m = await _run("controller")
    _common(m)
    caps = [r["exec_cap"] for r in m.effective_cap_timeline]
    assert caps[0] == 10 and min(caps) < 10
    assert m.controller_counts["decrease"] >= 1
    assert m.controller_counts["increase"] >= 1
    assert m.failed >= 1  # the injected start timeouts end attributable, never as done


@pytest.mark.asyncio
async def test_controller_raw_reproduces_the_pinned_cap_defect_d1():
    m = await _run("controller_raw")
    _common(m)
    caps = [r["exec_cap"] for r in m.effective_cap_timeline]
    assert min(caps) < 10
    # D1: with the lifetime failure counter the exec cap never earns an
    # increase once it has been cut (the one counted increase is the gate
    # track's, before the first failure landed)
    low = caps.index(min(caps))
    assert all(c == min(caps) for c in caps[low:])
    assert caps[-1] == min(caps)


@pytest.mark.asyncio
async def test_restart_mid_run_reconciles_and_adopts_a_late_start():
    m = await _run("restart")
    _common(m)
    assert m.restarts == 1
    assert m.restart["queued_survived"] and not m.restart["terminal_regressed"]
    assert m.restart["in_flight_at_crash"] == m.unknown_side_effect  # class unknown: never re-run
    assert m.completed + m.unknown_side_effect + m.failed == m.accepted
    assert m.start_collector["outcome"] == "adopted"
    assert m.start_collector["gate_releases"] == 1


@pytest.mark.asyncio
async def test_outage_parks_runs_on_one_scope_and_nothing_is_severed():
    drv = _driver()
    m = await _run("outage", faults=drv.Faults(outage=(5.0, 20 * 60.0)))
    _common(m)
    assert m.failed == 0 and m.completed == N
    assert m.coordinator_scopes_seen == 1
    assert m.waits_by_state.get("waiting_dependency", 0) >= 1
    assert m.recovery_attempts_by_layer.get("L1_tool_call", 0) >= 1
    assert m.outage_recovery_secs is not None and m.outage_recovery_secs < 60.0
    assert m.host_budget_peaks["procs"] <= m.user_max


@pytest.mark.asyncio
async def test_throttle_storm_from_three_sessions_is_one_schedule():
    m = await _run("throttle")
    _common(m)
    assert m.failed == 0 and m.completed == N
    assert "provider:bedrock" in m.coordinator_scopes
    # parked runs keep their residency, so the host budget (16 procs) is the
    # next bound the fresh starts hit: at most ONE more scope, the gateway's
    assert set(m.coordinator_scopes) <= {"provider:bedrock", "mcp_gateway:capacity"}
    assert m.host_budget_peaks["procs"] <= 16
    assert len(m.throttle_reports_by_session) >= 2
    # a provider 429 is never itself a host signal: every decrease the
    # controller took names REAL host pressure (parked runs keep their
    # residency, so the 16-proc budget fills) -- never the throttle
    for row in m.effective_cap_timeline:
        if row["reason"].startswith("decrease"):
            assert "procs" in row["reason"] and "provider" not in row["reason"], row


@pytest.mark.asyncio
async def test_tree_at_cap_two_with_child_reserve_completes():
    m = await _run("tree")
    _common(m)
    assert m.tree["roots_done"] == m.tree["roots"]
    assert m.tree["children_done"] == m.tree["children"]
    assert m.tree["max_depth"] == 2
    assert m.max_in_flight <= 2


def test_report_writer_renders_every_scenario_row(tmp_path):
    drv = _driver()
    m = drv.Metrics(scenario="x", n=1, accepted=1, completed=1)
    m.effective_cap_timeline = [
        {
            "t": 0,
            "exec_cap": 4,
            "gate_cap": 4,
            "running": 0,
            "queued_window": 0,
            "procs": 0,
            "reason": "",
        }
    ]
    m.invariants = {"ok": True}
    text = drv.render_markdown([m], machine={"platform": "test"})
    assert "| x |" in text and "Invariants: ok=OK" in text
