"""The gateway publishes the adaptive controller's caps, the degrade reason and
the manager's dependency coordinator at start, and withdraws them at shutdown.

``session_health`` renders ``effective_caps`` and ``degrade_reason`` from the
sources registered here; ``taskq.dependency.current_coordinator`` is how a
caller without a task row reads a scope's shared ``retry_at``. Without this
wiring both surfaces are empty in the running gateway even though every
producer exists.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from kiro_crew.dashboard import session_health
from kiro_crew.slack.gateway import GatewayOrchestrator
from kiro_crew.taskq import dependency as taskq_dependency


class _Controller:
    def __init__(self, **state):
        self._state = {
            "enabled": True,
            "spawn_gate_capacity": 4,
            "applied_gate_cap": 4,
            "gate_pending": None,
            "gate_floor": 1,
            "gate_ceiling": 8,
            "effective_exec_cap": 6,
            "exec_ceiling": 10,
            "paused": False,
            "probing": False,
            "last": None,
        }
        self._state.update(state)

    def state(self) -> dict:
        return dict(self._state)


@pytest.fixture
def clean_sources():
    monitor = session_health.default_monitor()
    monitor.clear_sources()
    taskq_dependency.register_coordinator(None)
    yield monitor
    monitor.clear_sources()
    taskq_dependency.register_coordinator(None)


def _orch(coordinator=None) -> GatewayOrchestrator:
    orch = GatewayOrchestrator.__new__(GatewayOrchestrator)
    orch.subagent_mgr = SimpleNamespace(dependency_coordinator=lambda: coordinator)
    return orch


def _caps(monitor) -> dict:
    snapshot = session_health.HealthSnapshot()
    return monitor._effective_caps(snapshot)


def test_caps_and_reason_come_from_the_controller(clean_sources) -> None:
    monitor = clean_sources
    orch = _orch()
    orch._wire_overload_health(_Controller(last={"action": "decrease"}))
    caps = _caps(monitor)
    assert caps["spawn_gate"]["effective"] == 4
    assert caps["spawn_gate"]["ceiling"] == 8
    assert caps["subagents"]["adaptive"] == 6
    assert monitor._degrade_reason() == "adaptive_decrease"


@pytest.mark.parametrize(
    "state,reason",
    [
        ({"paused": True}, "adaptive_pause"),
        ({"probing": True}, "adaptive_probe"),
        ({"last": {"action": "hold"}}, None),
        ({"last": {"action": "increase"}}, None),
    ],
)
def test_degrade_reason_is_a_closed_set_token(clean_sources, state, reason) -> None:
    orch = _orch()
    orch._wire_overload_health(_Controller(**state))
    assert clean_sources._degrade_reason() == reason


def test_disabled_controller_publishes_no_caps(clean_sources) -> None:
    orch = _orch()
    orch._wire_overload_health(_Controller(enabled=False))
    assert "spawn_gate" not in _caps(clean_sources)
    assert clean_sources._degrade_reason() is None


def test_coordinator_is_registered_and_withdrawn(clean_sources) -> None:
    coordinator = object()
    orch = _orch(coordinator)
    orch._wire_overload_health(_Controller())
    assert taskq_dependency.current_coordinator() is coordinator
    orch._unwire_overload_health()
    assert taskq_dependency.current_coordinator() is None
    assert _caps(clean_sources) == {}
    assert clean_sources._degrade_reason() is None


def test_manager_without_a_coordinator_leaves_the_handle_empty(clean_sources) -> None:
    orch = GatewayOrchestrator.__new__(GatewayOrchestrator)
    orch.subagent_mgr = SimpleNamespace()
    orch._wire_overload_health(_Controller())
    assert taskq_dependency.current_coordinator() is None
