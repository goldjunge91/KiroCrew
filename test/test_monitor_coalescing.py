"""Coalescing and re-assertion in the pure decision engine.

These pin the window the engine grows over successive changes to ONE subject:
the first actionable change wakes at once, a rapid follow-up folds into one wake,
the hard cap fires on its own schedule, a head change resets the window,
re-assertion re-wakes an unresolved change on the re-alert interval, and the
re-alert map is pruned unconditionally. They are the engine-terms counterpart of
``test/test_irq_port_baseline.py``: that file pins ``irq.py``, which this port
does not touch, so these tests are what verify the ported behaviour.

Each test states the behaviour it pins in present tense. The window rides on
``MonitorState``; a decision reads and updates it, and the caller persists the
same state, so no file is held here.
"""

from __future__ import annotations

import pytest

from kiro_crew.monitoring.decision import decide_monitor
from kiro_crew.monitoring.models import (
    DEFAULT_MONITOR_COALESCE_MAX_SECS,
    DEFAULT_MONITOR_COALESCE_SECS,
    DEFAULT_MONITOR_REALERT_SECS,
    MonitorBudgets,
    MonitorDecision,
    MonitorObservation,
    MonitorObservationStatus,
    MonitorState,
    monitor_state_from_dict,
)

_FLOOR = DEFAULT_MONITOR_COALESCE_SECS
_CAP = DEFAULT_MONITOR_COALESCE_MAX_SECS
_REALERT = DEFAULT_MONITOR_REALERT_SECS


def _state(**overrides) -> MonitorState:
    base = dict(
        kind="github_pull_request",
        target="owner/repo/pull/1",
        objective="review_ready",
        created_ts=0.0,
        budgets=MonitorBudgets(max_runtime_secs=10_000_000),
    )
    base.update(overrides)
    return MonitorState(**base)


def _actionable(fingerprint: str, *, head_changed: bool = False) -> MonitorObservation:
    return MonitorObservation(
        fingerprint,
        MonitorObservationStatus.ACTIONABLE,
        head_changed=head_changed,
    )


def _decide(state, obs, *, now):
    return decide_monitor(state, obs, now=now).decision


def test_the_first_actionable_change_wakes_at_once():
    """A new actionable fingerprint wakes on the tick it is seen.

    The probe interval is user-set and typically exceeds the floor, so holding
    the first change adds an interval of latency while grouping nothing. The
    first change wakes now and opens the window that a later change folds into.
    """
    state = _state()
    assert _decide(state, _actionable("red:a"), now=100.0) is MonitorDecision.WAKE_ACTIONABLE
    assert state.coalesce_fingerprint == "red:a"
    assert state.coalesce_opened_at == 100.0


def test_a_rapid_follow_up_change_folds_into_one_wake():
    """A different change arriving inside the floor is held, then fired once.

    This is the flood case the window exists for: a subject changing again while
    the first wake's window is still open does not buy a second immediate wake.
    """
    state = _state()
    assert _decide(state, _actionable("red:a"), now=100.0) is MonitorDecision.WAKE_ACTIONABLE

    # A second, different change well inside the floor is held.
    assert _decide(state, _actionable("red:b"), now=100.0 + _FLOOR * 0.3) is (
        MonitorDecision.RECORD_ONLY
    )

    # Once the window has aged past the floor, the held change fires.
    assert _decide(state, _actionable("red:b"), now=100.0 + _FLOOR * 1.1) is (
        MonitorDecision.WAKE_ACTIONABLE
    )


def test_the_hard_cap_fires_on_its_own_schedule():
    """The cap flushes a held change even when the floor is never reached.

    With a floor set above the cap and a subject that keeps changing, the cap is
    the bound that fires: it is measured from when the window opened and is not
    gated behind the floor.
    """
    state = _state()
    floor = _CAP * 20  # deliberately far above the cap; both legal
    assert (
        decide_monitor(
            state, _actionable("red:a"), now=0.0, coalesce_secs=floor, coalesce_max_secs=_CAP
        ).decision
        is MonitorDecision.WAKE_ACTIONABLE
    )
    # A follow-up inside the (huge) floor but past the cap fires on the cap.
    assert (
        decide_monitor(
            state,
            _actionable("red:b"),
            now=_CAP * 1.1,
            coalesce_secs=floor,
            coalesce_max_secs=_CAP,
        ).decision
        is MonitorDecision.WAKE_ACTIONABLE
    )


def test_a_head_change_opens_a_fresh_window_and_wakes_now():
    """A head change is a new subject state: it wakes at once, not held.

    A follow-up change is folded only within one head. When the head moves the
    window resets, so the first change on the new head wakes immediately the way
    the first change on any subject does.
    """
    state = _state()
    assert _decide(state, _actionable("red:a"), now=0.0) is MonitorDecision.WAKE_ACTIONABLE
    # Same head, rapid follow-up: held.
    assert _decide(state, _actionable("red:b"), now=1.0) is MonitorDecision.RECORD_ONLY
    # Head change: the new head's first change wakes now.
    assert _decide(state, _actionable("red:c", head_changed=True), now=2.0) is (
        MonitorDecision.WAKE_ACTIONABLE
    )
    assert state.coalesce_fingerprint == "red:c"
    assert state.coalesce_opened_at == 2.0


def test_the_same_change_re_wakes_only_on_the_re_alert_interval():
    """An unresolved change re-wakes once its re-alert entry ages out.

    Level-triggered re-assertion: the same fingerprint stays masked inside the
    interval and re-wakes past it, so a persisting condition is re-reported
    rather than told once.
    """
    state = _state()
    assert _decide(state, _actionable("red:a"), now=0.0) is MonitorDecision.WAKE_ACTIONABLE
    # Inside the interval: masked.
    assert _decide(state, _actionable("red:a"), now=_REALERT * 0.5) is MonitorDecision.NO_CHANGE
    # Past the interval: re-asserted.
    assert _decide(state, _actionable("red:a"), now=_REALERT * 1.1) is (
        MonitorDecision.WAKE_ACTIONABLE
    )


def test_a_future_re_alert_timestamp_reads_as_stale():
    """A re-alert time in the future does not suppress a wake forever.

    A clock rollback (or corrupt state) can leave a future timestamp; reading it
    as stale rather than fresh keeps it from silencing the subject permanently.
    """
    state = _state(coalesce_alerted={"red:a": 10_000.0})
    assert _decide(state, _actionable("red:a"), now=100.0) is MonitorDecision.WAKE_ACTIONABLE


def test_the_re_alert_map_is_pruned_unconditionally():
    """Entries past the re-alert interval are dropped on the next decision.

    The map lives on a durable per-loop record, so an entry past its interval
    suppresses nothing and is dropped rather than kept, bounding growth across
    restarts.
    """
    state = _state(coalesce_alerted={"old:1": 0.0, "old:2": 1.0})
    # A fresh unrelated change past the interval prunes the stale entries.
    _decide(state, _actionable("red:new"), now=_REALERT + 100.0)
    assert "old:1" not in state.coalesce_alerted
    assert "old:2" not in state.coalesce_alerted


def test_a_record_without_the_window_fields_loads_as_an_unopened_window():
    """A persisted record written before the window fields loads as no window.

    The loader takes the dataclass default for any absent field, so an old
    record shows an empty ``coalesce_fingerprint`` -- not a window opened at time
    zero -- and its first actionable change wakes at once like any first change.
    """
    raw = {
        "kind": "github_pull_request",
        "target": "owner/repo/pull/1",
        "objective": "review_ready",
        "created_ts": 0.0,
    }
    state = monitor_state_from_dict(raw)
    assert state.coalesce_fingerprint == ""
    assert state.coalesce_opened_at == 0.0
    assert state.coalesce_alerted == {}
    assert _decide(state, _actionable("red:a"), now=100.0) is MonitorDecision.WAKE_ACTIONABLE


def test_a_non_string_coalesce_fingerprint_is_refused():
    """A wrong-typed window fingerprint is rejected at construction.

    Absent is legal and loads as an unopened window; present-but-wrong is not,
    because a decision reads the field and a malformed value would raise deep in
    a tick rather than at the boundary.
    """
    with pytest.raises(ValueError, match="coalesce_fingerprint"):
        _state(coalesce_fingerprint=["not", "a", "string"])


def test_a_non_numeric_coalesce_opened_at_is_refused():
    """A wrong-typed window open time is rejected at construction."""
    with pytest.raises(ValueError, match="coalesce_opened_at"):
        _state(coalesce_opened_at="not-a-number")


def test_a_non_dict_coalesce_alerted_is_refused():
    """A wrong-typed re-alert map is rejected at construction.

    A persisted ``coalesce_alerted: []`` is the shape that raised inside a tick
    when the map was iterated; refusing it at the boundary turns that into a
    clean rejection the loader quarantines.
    """
    with pytest.raises(ValueError, match="coalesce_alerted"):
        _state(coalesce_alerted=[])
