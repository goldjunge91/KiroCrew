"""The taskq state machine: one table, every edge validated, terminals never regress."""

from __future__ import annotations

import pytest

from kiro_crew.taskq import model
from kiro_crew.taskq.model import (
    ACTIVE,
    CANCELLED,
    CLAIMABLE,
    STATES,
    TERMINAL,
    TRANSITIONS,
    InvalidTransition,
    TaskRecord,
    check_transition,
    recovery_backoff_secs,
)


def test_fifteen_states_and_every_state_has_a_row() -> None:
    # 13 (RFC §3.3) + the addendum's ``waiting_dependency`` and ``waiting_input``.
    assert len(STATES) == 15
    assert {model.WAITING_DEPENDENCY, model.WAITING_INPUT} <= STATES
    assert model.SUCCEEDED == model.DONE
    assert set(TRANSITIONS) == STATES
    for state, targets in TRANSITIONS.items():
        assert targets <= STATES, state


def test_terminal_states_have_no_exits_except_unknown_side_effect_resolution() -> None:
    for state in TERMINAL - {model.UNKNOWN_SIDE_EFFECT}:
        assert TRANSITIONS[state] == frozenset(), state
    assert TRANSITIONS[model.UNKNOWN_SIDE_EFFECT] == {model.DONE, model.FAILED}


def test_cancelled_beats_every_non_terminal_state() -> None:
    for state in STATES - TERMINAL:
        check_transition(state, CANCELLED)


def test_failed_reachable_from_every_non_terminal_state() -> None:
    for state in STATES - TERMINAL:
        check_transition(state, model.FAILED)


def test_done_and_unknown_side_effect_reachable_only_from_owned_states() -> None:
    for state in ACTIVE:
        check_transition(state, model.DONE)
        check_transition(state, model.UNKNOWN_SIDE_EFFECT)
    for state in (model.QUEUED, model.RETRY_WAIT, model.WAITING_INFRA):
        with pytest.raises(InvalidTransition):
            check_transition(state, model.DONE)


@pytest.mark.parametrize(
    "old,new",
    [
        (model.DONE, model.RUNNING),
        (model.CANCELLED, model.QUEUED),
        (model.FAILED, model.DONE),
        (model.QUEUED, model.RUNNING),  # must go through admitted/starting
        (model.QUEUED, model.DONE),
        (model.RETRY_WAIT, model.RUNNING),
        (model.RETRY_WAIT, model.DONE),
        (model.WAITING_INFRA, model.DONE),
    ],
)
def test_forbidden_edges_raise(old: str, new: str) -> None:
    with pytest.raises(InvalidTransition):
        check_transition(old, new)


def test_unknown_state_names_raise() -> None:
    with pytest.raises(InvalidTransition):
        check_transition("queued", "sleeping")
    with pytest.raises(InvalidTransition):
        check_transition("bogus", "queued")


def test_happy_path_chain_is_allowed() -> None:
    chain = [model.QUEUED, model.ADMITTED, model.STARTING, model.RUNNING, model.DONE]
    for old, new in zip(chain, chain[1:]):
        check_transition(old, new)


def test_recovery_chain_is_allowed() -> None:
    check_transition(model.RUNNING, model.RECOVERING)
    check_transition(model.RECOVERING, model.ADMITTED)
    check_transition(model.STARTING, model.RETRY_WAIT)
    check_transition(model.RETRY_WAIT, model.ADMITTED)
    check_transition(model.ADMITTED, model.WAITING_INFRA)
    check_transition(model.WAITING_INFRA, model.RETRY_WAIT)


def test_claimable_and_active_partition_the_live_states() -> None:
    assert CLAIMABLE & ACTIVE == {model.RECOVERING}
    assert CLAIMABLE | ACTIVE | TERMINAL | {model.WAITING_INFRA} == STATES
    # recovering is both re-claimable (dead owner) and owned (live owner);
    # the lease decides which, so it appears in both sets.
    assert model.RECOVERING in CLAIMABLE and model.RECOVERING in ACTIVE


def test_record_validates_kind_state_and_side_effect_class() -> None:
    with pytest.raises(ValueError):
        TaskRecord(id="", kind="subagent")
    with pytest.raises(ValueError):
        TaskRecord(id="x", kind="nope")
    with pytest.raises(ValueError):
        TaskRecord(id="x", kind="subagent", state="sleeping")
    with pytest.raises(ValueError):
        TaskRecord(id="x", kind="subagent", side_effect_class="maybe")
    rec = TaskRecord(id="x", kind="subagent", parent_id="p")
    assert rec.root_id == "p"
    assert TaskRecord(id="y", kind="subagent").root_id == "y"


def test_record_row_round_trip_preserves_every_column() -> None:
    rec = TaskRecord(
        id="r1",
        kind="subagent",
        session_key="dash:1",
        parent_id="p1",
        params={"task": "do", "batch_id": "b"},
        scope_ref={"memory_store": "crew-a"},
        state=model.RETRY_WAIT,
        attempts=2,
        next_run_at=123.5,
        lease_owner="inc",
        lease_expires_at=200.0,
        generation=3,
        progress={"step": 4},
        result_ref="/tmp/r",
        deadline_at=999.0,
        idempotency_key="k",
        side_effect_class=model.SIDE_EFFECT_NONE,
        created_at=1.0,
        updated_at=2.0,
    )
    row = dict(zip(TaskRecord.COLUMNS, rec.to_row()))
    back = TaskRecord.from_row(row)
    assert back == rec
    assert "params" not in rec.public() and rec.public()["terminal"] is False


def test_recovery_backoff_is_exponential_and_capped() -> None:
    assert recovery_backoff_secs(0) == 2.0
    assert recovery_backoff_secs(1) == 4.0
    assert recovery_backoff_secs(3) == 16.0
    assert recovery_backoff_secs(10) == 120.0
    assert recovery_backoff_secs(-5) == 2.0
