"""Pure decision policy for structured monitors."""

from __future__ import annotations

from kiro_crew.monitoring.models import (
    DEFAULT_MONITOR_COALESCE_MAX_SECS,
    DEFAULT_MONITOR_COALESCE_SECS,
    DEFAULT_MONITOR_REALERT_SECS,
    MONITOR_STATE_VERSION,
    MONITOR_STOP_AGENT_TURN_BUDGET,
    MONITOR_STOP_PROVIDER_ERROR_BUDGET,
    MONITOR_STOP_RUNTIME_BUDGET,
    MONITOR_STOP_TOKEN_BUDGET,
    MonitorBudgets,
    MonitorDecision,
    MonitorObservation,
    MonitorObservationStatus,
    MonitorOutcome,
    MonitorState,
    MonitorVerdict,
    ProviderErrorKind,
)

_RETRYABLE_PROVIDER_ERRORS = frozenset(
    {ProviderErrorKind.TRANSIENT, ProviderErrorKind.RATE_LIMITED}
)


def decide_monitor(
    state: MonitorState,
    observation: MonitorObservation,
    *,
    now: float,
    coalesce_secs: float = DEFAULT_MONITOR_COALESCE_SECS,
    coalesce_max_secs: float = DEFAULT_MONITOR_COALESCE_MAX_SECS,
    realert_secs: float = DEFAULT_MONITOR_REALERT_SECS,
) -> MonitorVerdict:
    """Return the only controller effect permitted for an observation.

    The effect is returned inside a :class:`MonitorVerdict` so it arrives with
    the observations it was rendered against. Every path here judges exactly one
    observation, so the verdict names that one; a probe reporting several
    independent conditions fills the same tuple with several entries without
    changing this signature or any caller.

    A structured monitor watches one subject, so a wake-worthy change coalesces
    over TIME rather than across simultaneous signals: successive changes to the
    subject share one window, aged from when the window opened. This updates the
    window fields on *state* as part of deciding, and the caller persists that
    same state, so the window rides on the state object rather than on any file.
    """
    return MonitorVerdict(
        decision=_decide_effect(
            state,
            observation,
            now=now,
            coalesce_secs=coalesce_secs,
            coalesce_max_secs=coalesce_max_secs,
            realert_secs=realert_secs,
        ),
        entries=(observation,),
    )


def _decide_effect(
    state: MonitorState,
    observation: MonitorObservation,
    *,
    now: float,
    coalesce_secs: float,
    coalesce_max_secs: float,
    realert_secs: float,
) -> MonitorDecision:
    """Select the effect alone.

    Budget checks lead because a spent bound must never buy one additional
    unattended turn. Provider failures are classified without a model. An
    actionable change passes through the coalescing window before it may wake
    the owning session.
    """
    if state.version != MONITOR_STATE_VERSION:
        return MonitorDecision.STOP_BLOCKED
    terminal = terminal_decision_for_outcome(state.outcome)
    if terminal is not None:
        return terminal
    if monitor_budget_reason(state, now=now):
        return MonitorDecision.STOP_BUDGET
    if observation.status is MonitorObservationStatus.PROVIDER_ERROR:
        return _provider_error_decision(state, observation, state.budgets)
    if observation.status is MonitorObservationStatus.ACTIONABLE:
        if observation.fingerprint == state.last_wake_fingerprint:
            if observation.supplemental_provider_error is not None:
                return _supplemental_provider_error_decision(state, state.budgets)
            return MonitorDecision.NO_CHANGE
        return _coalesce_actionable(
            state,
            observation,
            now=now,
            coalesce_secs=coalesce_secs,
            coalesce_max_secs=coalesce_max_secs,
            realert_secs=realert_secs,
        )
    # Any non-actionable outcome settles the subject, so no window stays open.
    _close_window(state, now=now, realert_secs=realert_secs)
    if observation.supplemental_provider_error is not None:
        return _supplemental_provider_error_decision(state, state.budgets)
    if observation.status is MonitorObservationStatus.SUCCESS:
        if observation.head_changed:
            return MonitorDecision.WAKE_ACTIONABLE
        return MonitorDecision.STOP_SUCCESS
    if observation.fingerprint == state.last_fingerprint:
        return MonitorDecision.NO_CHANGE
    if observation.status is MonitorObservationStatus.PENDING:
        return MonitorDecision.RECORD_ONLY
    return MonitorDecision.STOP_BLOCKED


def _coalesce_actionable(
    state: MonitorState,
    observation: MonitorObservation,
    *,
    now: float,
    coalesce_secs: float,
    coalesce_max_secs: float,
    realert_secs: float,
) -> MonitorDecision:
    """Decide one actionable change through the coalescing window.

    The first actionable change wakes immediately and opens the window. A
    structured monitor's probe interval is user-set (15 to 86400 seconds,
    default 300) and typically exceeds the floor, so holding the first change
    for the floor adds a probe interval of latency at the default and groups
    nothing -- two probes are already further apart than the floor. The floor
    earns its keep only when the interval is materially shorter than it, so it
    holds the SUBSEQUENT change: a second, different actionable fingerprint
    arriving while the window is still open waits out the floor, which folds a
    burst of rapid changes into one wake. The cap fires the held change on its
    own schedule regardless of the floor. A change already inside its re-alert
    interval stays masked; a head change opens a fresh window because a new
    commit is a different subject state.
    """
    _prune_alerted(state, now=now, realert_secs=realert_secs)
    fingerprint = observation.fingerprint

    if not _realert_ready(state, fingerprint, now=now, realert_secs=realert_secs):
        return MonitorDecision.NO_CHANGE

    # Past the re-alert mask. A fingerprint that already owns the open window is
    # an unresolved change re-asserting on its interval: re-wake and restamp so
    # it is re-reported rather than told once.
    if state.coalesce_fingerprint == fingerprint and not observation.head_changed:
        state.coalesce_alerted[fingerprint] = now
        state.coalesce_opened_at = now
        return MonitorDecision.WAKE_ACTIONABLE

    window_open = bool(state.coalesce_fingerprint) and not observation.head_changed
    if not window_open:
        # First actionable change (or the first after a head change): wake now
        # and open the window so a rapid follow-up change is what the floor holds.
        state.coalesce_fingerprint = fingerprint
        state.coalesce_opened_at = now
        state.coalesce_alerted[fingerprint] = now
        return MonitorDecision.WAKE_ACTIONABLE

    age = now - state.coalesce_opened_at
    # The cap is an absolute wall on its own, never gated behind the floor: a
    # floor set above the cap plus a subject that keeps changing must still flush
    # at the cap rather than wait for a floor it cannot reach.
    if age >= coalesce_max_secs or age >= coalesce_secs:
        state.coalesce_alerted[fingerprint] = now
        state.coalesce_fingerprint = fingerprint
        state.coalesce_opened_at = now
        return MonitorDecision.WAKE_ACTIONABLE
    # A follow-up change still inside the floor: record it and keep waiting.
    return MonitorDecision.RECORD_ONLY


def _realert_ready(
    state: MonitorState,
    fingerprint: str,
    *,
    now: float,
    realert_secs: float,
) -> bool:
    """Whether *fingerprint* may wake again, given the re-alert interval.

    A future timestamp reads as stale rather than as permanent suppression, so a
    clock rollback cannot silence the subject forever.
    """
    last = state.coalesce_alerted.get(fingerprint)
    if not isinstance(last, (int, float)):
        return True
    elapsed = now - float(last)
    return not (0 <= elapsed < realert_secs)


def _close_window(state: MonitorState, *, now: float, realert_secs: float) -> None:
    """Close any open window and prune the re-alert map.

    The prune is unconditional because the re-alert map lives on a durable
    per-loop record: an entry past its interval suppresses nothing, so dropping
    it frees state that otherwise grows across every restart.
    """
    state.coalesce_fingerprint = ""
    state.coalesce_opened_at = 0.0
    _prune_alerted(state, now=now, realert_secs=realert_secs)


def _prune_alerted(state: MonitorState, *, now: float, realert_secs: float) -> None:
    """Drop re-alert entries older than the interval, or with an unusable time."""
    stale = [
        fingerprint
        for fingerprint, last in state.coalesce_alerted.items()
        if not isinstance(last, (int, float)) or now - float(last) >= realert_secs
    ]
    for fingerprint in stale:
        state.coalesce_alerted.pop(fingerprint, None)


def terminal_decision_for_outcome(outcome: MonitorOutcome | None) -> MonitorDecision | None:
    """Return the decision a recorded terminal outcome forces, or None if live.

    Both :func:`decide_monitor` and the persistence-only shadow path
    short-circuit here so a stopped monitor is never re-probed. This is NOT the
    verdict the delivery controller reports: ``autonudge``'s
    ``apply_monitor_probe`` refuses a monitor with a recorded outcome before
    :func:`decide_monitor` runs, flattening every terminal outcome to
    ``STOP_BLOCKED``.
    """
    if outcome is MonitorOutcome.SUCCESS:
        return MonitorDecision.STOP_SUCCESS
    if outcome is MonitorOutcome.BUDGET:
        return MonitorDecision.STOP_BUDGET
    if outcome is not None:
        return MonitorDecision.STOP_BLOCKED
    return None


def monitor_budget_reason(state: MonitorState, *, now: float) -> str:
    """Return the first exhausted hard bound in stable policy order."""
    budgets = state.budgets
    if now - state.created_ts >= budgets.max_runtime_secs:
        return MONITOR_STOP_RUNTIME_BUDGET
    if state.agent_turns >= budgets.max_agent_turns:
        return MONITOR_STOP_AGENT_TURN_BUDGET
    if state.total_tokens >= budgets.max_tokens:
        return MONITOR_STOP_TOKEN_BUDGET
    if state.provider_error_count >= budgets.max_provider_errors:
        return MONITOR_STOP_PROVIDER_ERROR_BUDGET
    return ""


def _provider_error_decision(
    state: MonitorState,
    observation: MonitorObservation,
    budgets: MonitorBudgets,
) -> MonitorDecision:
    error = observation.provider_error
    if error not in _RETRYABLE_PROVIDER_ERRORS:
        return MonitorDecision.STOP_BLOCKED
    if state.consecutive_provider_errors + 1 >= budgets.max_provider_errors:
        return MonitorDecision.STOP_BLOCKED
    return MonitorDecision.RETRY_PROVIDER


def _supplemental_provider_error_decision(
    state: MonitorState,
    budgets: MonitorBudgets,
) -> MonitorDecision:
    """Retry incomplete secondary evidence before retiring the readable target."""
    if state.consecutive_provider_errors + 1 >= budgets.max_provider_errors:
        return MonitorDecision.STOP_BLOCKED
    return MonitorDecision.RETRY_PROVIDER
