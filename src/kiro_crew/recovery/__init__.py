"""Bounded, layered automatic recovery shared by every layer that retries.

The stub reconnect, the backend respawn, the ACP runtime rebuild, the gatewayd
supervisor and the task re-dispatch would otherwise each carry their own backoff literals,
so under one incident each layer retried on its own schedule and the retries
multiplied. :mod:`kiro_crew.recovery.policy` is the single owner of that
schedule; :mod:`kiro_crew.recovery.ladder` is the order the layers are tried
in and the rule for moving up a layer. Both are pure (no I/O, no asyncio, no
config import) so any layer -- including the MCP stub, which cannot afford the
config import chain -- can depend on them.

See ``docs/request-for-change/rfc-overload-resilience.md`` §7 and
``docs/system-specs/modules/session.md`` § Recovery ladder.
"""

from __future__ import annotations

from kiro_crew.recovery.ladder import (
    ACTION_ESCALATE,
    ACTION_GIVE_UP,
    ACTION_NOTIFY,
    ACTION_RETRY,
    CLASS_CAPACITY,
    CLASS_RECOVERABLE_INFRA,
    L1_TOOL_CALL,
    L2_BACKEND,
    L3_ACP_RUNTIME,
    L4_GATEWAYD,
    L5_GATEWAY,
    LADDER,
    LAYERS,
    InfraError,
    RecoveryDecision,
    RecoveryLadder,
    classify_infra_error,
    default_ladder,
    record_restart,
)
from kiro_crew.recovery.policy import (
    DEFAULT_BACKOFF_BASE_SECS,
    DEFAULT_BACKOFF_MAX_SECS,
    LayerPolicy,
    RecoveryPolicy,
    RecoveryTracker,
)

__all__ = [
    "ACTION_ESCALATE",
    "ACTION_GIVE_UP",
    "ACTION_NOTIFY",
    "ACTION_RETRY",
    "CLASS_CAPACITY",
    "CLASS_RECOVERABLE_INFRA",
    "DEFAULT_BACKOFF_BASE_SECS",
    "DEFAULT_BACKOFF_MAX_SECS",
    "L1_TOOL_CALL",
    "L2_BACKEND",
    "L3_ACP_RUNTIME",
    "L4_GATEWAYD",
    "L5_GATEWAY",
    "LADDER",
    "LAYERS",
    "InfraError",
    "LayerPolicy",
    "RecoveryDecision",
    "RecoveryLadder",
    "RecoveryPolicy",
    "RecoveryTracker",
    "classify_infra_error",
    "default_ladder",
    "record_restart",
]
