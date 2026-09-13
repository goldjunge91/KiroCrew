"""The gateway daemon's own liveness self-report: where it lives, how to read it.

``gatewayd._zombie_diagnostic`` appends one ``probe`` record every
:data:`ZOMBIE_PROBE_INTERVAL_SECS` to a JSONL side-channel under the data home
(``logs/gatewayd_zombie_diagnostic.jsonl``), carrying the daemon's ``pid``, its
``server.is_serving()`` reading, its task count and its connections in flight.
That record is the daemon's word on whether it is alive, written from INSIDE its
event loop -- so it is the one piece of evidence a supervisor can consult when
the daemon stops answering a ping in time.

The supervisor needs exactly that evidence. ``GatewayManager``'s watchdog pings
the daemon on a short deadline and, after a run of misses, declares it a zombie
and SIGKILLs it. Under a wide fan-out (a thousand sessions, a hundred-plus
connections in flight) the daemon's loop gets slow enough that pings time out
while ``is_serving`` stays true, and the watchdog then kills a daemon that is
merely busy -- ten times in thirty-five minutes in the incident this guards
against, each kill severing every session's ``kirocrew-core`` transport. A fresh self-report from the very
pid the watchdog is about to kill is what distinguishes "overloaded" from
"dead".

This is a dependency-free leaf module for the same reason as
:mod:`kiro_crew.mcp_gateway.shutdown_budget`: ``gatewayd`` imports ``manager``,
so ``manager`` cannot import ``gatewayd`` to reach its path resolver, and two
independent spellings of the path would drift into a reader that looks where the
writer never writes.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Optional

from kiro_crew.config.paths import config_dir

#: Interval between the daemon's self-report snapshots. A 30 s sample rate
#: catches the ~90 s window between an accept-loop death and the watchdog kill
#: without generating excessive log volume in the healthy case.
ZOMBIE_PROBE_INTERVAL_SECS = 30.0

#: Leaf filename of the JSONL side-channel under ``<data home>/logs/``.
SELF_REPORT_FILENAME = "gatewayd_zombie_diagnostic.jsonl"

#: How much of the file's tail :func:`read_last_probe` reads. One probe record
#: is a few hundred bytes; a ``zombie_detected`` dump with task stacks is tens of
#: kilobytes. 64 KiB always spans at least the last complete record without
#: ever reading a days-old file whole on the supervisor's event loop.
SELF_REPORT_TAIL_BYTES = 64 * 1024


def zombie_diagnostic_path(base: Optional[Path] = None) -> Path:
    """Return the JSONL file that receives the daemon's self-reports.

    Lives next to the soak/gatewayd logs under
    ``$KIROCREW_HOME/logs/gatewayd_zombie_diagnostic.jsonl`` so a single
    ``tail -f`` follows both heartbeat (gatewayd.log) and any detected zombie
    state. ``base`` defaults to the data home (``config_dir`` already honours
    ``KIROCREW_HOME``); ``gatewayd`` passes its own resolver's answer so its
    module-level seam keeps working for tests.
    """
    return (base if base is not None else config_dir()) / "logs" / SELF_REPORT_FILENAME


def read_last_probe(
    path: Path, *, tail_bytes: int = SELF_REPORT_TAIL_BYTES
) -> Optional[dict[str, Any]]:
    """Return the LAST COMPLETE record in ``path``, or ``None``.

    Reads at most ``tail_bytes`` from the end of the file, so the cost is
    bounded whatever the file has grown to. Only a line terminated by ``\\n``
    counts as complete: the writer appends whole lines, so an unterminated tail
    is a record still being written (or torn by a crash) and is dropped rather
    than parsed. The record before it is returned instead, so a reader racing
    the writer sees the previous snapshot, never a partial one.

    ``None`` on a missing or unreadable file, an empty file, and a last complete
    line that is not a JSON object. Never raises: the caller treats ``None`` as
    "no evidence", which is the fail-closed answer (the daemon does not get the
    benefit of a report it did not write).
    """
    try:
        with path.open("rb") as fh:
            fh.seek(0, os.SEEK_END)
            size = fh.tell()
            start = max(0, size - max(tail_bytes, 0))
            fh.seek(start)
            data = fh.read(size - start)
    except OSError:
        return None
    # Everything after the final newline is unterminated -- either b"" (the
    # file ends cleanly) or a partial line -- so it is never a candidate.
    complete = data.split(b"\n")[:-1]
    for raw in reversed(complete):
        line = raw.strip()
        if not line:
            continue
        try:
            record = json.loads(line.decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            return None
        return record if isinstance(record, dict) else None
    return None
