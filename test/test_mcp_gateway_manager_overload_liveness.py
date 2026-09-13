"""The watchdog must not kill a daemon that is merely overloaded.

A ~1000-session fan-out put 100-185 connections in flight on gatewayd. Its event loop got slow, the watchdog's 2s pings timed out three times
in a row, and ``GatewayManager`` declared a zombie and SIGKILLed the daemon --
ten times in thirty-five minutes -- while the daemon's own self-report
(``logs/gatewayd_zombie_diagnostic.jsonl``) said ``is_serving: true``
throughout. Each kill severed every session's kirocrew-core MCP transport.

This module pins the gate that closes it: at the failure threshold the watchdog
reads the daemon's LAST self-report before deciding. A fresh ``probe`` record
from the pid it owns, saying it is serving, means alive-but-overloaded -- keep
it, warn once, count it, reset the misses. Anything less is no evidence of life
and the existing zombie path runs: a stale record, another pid, a
``zombie_detected`` tag, ``is_serving`` false, a missing or corrupt file.

Also pinned: the read is a bounded tail that never parses a torn last line, and
the watchdog's ping deadline is the wider ``_LIVENESS_PING_TIMEOUT_SECS`` while
spawn/adoption keep ``_PING_TIMEOUT_SECS``.

Everything here is deterministic: the clock is injected, no socket is opened,
no process is spawned, and the only sleep is the patched-to-zero probe interval.
"""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from kiro_crew.mcp_gateway import manager as mgr
from kiro_crew.mcp_gateway import self_report
from kiro_crew.metrics import events as ev

_NOW = 1_800_000_000.0
_OUR_PID = 10795


def _manager(tmp_path: Path) -> mgr.GatewayManager:
    m = mgr.GatewayManager(
        mgr.GatewaySpec(
            socket_path=tmp_path / "gw.sock",
            mcp_target_env={"KIROCREW_MCP_TARGET_CORE": "run core"},
        )
    )
    proc = MagicMock()
    proc.pid = _OUR_PID
    proc.returncode = None
    m._process = proc
    return m


def _probe(**overrides: Any) -> dict[str, Any]:
    rec: dict[str, Any] = {
        "ts_iso": "2027-01-15T08:00:00Z",
        "ts_epoch": _NOW - 20.0,
        "pid": _OUR_PID,
        "is_serving": True,
        "task_count": 412,
        "fd_count": 900,
        "rss_kb": 250_000,
        "pool_size": 40,
        "connections_in_flight": 95,
        "tag": "probe",
    }
    rec.update(overrides)
    return rec


def _write_jsonl(path: Path, *records: dict[str, Any], trailing: bytes = b"") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = b"".join(json.dumps(r, separators=(",", ":")).encode() + b"\n" for r in records)
    path.write_bytes(body + trailing)


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mgr, "_wall_clock", lambda: _NOW)


@pytest.fixture
def report_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the manager's reader at a file under tmp_path, never the data home."""
    path = tmp_path / "logs" / self_report.SELF_REPORT_FILENAME
    monkeypatch.setattr(mgr.GatewayManager, "_self_report_path", staticmethod(lambda: path))
    return path


@pytest.fixture
def counters(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, dict]]:
    seen: list[tuple[str, dict]] = []
    monkeypatch.setattr(mgr, "emit_counter", lambda name, attrs: seen.append((name, attrs)))
    return seen


async def _run_probe_loop_until(
    manager: mgr.GatewayManager,
    monkeypatch: pytest.MonkeyPatch,
    *,
    ping_results: list[bool],
) -> tuple[str | None, list[float]]:
    """Drive ``_liveness_probe_loop`` through ``ping_results`` with no real waits.

    Returns the loop's verdict (``None`` if it was still probing when the
    scripted pings ran out) and every ``timeout`` the pings were issued with.
    """
    monkeypatch.setattr(mgr, "_LIVENESS_PING_INTERVAL_SECS", 0.0)
    timeouts: list[float] = []
    remaining = list(ping_results)

    async def _ping(*, timeout: float) -> bool:
        timeouts.append(timeout)
        if not remaining:
            # Scripted pings exhausted: the loop is still alive and probing.
            manager._stopping = True
            return True
        return remaining.pop(0)

    monkeypatch.setattr(manager, "_ping_once", _ping)
    verdict = await asyncio.wait_for(manager._liveness_probe_loop(), timeout=5)
    return (None if verdict == "stopping" else verdict), timeouts


# ── (a) alive-but-overloaded: no kill ──────────────────────────────


class TestOverloadedDaemonIsKept:
    @pytest.mark.asyncio
    async def test_three_misses_plus_a_fresh_matching_report_do_not_kill(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        clock: None,
        report_path: Path,
        counters: list[tuple[str, dict]],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        manager = _manager(tmp_path)
        _write_jsonl(report_path, _probe(ts_epoch=_NOW - 20.0))
        caplog.set_level(logging.WARNING, logger=mgr.logger.name)

        # Three misses reach the threshold; the loop must survive them and
        # still be probing when the script runs out (the 4th, terminating ping).
        verdict, _ = await _run_probe_loop_until(
            manager, monkeypatch, ping_results=[False, False, False]
        )

        assert verdict is None, f"the daemon was declared dead: {verdict!r}"
        assert manager._process is not None and manager._process.pid == _OUR_PID
        overloaded = [r for r in caplog.records if "OVERLOADED" in r.getMessage()]
        assert len(overloaded) == 1, "exactly one warning per averted kill"
        msg = overloaded[0].getMessage()
        assert "connections_in_flight=95" in msg
        assert "task_count=412" in msg
        assert f"pid={_OUR_PID}" in msg
        assert counters == [(ev.MCP_GATEWAY_LIVENESS_OVERLOADED, {})]

    @pytest.mark.asyncio
    async def test_the_miss_counter_is_reset_after_an_averted_kill(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        clock: None,
        report_path: Path,
        counters: list[tuple[str, dict]],
    ) -> None:
        """Two more misses after the reprieve must NOT reach the threshold.

        If the counter were left at 3, the very next miss would consult the
        report again and -- worse -- a report that had meanwhile gone stale
        would kill on ONE miss instead of three.
        """
        manager = _manager(tmp_path)
        _write_jsonl(report_path, _probe())

        verdict, _ = await _run_probe_loop_until(
            manager, monkeypatch, ping_results=[False, False, False, False, False]
        )

        assert verdict is None
        # One averted kill at miss 3; misses 4 and 5 never reached a verdict.
        assert len(counters) == 1

    @pytest.mark.asyncio
    async def test_a_second_threshold_with_a_fresh_report_is_averted_again(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        clock: None,
        report_path: Path,
        counters: list[tuple[str, dict]],
    ) -> None:
        manager = _manager(tmp_path)
        _write_jsonl(report_path, _probe())

        verdict, _ = await _run_probe_loop_until(manager, monkeypatch, ping_results=[False] * 6)

        assert verdict is None
        assert len(counters) == 2

    @pytest.mark.asyncio
    async def test_a_report_at_the_edge_of_the_freshness_window_still_counts(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clock: None, report_path: Path
    ) -> None:
        """Exactly three probe intervals old is the last instant a report vouches."""
        manager = _manager(tmp_path)
        _write_jsonl(report_path, _probe(ts_epoch=_NOW - mgr._SELF_REPORT_FRESH_SECS))
        assert await manager._alive_but_overloaded() is not None

    @pytest.mark.asyncio
    async def test_a_report_stamped_slightly_in_the_future_still_counts(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clock: None, report_path: Path
    ) -> None:
        """Both stamps are wall-clock; a step between processes must not read as stale."""
        manager = _manager(tmp_path)
        _write_jsonl(report_path, _probe(ts_epoch=_NOW + 5.0))
        assert await manager._alive_but_overloaded() is not None


# ── (b)(c)(d) anything less than fresh, ours and serving: zombie ───


class TestZombiePathStillRuns:
    async def _expect_zombie(
        self, manager: mgr.GatewayManager, monkeypatch: pytest.MonkeyPatch
    ) -> str:
        verdict, _ = await _run_probe_loop_until(
            manager, monkeypatch, ping_results=[False, False, False]
        )
        assert verdict is not None, "the zombie path must have run"
        assert verdict.startswith("zombie detected: 3 consecutive ping failures")
        return verdict

    @pytest.mark.asyncio
    async def test_a_stale_report_is_no_evidence_of_life(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        clock: None,
        report_path: Path,
        counters: list[tuple[str, dict]],
    ) -> None:
        manager = _manager(tmp_path)
        _write_jsonl(report_path, _probe(ts_epoch=_NOW - mgr._SELF_REPORT_FRESH_SECS - 1.0))
        await self._expect_zombie(manager, monkeypatch)
        assert counters == []

    @pytest.mark.asyncio
    async def test_a_report_from_another_pid_is_not_ours(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clock: None, report_path: Path
    ) -> None:
        """A predecessor's or a foreign daemon's fresh report says nothing about ours."""
        manager = _manager(tmp_path)
        _write_jsonl(report_path, _probe(pid=_OUR_PID + 1))
        await self._expect_zombie(manager, monkeypatch)

    @pytest.mark.asyncio
    async def test_a_missing_file_is_no_evidence_of_life(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clock: None, report_path: Path
    ) -> None:
        manager = _manager(tmp_path)
        assert not report_path.exists()
        await self._expect_zombie(manager, monkeypatch)

    @pytest.mark.asyncio
    async def test_a_corrupt_last_record_is_no_evidence_of_life(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clock: None, report_path: Path
    ) -> None:
        manager = _manager(tmp_path)
        report_path.parent.mkdir(parents=True)
        report_path.write_bytes(b"{not json at all\n")
        await self._expect_zombie(manager, monkeypatch)

    @pytest.mark.asyncio
    async def test_a_zombie_detected_record_is_the_daemon_confirming_death(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clock: None, report_path: Path
    ) -> None:
        manager = _manager(tmp_path)
        _write_jsonl(report_path, _probe(tag="zombie_detected", is_serving=False))
        await self._expect_zombie(manager, monkeypatch)

    @pytest.mark.asyncio
    async def test_is_serving_false_on_a_probe_is_no_evidence_of_life(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clock: None, report_path: Path
    ) -> None:
        manager = _manager(tmp_path)
        _write_jsonl(report_path, _probe(is_serving=False))
        await self._expect_zombie(manager, monkeypatch)

    @pytest.mark.asyncio
    async def test_is_serving_null_on_a_probe_is_no_evidence_of_life(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clock: None, report_path: Path
    ) -> None:
        """``None`` is what the daemon writes when ``is_serving()`` itself raised."""
        manager = _manager(tmp_path)
        _write_jsonl(report_path, _probe(is_serving=None))
        await self._expect_zombie(manager, monkeypatch)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("ts", ["1800000000", None, True])
    async def test_a_non_numeric_timestamp_is_no_evidence_of_life(
        self,
        ts: Any,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        clock: None,
        report_path: Path,
    ) -> None:
        manager = _manager(tmp_path)
        _write_jsonl(report_path, _probe(ts_epoch=ts))
        assert await manager._alive_but_overloaded() is None

    @pytest.mark.asyncio
    async def test_no_owned_process_means_nothing_to_vouch_for(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clock: None, report_path: Path
    ) -> None:
        manager = _manager(tmp_path)
        _write_jsonl(report_path, _probe())
        manager._process = None
        assert await manager._alive_but_overloaded() is None

    @pytest.mark.asyncio
    async def test_the_report_is_only_consulted_at_the_threshold(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clock: None, report_path: Path
    ) -> None:
        """One or two misses never touch the file: the run may still self-heal."""
        manager = _manager(tmp_path)
        reads = AsyncMock(return_value=None)
        monkeypatch.setattr(manager, "_alive_but_overloaded", reads)
        verdict, _ = await _run_probe_loop_until(
            manager, monkeypatch, ping_results=[False, False, True, False]
        )
        assert verdict is None
        reads.assert_not_awaited()


# ── (e) the bounded tail read ──────────────────────────────────────


class TestReadLastProbe:
    def test_a_torn_last_line_yields_the_record_before_it(self, tmp_path: Path) -> None:
        """The writer appends whole lines; an unterminated tail is mid-write."""
        path = tmp_path / "d.jsonl"
        older = _probe(ts_epoch=_NOW - 50.0, connections_in_flight=80)
        _write_jsonl(path, older, trailing=b'{"ts_epoch":1800000000.0,"pid":10795,"is_ser')
        got = self_report.read_last_probe(path)
        assert got == older

    def test_the_last_complete_record_wins(self, tmp_path: Path) -> None:
        path = tmp_path / "d.jsonl"
        a = _probe(ts_epoch=_NOW - 60.0)
        b = _probe(ts_epoch=_NOW - 30.0)
        _write_jsonl(path, a, b)
        assert self_report.read_last_probe(path) == b

    def test_only_the_tail_is_read(self, tmp_path: Path) -> None:
        """A file far larger than the tail budget costs the same and still parses."""
        path = tmp_path / "d.jsonl"
        filler = [_probe(ts_epoch=_NOW - 100_000.0 + i) for i in range(2_000)]
        last = _probe(ts_epoch=_NOW - 10.0)
        _write_jsonl(path, *filler, last)
        assert path.stat().st_size > 4 * self_report.SELF_REPORT_TAIL_BYTES
        assert self_report.read_last_probe(path) == last

    def test_a_tail_that_starts_mid_line_does_not_parse_the_fragment(self, tmp_path: Path) -> None:
        """With a tiny budget the slice opens inside a record; that fragment is
        not a candidate, and the complete record after it is what comes back."""
        path = tmp_path / "d.jsonl"
        a = _probe(ts_epoch=_NOW - 60.0)
        b = _probe(ts_epoch=_NOW - 30.0)
        _write_jsonl(path, a, b)
        b_len = len(json.dumps(b, separators=(",", ":")).encode()) + 1
        assert self_report.read_last_probe(path, tail_bytes=b_len + 7) == b

    def test_a_budget_smaller_than_the_last_record_yields_none(self, tmp_path: Path) -> None:
        """Fail closed rather than parse half a record."""
        path = tmp_path / "d.jsonl"
        _write_jsonl(path, _probe())
        assert self_report.read_last_probe(path, tail_bytes=16) is None

    @pytest.mark.parametrize(
        "body",
        [
            b"",
            b"\n\n",
            b"{not json\n",
            b"[1,2,3]\n",
            b'"a string"\n',
            b"\xff\xfe\n",
            b'{"ok":true}',  # complete JSON but no terminator: still mid-write
        ],
    )
    def test_nothing_usable_yields_none(self, tmp_path: Path, body: bytes) -> None:
        path = tmp_path / "d.jsonl"
        path.write_bytes(body)
        assert self_report.read_last_probe(path) is None

    def test_a_missing_file_yields_none(self, tmp_path: Path) -> None:
        assert self_report.read_last_probe(tmp_path / "absent.jsonl") is None

    def test_a_directory_yields_none(self, tmp_path: Path) -> None:
        assert self_report.read_last_probe(tmp_path) is None


# ── the two ping deadlines ─────────────────────────────────────────


class TestPingDeadlines:
    def test_the_watchdog_deadline_is_wider_than_the_spawn_deadline(self) -> None:
        assert mgr._LIVENESS_PING_TIMEOUT_SECS == 10.0
        assert mgr._LIVENESS_PING_TIMEOUT_SECS > mgr._PING_TIMEOUT_SECS == 2.0

    def test_the_freshness_window_is_derived_from_the_writer_s_interval(self) -> None:
        assert mgr._SELF_REPORT_FRESH_SECS == self_report.ZOMBIE_PROBE_INTERVAL_SECS * 3

    @pytest.mark.asyncio
    async def test_the_watchdog_pings_on_the_wide_deadline(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        manager = _manager(tmp_path)
        _, timeouts = await _run_probe_loop_until(manager, monkeypatch, ping_results=[True])
        assert timeouts and set(timeouts) == {mgr._LIVENESS_PING_TIMEOUT_SECS}

    @pytest.mark.asyncio
    async def test_the_public_ping_and_adoption_keep_the_short_deadline(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        manager = _manager(tmp_path)
        seen: list[float] = []

        async def _raw(*, timeout: float) -> dict | None:
            seen.append(timeout)
            return {"type": "pong"}

        monkeypatch.setattr(manager, "_ping_raw", _raw)
        assert await manager.ping() is True
        assert await manager._ping_payload() == {"type": "pong"}
        assert seen == [mgr._PING_TIMEOUT_SECS, mgr._PING_TIMEOUT_SECS]


# ── the writer and the reader agree on the path ────────────────────


class TestPathAgreement:
    def test_gatewayd_writes_where_the_manager_reads(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from kiro_crew.mcp_gateway import gatewayd as gw

        monkeypatch.setattr(gw, "_config_dir", lambda: tmp_path)
        monkeypatch.setattr(self_report, "config_dir", lambda: tmp_path)
        assert gw._zombie_diagnostic_path() == mgr.GatewayManager._self_report_path()
        assert gw._zombie_diagnostic_path() == (
            tmp_path / "logs" / "gatewayd_zombie_diagnostic.jsonl"
        )

    def test_gatewayd_s_probe_interval_is_the_shared_one(self) -> None:
        from kiro_crew.mcp_gateway import gatewayd as gw

        assert gw._ZOMBIE_PROBE_INTERVAL_SECS == self_report.ZOMBIE_PROBE_INTERVAL_SECS
