"""Real ACP bytes for workflow warm reuse; no gateway or namespace is replaced."""

import json
import subprocess
import sys

from kiro_crew.testing import fake_acp_backend


def test_workflow_warm_reset_allocates_distinct_native_sessions(tmp_path):
    requests = [
        {"id": 1, "method": "initialize", "params": {}},
        {"id": 2, "method": "session/new", "params": {"cwd": str(tmp_path)}},
        {"id": 3, "method": "session/new", "params": {"cwd": str(tmp_path)}},
        {"id": 4, "method": "_kiro.dev/session/terminate", "params": {"sessionId": "fake-1"}},
        {
            "id": 5,
            "method": "session/prompt",
            "params": {
                "sessionId": "fake-2",
                "prompt": [{"type": "text", "text": "warm workflow worker"}],
            },
        },
    ]
    result = subprocess.run(
        [sys.executable, "-m", "kiro_crew.testing.fake_acp_backend", "acp"],
        input="".join(json.dumps({"jsonrpc": "2.0", **row}) + "\n" for row in requests),
        capture_output=True,
        text=True,
        encoding="utf-8",
        cwd=tmp_path,
        timeout=20,
        check=True,
    )
    messages = [json.loads(line) for line in result.stdout.splitlines()]
    replies = {row["id"]: row["result"] for row in messages if "id" in row}
    # AcpSessionProvider creates the replacement before destroying the old handle.
    # Reusing its ID lets old.destroy() unregister the NEW notification queue.
    assert replies[2]["sessionId"] != replies[3]["sessionId"]
    assert replies[3]["sessionId"] == "fake-2"
    chunks = [row["params"] for row in messages if row.get("method") == "session/update"]
    assert chunks == [
        {
            "sessionId": replies[3]["sessionId"],
            "update": {
                "sessionUpdate": "agent_message_chunk",
                "content": {"type": "text", "text": fake_acp_backend.REPLY_TEXT},
            },
        }
    ]
    assert replies[5]["stopReason"] == "end_turn"


def test_workflow_timeout_summary_omits_private_payloads():
    from kiro_crew.testing.workflow_memory_scenario import progress_summary

    private = "PRIVATE_PAYLOAD_MUST_NOT_APPEAR"
    run = {
        "status": "running",
        "source": private,
        "result": private,
        "agent_errors": {"0": private},
        "events": [
            {"type": "run_started", "data": {"args": private}},
            {"type": "agent_started", "data": {"call_index": 0, "label": private}},
            {"type": "agent_finished", "data": {"agent_id": "a0", "error": private}},
            {"type": "agent_started", "data": {"call_index": 1, "label": private}},
            {"type": "log", "data": {"message": private}},
        ],
    }
    assert progress_summary(run) == {
        "status": "running",
        "events": {"run_started": 1, "agent_started": 2, "agent_finished": 1, "log": 1},
        "pending_calls": [1],
    }
    assert private not in json.dumps(progress_summary(run))


def test_workflow_backend_routes_each_session_to_its_own_projection(tmp_path, monkeypatch):
    """Only observe the scenario entry; projection and proof code are not replaced."""
    import io

    from kiro_crew.testing import workflow_memory_scenario

    captured = io.StringIO()
    monkeypatch.setattr(fake_acp_backend, "_SESSIONS", {})
    monkeypatch.setattr(fake_acp_backend.sys, "stdout", captured)
    calls = []

    def decision(text, servers, cwd):
        calls.append((text, servers, cwd))
        return "model decision"

    monkeypatch.setattr(workflow_memory_scenario, "respond", decision)

    def request(index, method, params):
        captured.seek(0)
        captured.truncate()
        fake_acp_backend._handle({"id": index, "method": method, "params": params})
        return [json.loads(line) for line in captured.getvalue().splitlines()]

    projections = [
        {"cwd": str(tmp_path / name), "mcpServers": [{"name": name, "command": name}]}
        for name in ("member-a", "member-b")
    ]
    a, b = [
        request(index, "session/new", params)[0]["result"]["sessionId"]
        for index, params in enumerate(projections, 1)
    ]
    prompt = [{"type": "text", "text": "[[WF_E2E:WORK:A]]"}]
    request(3, "session/prompt", {"sessionId": a, "prompt": prompt})
    assert calls[-1][1:] == (projections[0]["mcpServers"], projections[0]["cwd"])
    request(4, "_kiro.dev/session/terminate", {"sessionId": a})
    request(5, "session/prompt", {"sessionId": b, "prompt": prompt})
    assert calls[-1][1:] == (projections[1]["mcpServers"], projections[1]["cwd"])
    count = len(calls)
    for index, params in enumerate(
        (
            {"sessionId": a, "prompt": prompt},
            {"sessionId": "unknown", "prompt": prompt},
            {"prompt": prompt},
        ),
        6,
    ):
        response = request(index, "session/prompt", params)
        assert len(response) == 1 and "error" in response[0]
        assert len(calls) == count, "Unknown session must not borrow a live projection"
