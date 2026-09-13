"""Real namespace gateway, scheduled workflows, projected MCP and SQLite stores.

Only the model's decisions are deterministic. No identity, proof, ownership,
namespace, store resolver, SessionManager or workflow service is replaced.
Run explicitly in the namespace-enabled CI job with KIROCREW_E2E_REQUIRE=1.
"""

import json
import os
import re
import sqlite3
import time
import urllib.request

import pytest
from e2e.test_gateway_boot_matrix import _await_assistant_reply, _booted

from kiro_crew.testing.workflow_memory_scenario import progress_summary, source

pytestmark = pytest.mark.skipif(
    not os.environ.get("KIROCREW_E2E"), reason="Set KIROCREW_E2E=1 for real private workflow E2E"
)


def _post_as(client, path, body, session):
    request = urllib.request.Request(
        f"http://localhost:{client._port}{path}",
        data=json.dumps(body).encode(),
        method="POST",
        headers={"Content-Type": "application/json", "X-Session-Key": session},
    )
    return client._open(request, timeout=60)


def _finished(client, run_id):
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        run = client.get(f"/api/workflows/runs/{run_id}")
        if run["status"] != "running":
            assert run["status"] == "finished", run
            assert not run.get("agent_errors"), run
            return run
        time.sleep(0.1)
    pytest.fail(f"Workflow did not terminate: {run_id}; {json.dumps(progress_summary(run))}")


def _assert_result(run, marker):
    result = json.dumps(run["result"])
    assert f"WF_E2E_{marker}_PRIVATE_LESSON" in result, run
    assert '"isError": true' not in result, run
    for other in {"A", "B", "V"} - {marker}:
        assert f"WF_E2E_{other}_PRIVATE_LESSON" not in result, run


def test_private_workflows_execute_over_real_projected_mcp():
    with _booted("rich") as (handle, client):
        cfg = json.loads((handle.home / "config.json").read_text(encoding="utf-8"))
        assert cfg.get("agent", {}).get("sandbox", "auto") == "auto"
        members = {}
        for marker in ("A", "B"):
            member = f"workflow-e2e-{marker.lower()}"
            client.post("/api/agents", {"name": member, "kiro_agent": "kirocrew"})
            members[marker] = client.post("/api/chat/slots", {"agent": member})["key"]
        members["V"] = client.post("/api/chat/slots", {"agent": "default"})["key"]
        starts = {}
        # All three runs are admitted before waiting, so their workers overlap.
        for marker, slot in members.items():
            client.post("/api/chat?ws=1", {"slot": slot, "message": f"[[WF_E2E:START:{marker}]]"})
        for marker, slot in members.items():
            response = _await_assistant_reply(client, slot)
            match = re.search(r"wf_\d+", str(response["content"]))
            assert match, response
            starts[marker] = match.group()
        for marker, run_id in starts.items():
            _assert_result(_finished(client, run_id), marker)
        session = f"dashboard:{members['A']}"
        authored = _post_as(
            client, "/api/workflows/author", {"intent": "[[WF_E2E:AUTHOR:A]]"}, session
        )
        assert authored["ok"], authored
        assert authored["source"] == source("A")
        intent = _post_as(
            client, "/api/workflows/run_intent", {"intent": "[[WF_E2E:AUTHOR:A]]"}, session
        )
        _assert_result(_finished(client, intent["run_id"]), "A")
        definition = client.post("/api/workflows/definitions", {"source": source("A")})[
            "definition"
        ]
        saved = _post_as(client, f"/api/workflows/definitions/{definition['id']}/run", {}, session)
        _assert_result(_finished(client, saved["run_id"]), "A")
        rerun = _post_as(
            client, f"/api/workflows/runs/{starts['A']}/rerun", {"from_index": 2}, session
        )
        _assert_result(_finished(client, rerun["run_id"]), "A")
        # These calls originate in workflow workers, not the owner HTTP client.
        nested_source = 'META = {"name": "nested"}\nasync def workflow(ctx):\n    return await ctx.agent("[[WF_E2E:NEST:A]]")\n'
        nested_start = _post_as(client, "/api/workflows/run", {"source": nested_source}, session)
        nested = json.loads(_finished(client, nested_start["run_id"])["result"])
        nested_id = re.search(r"wf_\d+", json.dumps(nested["nested_workflow"])).group()
        _assert_result(_finished(client, nested_id), "A")
        spawn_text = "\n".join(
            item.get("text", "") for item in nested["nested_spawn"].get("content", [])
        )
        spawn_match = re.search(
            r"^\s+([A-Za-z0-9_-]+)(?: \([^)]*\))?: \[\[WF_E2E", spawn_text, re.MULTILINE
        )
        assert spawn_match, nested
        spawn_id = spawn_match.group(1)
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            try:
                result = json.loads(
                    (handle.home / "subagents" / spawn_id / "result.txt").read_text(
                        encoding="utf-8"
                    )
                )
                assert "WF_E2E_A_PRIVATE_LESSON" in json.dumps(result)
                break
            except (FileNotFoundError, json.JSONDecodeError):
                time.sleep(0.1)
        else:
            pytest.fail("Nested MCP spawn did not produce its real memory result")
        protected = json.loads(
            (handle.home / "member-memory-bindings" / spawn_id / "memory.json").read_text(
                encoding="utf-8"
            )
        )
        config = json.loads((handle.home / "config.json").read_text(encoding="utf-8"))
        assert protected["memory_store"] == config["agents"]["workflow-e2e-a"]["memory_store"]
        for marker in ("B", "V"):
            control_source = f'META = {{"name": "foreign control"}}\nasync def workflow(ctx):\n    return await ctx.agent("[[WF_E2E:CONTROL:{marker}:{starts["A"]}]]")\n'
            control = _post_as(
                client,
                "/api/workflows/run",
                {"source": control_source},
                f"dashboard:{members[marker]}",
            )
            results = json.loads(_finished(client, control["run_id"])["result"])
            for tool, result in results.items():
                text = json.dumps(result)
                if tool == "workflow_list":
                    assert starts["A"] not in text
                else:
                    assert "refused" in text.lower(), (tool, result)
        config = json.loads((handle.home / "config.json").read_text(encoding="utf-8"))
        for marker in ("A", "B", "V"):
            if marker == "V":
                path = handle.home / "memory.db"
            else:
                store = config["agents"][f"workflow-e2e-{marker.lower()}"]["memory_store"]
                path = handle.home / "memory_stores" / store / "memory.db"
            with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as db:
                content = "\n".join(db.iterdump())
            assert f"WF_E2E_{marker}_PRIVATE_LESSON" in content
            for other in {"A", "B", "V"} - {marker}:
                assert f"WF_E2E_{other}_PRIVATE_LESSON" not in content
        from e2e.test_gateway_boot_matrix import _Client

        old_pid, old_home = handle.proc.pid, handle.home
        handle = handle.restart()
        assert handle.proc.pid != old_pid
        assert handle.home == old_home
        client = _Client(handle.port, handle.token)
        client.diagnostics = handle.diagnostics
        restored = client.get(f"/api/workflows/runs/{starts['A']}")
        _assert_result(restored, "A")
        replay = _post_as(
            client, f"/api/workflows/runs/{starts['A']}/rerun", {"from_index": 2}, session
        )
        assert replay["run_id"] != starts["A"]
        assert replay["replayed_before"] == 2
        _assert_result(_finished(client, replay["run_id"]), "A")
