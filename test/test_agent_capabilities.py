"""Real-file capability editing with isolated config, sidecar and agent homes."""

from __future__ import annotations

import asyncio
import json

import pytest

from kiro_crew import agent, agent_state
from kiro_crew.agent_capabilities import (
    CapabilityError,
    CapabilityService,
    prepare_member_capabilities,
    validate_request,
)
from kiro_crew.config import loader


@pytest.fixture
def editor(tmp_path, monkeypatch):
    home = tmp_path / "home"
    home.mkdir()
    specs = home / "agents"
    specs.mkdir()
    monkeypatch.setenv("KIROCREW_HOME", str(home))
    monkeypatch.setenv("KIRO_HOME", str(home / "kiro"))
    monkeypatch.setattr(agent, "KIRO_AGENTS_DIR", specs)
    monkeypatch.setattr(agent_state, "_state_path", lambda: home / "agent_model_state.json")
    monkeypatch.setattr("kiro_crew.agent_capabilities.default_project_dir", lambda _: "")
    loader._invalidate_config_cache()
    config = {
        "agents": {
            "A": {"kiro_agent": "parent", "workspace": "default", "memory_store": "default"},
            "B": {"kiro_agent": "parent", "workspace": "default", "memory_store": "default"},
        },
        "dashboard": {"bot_name": "unchanged"},
    }
    (home / "config.json").write_text(json.dumps(config), encoding="utf-8")
    parent = {
        "includeMcpJson": False,
        "name": "parent",
        "prompt": "original",
        "tools": ["read"],
        "allowedTools": [],
        "mcpServers": {
            "search": {"command": "search", "args": ["old"], "env": {"API_KEY": "opaque-secret"}}
        },
        "resources": ["file://guide.md", "skill://manual/*"],
    }
    (specs / "parent.json").write_text(json.dumps(parent), encoding="utf-8")
    return CapabilityService(), home, specs, parent


def save(service, operations=None, *, enroll=False, accept_parent=None, accept_members=None):
    body = {
        "revision": service.get("A")["revision"],
        "enroll": enroll,
        "operations": operations or [],
        "accept_parent": accept_parent or [],
        "accept_members": accept_members or [],
    }
    preview = service.preview("A", body)
    return service.put("A", {**body, "preview_token": preview["preview_token"]})


def spec_for(home, specs, member="A"):
    config = json.loads((home / "config.json").read_text())
    return json.loads((specs / (config["agents"][member]["kiro_agent"] + ".json")).read_text())


def test_preview_is_pure_and_redacts_transports(editor):
    service, home, specs, parent = editor
    current = service.get("A")
    # Normal gateway startup has already loaded/migrated the installation config.
    # Preview starts from the revision obtained by GET and must write nothing.
    before = {p: p.read_bytes() for p in home.rglob("*") if p.is_file()}
    preview = service.preview("A", {"revision": current["revision"], "enroll": True})
    assert preview["mode"] == "inherited"
    assert "opaque-secret" not in json.dumps(preview)
    assert {p: p.read_bytes() for p in home.rglob("*") if p.is_file()} == before


def test_enrollment_is_explicit_and_keeps_other_bindings(editor):
    service, home, specs, parent = editor
    with pytest.raises(CapabilityError, match="enrollment_required"):
        save(service)
    result = save(service, enroll=True)
    assert result["runtime"]["status"] == "pending"
    assert spec_for(home, specs)["prompt"] == "original"
    assert spec_for(home, specs, "B") == parent
    assert json.loads((home / "config.json").read_text())["dashboard"]["bot_name"] == "unchanged"
    prepared = prepare_member_capabilities("A")
    assert prepared["status"] == "unverified"
    assert prepared["revision"]


def test_whole_transport_replacement_and_custom_resources_survive(editor):
    service, home, specs, _ = editor
    save(
        service,
        [
            {
                "section": "mcpServers",
                "id": "search",
                "action": "set",
                "value": {"url": "https://example.test/mcp"},
            }
        ],
        enroll=True,
    )
    spec = spec_for(home, specs)
    assert spec["mcpServers"]["search"] == {"url": "https://example.test/mcp"}
    assert spec["resources"] == ["file://guide.md", "skill://manual/*"]


def test_tombstone_survives_parent_update_and_restore_is_per_item(editor):
    service, home, specs, parent = editor
    save(
        service,
        [
            {"section": "tools", "id": "read", "action": "remove"},
            {"section": "prompt", "id": "prompt", "action": "set", "value": "mine"},
        ],
        enroll=True,
    )
    parent["prompt"] = "upstream"
    parent["tools"].append("write")
    (specs / "parent.json").write_text(json.dumps(parent))
    view = service.get("A")
    assert any(c["id"] == "prompt" and c["conflict"] for c in view["parent_changes"])
    save(service, [{"section": "tools", "id": "read", "action": "inherit"}])
    spec = spec_for(home, specs)
    assert spec["tools"] == ["read"]
    assert spec["prompt"] == "mine"
    save(service, accept_parent=[{"section": "tools", "id": "write"}])
    assert spec_for(home, specs)["tools"] == ["read", "write"]


def test_accepting_a_conflicting_parent_change_keeps_the_local_value_until_inherit(editor):
    """Accepting only moves the reviewed Parent baseline. An explicit local
    value stays until the owner sets that row to Inherited, which then adopts
    the accepted Parent value without another review."""
    service, home, specs, parent = editor
    save(
        service,
        [{"section": "prompt", "id": "prompt", "action": "set", "value": "mine"}],
        enroll=True,
    )
    parent["prompt"] = "upstream"
    (specs / "parent.json").write_text(json.dumps(parent))
    change = next(c for c in service.get("A")["parent_changes"] if c["id"] == "prompt")
    assert change["conflict"] is True
    view = save(service, accept_parent=[{"section": "prompt", "id": "prompt"}])
    assert spec_for(home, specs)["prompt"] == "mine"
    assert view["parent_changes"] == []
    assert next(r for r in view["rows"] if r["section"] == "prompt")["state"] == "local"
    save(service, [{"section": "prompt", "id": "prompt", "action": "inherit"}])
    assert spec_for(home, specs)["prompt"] == "upstream"


def test_stale_preview_parent_and_request_rejected(editor):
    service, home, specs, parent = editor
    body = {"revision": service.get("A")["revision"], "enroll": True}
    preview = service.preview("A", body)
    parent["prompt"] = "new"
    (specs / "parent.json").write_text(json.dumps(parent))
    with pytest.raises(CapabilityError, match="stale_revision"):
        service.put("A", {**body, "preview_token": preview["preview_token"]})
    body["revision"] = service.get("A")["revision"]
    with pytest.raises(CapabilityError, match="stale_preview"):
        service.put("A", {**body, "preview_token": preview["preview_token"]})
    assert len(list(specs.glob("*.json"))) == 1


@pytest.mark.parametrize(
    "extra",
    [
        {"unknown": True},
        {"enroll": "true"},
        {"operations": [{"section": "prompt", "id": "prompt", "action": "set", "value": None}]},
        {"operations": [{"section": "prompt", "id": "prompt", "action": "inherit", "value": "x"}]},
    ],
)
def test_request_rejects_ambiguous_or_unknown_fields(extra):
    with pytest.raises(CapabilityError):
        validate_request({"revision": "r", **extra})


def test_legacy_enrollment_preserves_all_local_choices(editor):
    service, home, specs, parent = editor
    private = {**parent, "name": "private", "tools": [], "prompt": "legacy"}
    (specs / "private.json").write_text(json.dumps(private))
    agent_state.set_fork_info("private", "parent", "A")
    config = json.loads((home / "config.json").read_text())
    config["agents"]["A"]["kiro_agent"] = "private"
    (home / "config.json").write_text(json.dumps(config))
    loader._invalidate_config_cache()
    assert service.get("A")["mode"] == "legacy_snapshot"
    save(service, enroll=True)
    assert spec_for(home, specs)["tools"] == []
    assert spec_for(home, specs)["prompt"] == "legacy"
    save(service, [{"section": "tools", "id": "read", "action": "inherit"}])
    assert spec_for(home, specs)["tools"] == ["read"]


def test_wildcard_exclusion_is_refused(editor):
    service, _, specs, parent = editor
    parent["tools"] = ["*"]
    (specs / "parent.json").write_text(json.dumps(parent))
    with pytest.raises(CapabilityError, match="wildcard_exclusion_unrepresentable"):
        save(service, [{"section": "tools", "id": "read", "action": "remove"}], enroll=True)


def test_missing_parent_preserves_saved_private_spec(editor):
    service, home, specs, _ = editor
    save(service, enroll=True)
    old = spec_for(home, specs)
    (specs / "parent.json").unlink()
    view = service.get("A")
    assert not view["template"]["available"]
    assert view["template"]["error_code"] == "parent_missing"
    with pytest.raises(CapabilityError, match="parent_missing"):
        save(service)
    assert spec_for(home, specs) == old


def test_corrupt_sidecar_is_not_legacy_mode(editor):
    service, home, _, _ = editor
    (home / "agent_model_state.json").write_text("{")
    with pytest.raises(ValueError):
        service.get("A")


def test_runtime_seam_refuses_unverified_bytes(editor):
    service, home, specs, _ = editor
    save(service, enroll=True)
    spec = spec_for(home, specs)
    path = specs / (spec["name"] + ".json")
    spec["prompt"] = "untracked"
    path.write_text(json.dumps(spec))
    with pytest.raises(CapabilityError, match="materialization_changed"):
        prepare_member_capabilities("A")


def test_selected_parent_rows_can_be_accepted_for_multiple_members(editor):
    service, home, specs, parent = editor
    save(service, enroll=True)
    body = {"revision": service.get("B")["revision"], "enroll": True}
    preview = service.preview("B", body)
    service.put("B", {**body, "preview_token": preview["preview_token"]})
    parent["tools"].append("write")
    (specs / "parent.json").write_text(json.dumps(parent))
    save(service, accept_parent=[{"section": "tools", "id": "write"}], accept_members=["B"])
    assert spec_for(home, specs)["tools"] == ["read", "write"]
    assert spec_for(home, specs, "B")["tools"] == ["read", "write"]


def test_failed_config_publication_preserves_all_valid_bindings(editor, monkeypatch):
    service, home, specs, parent = editor
    save(service, enroll=True)
    before = json.loads((home / "config.json").read_text())
    old_spec = spec_for(home, specs)
    body = {
        "revision": service.get("A")["revision"],
        "operations": [{"section": "prompt", "id": "prompt", "action": "set", "value": "new"}],
    }
    preview = service.preview("A", body)

    def fail(*args, **kwargs):
        raise OSError("disk unavailable")

    monkeypatch.setattr(loader, "write_config_atomically", fail)
    with pytest.raises(OSError):
        service.put("A", {**body, "preview_token": preview["preview_token"]})
    assert json.loads((home / "config.json").read_text()) == before
    assert spec_for(home, specs) == old_spec
    assert spec_for(home, specs, "B") == parent
    assert all(agent_state.get_fork_info(p.stem) for p in specs.glob("crew-*.json"))


def test_skills_preserve_manual_resources_and_do_not_change_permissions(editor):
    _, home, specs, _ = editor
    service = CapabilityService(catalog=lambda project: {"guide": "skill://guide/SKILL.md"})
    save(
        service, [{"section": "skills", "id": "guide", "action": "set", "value": True}], enroll=True
    )
    first = spec_for(home, specs)
    assert first["resources"] == ["file://guide.md", "skill://manual/*", "skill://guide/SKILL.md"]
    save(service, [{"section": "skills", "id": "guide", "action": "remove"}])
    second = spec_for(home, specs)
    assert second["resources"] == ["file://guide.md", "skill://manual/*"]
    for section in ("tools", "allowedTools", "mcpServers"):
        assert first[section] == second[section]


def test_background_refresh_preserves_tombstones_and_holds_expansions(editor):
    service, home, specs, parent = editor
    save(service, [{"section": "tools", "id": "read", "action": "remove"}], enroll=True)
    parent["prompt"] = "updated"
    parent["tools"].append("write")
    (specs / "parent.json").write_text(json.dumps(parent))
    agent._refresh_forked_templates()
    spec = spec_for(home, specs)
    assert spec["prompt"] == "updated"
    assert spec["tools"] == []
    assert prepare_member_capabilities("A")["status"] == "unverified"


def test_old_model_reset_cannot_override_inheritance(editor):
    service, home, specs, _ = editor
    save(service, enroll=True)
    old = spec_for(home, specs)
    with pytest.raises(CapabilityError, match="capabilities_editor_required"):
        agent.reset_agent_model(old["name"])
    assert spec_for(home, specs) == old


def test_transport_types_are_validated_before_any_write(editor):
    service, home, specs, _ = editor
    body = {
        "revision": service.get("A")["revision"],
        "enroll": True,
        "operations": [
            {
                "section": "mcpServers",
                "id": "search",
                "action": "set",
                "value": {"command": "search", "args": "not-an-array"},
            }
        ],
    }
    with pytest.raises(CapabilityError, match="invalid_transport"):
        service.preview("A", body)
    assert not (home / "agent_model_state.json").exists()
    assert len(list(specs.glob("*.json"))) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "caller,app_id,expected",
    [("owner", "", 200), ("viewer", "", 403), ("owner", "app", 403), ("", "", 403)],
)
async def test_http_owner_boundary(editor, caller, app_id, expected):
    from types import SimpleNamespace

    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    from kiro_crew.dashboard.handlers.agent_capabilities import _SERVICE
    from kiro_crew.dashboard.routes.agents import register

    service, _, _, _ = editor

    @web.middleware
    async def identity(request, handler):
        request["user"] = caller
        request["app"] = app_id
        return await handler(request)

    app = web.Application(middlewares=[identity])
    app["state"] = SimpleNamespace(owner_id="owner", push_refresh=lambda _: None)
    app[_SERVICE] = service
    register(app)
    async with TestClient(TestServer(app)) as client:
        response = await client.get("/api/agents/A/capabilities")
        assert response.status == expected
        if expected != 200:
            for method, path in (
                ("post", "/api/agents/A/capabilities/preview"),
                ("put", "/api/agents/A/capabilities"),
            ):
                response = await getattr(client, method)(path, json={})
                assert response.status == 403
            return
        current = await response.json()
        body = {"revision": current["revision"], "enroll": True}
        response = await client.post("/api/agents/A/capabilities/preview", json=body)
        assert response.status == 200
        preview = await response.json()
        response = await client.put(
            "/api/agents/A/capabilities", json={**body, "preview_token": preview["preview_token"]}
        )
        assert response.status == 200
        saved = await response.json()
        assert saved["mode"] == "inherited"
        assert saved["runtime"]["status"] == "pending"
        prepared = await asyncio.to_thread(prepare_member_capabilities, "A")
        response = await client.patch(
            "/api/agents/detail/" + prepared["template"], json={"model": "auto"}
        )
        assert response.status == 409


def test_whole_reset_restores_parent_and_keeps_member_metadata(editor):
    service, home, specs, parent = editor
    save(
        service,
        [
            {"section": "prompt", "id": "prompt", "action": "set", "value": "mine"},
            {"section": "tools", "id": "read", "action": "remove"},
        ],
        enroll=True,
    )
    before = json.loads((home / "config.json").read_text())["agents"]["A"]
    target = before["kiro_agent"]
    service.reset("A", target)
    after = json.loads((home / "config.json").read_text())["agents"]["A"]
    assert {k: v for k, v in before.items() if k != "kiro_agent"} == {
        k: v for k, v in after.items() if k != "kiro_agent"
    }
    assert spec_for(home, specs)["prompt"] == parent["prompt"]
    assert spec_for(home, specs)["tools"] == parent["tools"]
    assert service.get("A")["mode"] == "inherited"


def test_publish_flattens_saved_snapshot_without_accepting_pending_parent(editor):
    service, home, specs, parent = editor
    save(service, enroll=True)
    parent["tools"].append("write")
    (specs / "parent.json").write_text(json.dumps(parent))
    target = spec_for(home, specs)["name"]
    result = service.publish("A", target, "published")
    assert result["template"] == "published"
    spec = spec_for(home, specs)
    assert spec["tools"] == ["read"]
    assert "capabilities" not in spec
    assert "private_to" not in spec
    assert agent_state.get_fork_info("published") is None
    assert service.get("A")["mode"] == "shared"
    assert spec_for(home, specs, "B") == parent


def test_derived_permissions_follow_owner_approval_edits(editor):
    from kiro_crew.agent_sdk.drivers.acp import derived_agent_permissions

    service, home, specs, parent = editor
    parent["permissions"] = {"rules": []}
    (specs / "parent.json").write_text(json.dumps(parent))
    save(
        service,
        [{"section": "allowedTools", "id": "@search", "action": "set", "value": True}],
        enroll=True,
    )
    spec = spec_for(home, specs)
    assert spec["permissions"] == derived_agent_permissions(["@search"], spec["name"])
    save(service, [{"section": "allowedTools", "id": "@search", "action": "remove"}])
    assert spec_for(home, specs)["permissions"] == {"rules": []}


def test_policy_withheld_approval_never_resurrects_on_relaxation(editor):
    from dataclasses import replace

    from kiro_crew.platform.context import current_context, set_context
    from kiro_crew.platform.governance import parse_policy

    service, home, specs, parent = editor
    parent["allowedTools"] = ["@search"]
    (specs / "parent.json").write_text(json.dumps(parent))
    original = current_context()
    policy = parse_policy(
        {"version": 1, "boot": {"fail_closed": True}, "mcp": {"mode": "deny", "deny": ["@search"]}}
    )
    try:
        set_context(replace(original, governance=policy))
        save(service, enroll=True)
        assert spec_for(home, specs)["allowedTools"] == []
        set_context(original)
        agent._refresh_forked_templates()
        assert spec_for(home, specs)["allowedTools"] == []
    finally:
        set_context(original)


def test_project_parent_is_pinned_and_never_falls_back(editor, monkeypatch):
    service, home, specs, parent = editor
    project = home / "project"
    project_specs = project / ".kiro" / "agents"
    project_specs.mkdir(parents=True)
    project_parent = {**parent, "prompt": "project parent"}
    (project_specs / "parent.json").write_text(json.dumps(project_parent))
    config = json.loads((home / "config.json").read_text())
    config["workspaces"] = {"default": {"dir": str(project)}}
    (home / "config.json").write_text(json.dumps(config))
    monkeypatch.setattr(
        "kiro_crew.agent_capabilities.default_project_dir", loader.default_project_dir
    )
    loader._invalidate_config_cache()
    assert service.get("A")["template"]["scope"] == "project"
    save(service, enroll=True)
    assert spec_for(home, specs)["prompt"] == "project parent"
    (project_specs / "parent.json").unlink()
    assert service.get("A")["template"]["error_code"] == "parent_identity_changed"
    with pytest.raises(CapabilityError, match="parent_identity_changed"):
        save(service)
    assert spec_for(home, specs)["prompt"] == "project parent"


@pytest.mark.parametrize("junk", ["{", "[]", "null"])
def test_unrelated_invalid_spec_does_not_block_resolution(editor, junk):
    service, _, specs, _ = editor
    (specs / "unrelated.json").write_text(junk)
    assert service.get("A")["template"]["name"] == "parent"
    save(service, enroll=True)
    assert service.get("A")["mode"] == "inherited"


def test_duplicate_declared_parent_still_refuses(editor):
    service, _, specs, parent = editor
    (specs / "duplicate.json").write_text(json.dumps(parent))
    with pytest.raises(CapabilityError, match="ambiguous_template_name"):
        service.get("A")


def test_unmasked_transport_roundtrip_keeps_arguments(editor):
    service, home, specs, parent = editor
    parent["mcpServers"]["search"].pop("env")
    (specs / "parent.json").write_text(json.dumps(parent))
    row = next(r for r in service.get("A")["rows"] if r["section"] == "mcpServers")
    assert row["value"]["args"] == ["old"]
    value = {**row["value"], "command": "new-command"}
    save(
        service,
        [{"section": "mcpServers", "id": "search", "action": "set", "value": value}],
        enroll=True,
    )
    assert spec_for(home, specs)["mcpServers"]["search"] == {
        "command": "new-command",
        "args": ["old"],
    }


def test_redacted_transport_is_never_written_as_placeholders(editor):
    service, _, specs, parent = editor
    parent["mcpServers"]["search"]["args"] = ["--key", "opaque-secret"]
    (specs / "parent.json").write_text(json.dumps(parent))
    row = next(r for r in service.get("A")["rows"] if r["section"] == "mcpServers")
    assert row["value"]["env"] == {"API_KEY": "[REDACTED]"}
    assert row["value"]["args"] == ["--key", "[REDACTED]"]
    with pytest.raises(CapabilityError, match="redacted_value_not_writable"):
        save(
            service,
            [
                {
                    "section": "mcpServers",
                    "id": "search",
                    "action": "set",
                    "value": {**row["value"], "command": "changed"},
                }
            ],
            enroll=True,
        )
    assert json.loads((specs / "parent.json").read_text()) == parent


def test_existing_connection_preserves_secret_transport_fields(editor):
    _, home, specs, parent = editor
    service = CapabilityService(connections=lambda: {"existing": parent["mcpServers"]["search"]})
    save(
        service,
        [{"section": "mcpServers", "id": "search", "action": "set", "connection_id": "existing"}],
        enroll=True,
    )
    assert spec_for(home, specs)["mcpServers"]["search"] == parent["mcpServers"]["search"]


@pytest.mark.parametrize("failed_step", ["spec", "receipt"])
def test_failed_reconciliation_recovers_on_retry(editor, monkeypatch, failed_step):
    import kiro_crew.agent_capabilities as capabilities

    service, home, specs, parent = editor
    save(service, enroll=True)
    old = spec_for(home, specs)
    parent["prompt"] = "new parent prompt"
    (specs / "parent.json").write_text(json.dumps(parent))
    with monkeypatch.context() as patch:
        if failed_step == "spec":

            def fail(*args, **kwargs):
                raise OSError("test storage failure")

            patch.setattr(capabilities, "atomic_write", fail)
        else:
            real_write = agent_state._write
            count = 0

            def fail_receipt(data):
                nonlocal count
                count += 1
                if count == 2:
                    raise OSError("test receipt failure")
                real_write(data)

            patch.setattr(agent_state, "_write", fail_receipt)
        with pytest.raises(OSError):
            capabilities.reconcile_member_capabilities("A")
    with pytest.raises(CapabilityError, match="materialization_pending"):
        prepare_member_capabilities("A")
    assert json.loads((specs / (old["name"] + ".json")).read_text()) == old
    if failed_step == "spec":
        assert spec_for(home, specs)["name"] == old["name"]
    else:
        assert spec_for(home, specs)["name"] != old["name"]
    before_retry = set(specs.glob("*.json"))
    capabilities.reconcile_member_capabilities("A")
    if failed_step == "receipt":
        assert set(specs.glob("*.json")) == before_retry
    assert spec_for(home, specs)["prompt"] == "new parent prompt"
    assert prepare_member_capabilities("A")["status"] == "unverified"


def test_legacy_enrollment_preserves_all_noncapability_and_absent_fields(editor):
    service, home, specs, parent = editor
    private = {
        "name": "private",
        "prompt": None,
        "model": None,
        "includeMcpJson": True,
        "hooks": {"agentSpawn": [{"command": "custom-hook"}]},
        "description": "private description",
        "toolsSettings": {"read": {"setting": "custom"}},
    }
    (specs / "private.json").write_text(json.dumps(private))
    agent_state.set_fork_info("private", "parent", "A")
    config = json.loads((home / "config.json").read_text())
    config["agents"]["A"]["kiro_agent"] = "private"
    (home / "config.json").write_text(json.dumps(config))
    loader._invalidate_config_cache()
    save(service, enroll=True)
    effective = spec_for(home, specs)
    assert {k: v for k, v in effective.items() if k != "name"} == {
        k: v for k, v in private.items() if k != "name"
    }
    assert json.loads((specs / "private.json").read_text()) == private


def test_shared_enrollment_keeps_implicit_global_scope_unchanged(editor):
    service, home, specs, parent = editor
    parent.pop("includeMcpJson")
    (specs / "parent.json").write_text(json.dumps(parent))
    save(service, enroll=True)
    effective = spec_for(home, specs)
    assert {k: v for k, v in effective.items() if k != "name"} == {
        k: v for k, v in parent.items() if k != "name"
    }
    with pytest.raises(CapabilityError, match="global_mcp_exclusion_unrepresentable"):
        save(service, [{"section": "tools", "id": "read", "action": "remove"}])


def test_preview_and_publication_use_identical_governance_without_preview_audit(
    editor, monkeypatch
):
    from copy import deepcopy
    from dataclasses import replace

    from kiro_crew.platform import governance
    from kiro_crew.platform.context import current_context, set_context

    service, _, specs, parent = editor
    parent["allowedTools"] = ["@search"]
    parent["mcpServers"]["search"]["autoApprove"] = ["find"]
    (specs / "parent.json").write_text(json.dumps(parent))
    original = current_context()
    policy = governance.parse_policy(
        {"version": 1, "boot": {"fail_closed": True}, "mcp": {"mode": "deny", "deny": ["@search"]}}
    )
    try:
        set_context(replace(original, governance=policy))
        actual = deepcopy(parent)
        governance.sanitize_agent_config_governance(actual)
        audit_calls = []

        def audit_forbidden():
            audit_calls.append(True)
            raise AssertionError("preview must not emit an audit")

        with monkeypatch.context() as patch:
            patch.setattr(governance, "sel", audit_forbidden)
            projected = deepcopy(parent)
            governance.sanitize_agent_config_governance(projected, audit=False)
            assert projected == actual
            view = service.get("A")
            preview = service.preview("A", {"revision": view["revision"], "enroll": True})
            assert not any(
                r["section"] in ("allowedTools", "autoApprove") and r["present"]
                for r in preview["rows"]
            )
            assert audit_calls == []
    finally:
        set_context(original)


@pytest.mark.parametrize("connection_id", [[], {}, True, None, ""])
def test_connection_identity_requires_a_nonempty_string(connection_id):
    with pytest.raises(CapabilityError, match="invalid_connection_id"):
        validate_request(
            {
                "revision": "r",
                "operations": [
                    {
                        "section": "mcpServers",
                        "id": "search",
                        "action": "set",
                        "connection_id": connection_id,
                    }
                ],
            }
        )


def test_retain_paths_preserve_secrets_while_replacing_command(editor):
    service, home, specs, parent = editor
    parent["mcpServers"]["search"]["env"] = {"key/a~b": "opaque-secret"}
    (specs / "parent.json").write_text(json.dumps(parent))
    row = next(r for r in service.get("A")["rows"] if r["section"] == "mcpServers")
    value = {**row["value"], "command": "replacement"}
    save(
        service,
        [
            {
                "section": "mcpServers",
                "id": "search",
                "action": "set",
                "value": value,
                "retain_paths": ["/env/key~1a~0b"],
            }
        ],
        enroll=True,
    )
    assert spec_for(home, specs)["mcpServers"]["search"] == {
        "command": "replacement",
        "args": ["old"],
        "env": {"key/a~b": "opaque-secret"},
    }
    assert "opaque-secret" not in json.dumps(service.get("A"))


@pytest.mark.parametrize(
    "paths",
    [
        [""],
        ["/env/API_KEY", "/env/API_KEY"],
        ["/env", "/env/API_KEY"],
        ["/env/~2"],
        ["/args/99"],
        ["/command"],
        [True],
        "not-list",
        [],
    ],
)
def test_invalid_retain_paths_write_nothing(editor, paths):
    service, home, specs, parent = editor
    row = next(r for r in service.get("A")["rows"] if r["section"] == "mcpServers")
    with pytest.raises(CapabilityError):
        save(
            service,
            [
                {
                    "section": "mcpServers",
                    "id": "search",
                    "action": "set",
                    "value": row["value"],
                    "retain_paths": paths,
                }
            ],
            enroll=True,
        )
    assert not (home / "agent_model_state.json").exists()
    assert json.loads((specs / "parent.json").read_text()) == parent


def test_absent_scalars_are_editable_and_managed_rows_identify_ownership(editor):
    service, _, specs, parent = editor
    parent.pop("prompt")
    parent["mcpServers"]["kirocrew-core"] = {"command": "system", "args": []}
    (specs / "parent.json").write_text(json.dumps(parent))
    rows = service.get("A")["rows"]
    for section in ("prompt", "model"):
        row = next(r for r in rows if r["section"] == section)
        assert not row["present"] and row["editable"] and row["value"] == ""
    row = next(r for r in rows if r["id"] == "kirocrew-core")
    assert row["managed"] and row["locked_reason"] == "managed_transport_fields"


def test_local_overlay_member_saves_in_overlay_and_preserves_base(editor):
    service, home, specs, _ = editor
    service.get("A")
    base = (home / "config.json").read_bytes()
    local = home / "config.local.json"
    local.write_text(
        json.dumps(
            {
                "agents": {"A": {"description": "local member"}},
                "dashboard": {"bot_name": "local name"},
            }
        )
    )
    loader._invalidate_config_cache()
    save(service, enroll=True)
    assert (home / "config.json").read_bytes() == base
    overlay = json.loads(local.read_text())
    assert overlay["dashboard"]["bot_name"] == "local name"
    assert overlay["agents"]["A"]["description"] == "local member"
    target = overlay["agents"]["A"]["kiro_agent"]
    assert target.startswith("crew-") and (specs / (target + ".json")).is_file()
    assert service.get("A")["mode"] == "inherited"
    service.reset("A", target)
    current = loader.KiroCrewConfig.load().agents["A"].kiro_agent
    service.publish("A", current, "from-local")
    assert loader.KiroCrewConfig.load().agents["A"].kiro_agent == "from-local"
    assert (home / "config.json").read_bytes() == base


def test_reconcile_changes_ordinary_parent_fields_in_new_generation_only(editor):
    from kiro_crew.agent_capabilities import reconcile_member_capabilities

    service, home, specs, parent = editor
    parent["description"] = "before"
    (specs / "parent.json").write_text(json.dumps(parent))
    save(service, enroll=True)
    old = spec_for(home, specs)
    old_path = specs / (old["name"] + ".json")
    old_bytes = old_path.read_bytes()
    files = set(specs.glob("*.json"))
    reconcile_member_capabilities("A")
    assert set(specs.glob("*.json")) == files
    assert spec_for(home, specs)["name"] == old["name"]
    parent["description"] = "after"
    (specs / "parent.json").write_text(json.dumps(parent))
    reconcile_member_capabilities("A")
    assert old_path.read_bytes() == old_bytes
    assert spec_for(home, specs)["name"] != old["name"]
    assert spec_for(home, specs)["description"] == "after"
    assert agent_state.get_fork_info(old["name"])["private_to"] == "A"


def test_uninstalled_app_namespace_cannot_be_forged(editor):
    service, _, _, _ = editor
    with pytest.raises(CapabilityError, match="app_transport_unavailable"):
        save(
            service,
            [
                {
                    "section": "mcpServers",
                    "id": "missing-app:bridge",
                    "action": "set",
                    "value": {"command": "fake"},
                }
            ],
            enroll=True,
        )


def test_publish_respects_reserved_legacy_default_binding(editor):
    service, home, specs, _ = editor
    save(service, enroll=True)
    config = json.loads((home / "config.json").read_text())
    config.setdefault("agent", {})["default_agent"] = "reserved-template"
    (home / "config.json").write_text(json.dumps(config))
    loader._invalidate_config_cache()
    with pytest.raises(CapabilityError, match="name_taken"):
        service.publish("A", spec_for(home, specs)["name"], "reserved-template")


def test_owned_parent_refreshes_real_plumbing_without_regranting_tools(editor, monkeypatch):
    import sys

    from kiro_crew.agent_capabilities import reconcile_member_capabilities

    service, home, specs, parent = editor
    parent.update(name="kirocrew", prompt="owner prompt", model="owner-selected-model")
    parent["mcpServers"] = {
        "kirocrew-core": {
            "command": "outdated",
            "args": ["old"],
            "env": {"KIROCREW_HOME": "outdated", "KEEP": "custom"},
        }
    }
    parent["hooks"] = {"agentSpawn": [{"command": "outdated"}]}
    (specs / "kirocrew.json").write_text(json.dumps(parent))
    config = json.loads((home / "config.json").read_text())
    config["agents"]["A"]["kiro_agent"] = "kirocrew"
    (home / "config.json").write_text(json.dumps(config))
    loader._invalidate_config_cache()
    monkeypatch.setattr(
        agent,
        "_MANAGED_MCP_SERVERS",
        {
            "kirocrew-core": {"command": sys.executable, "args": ["mcp-core"]},
            "kirocrew-cron": {"command": sys.executable, "args": ["mcp-cron"]},
        },
    )
    save(service, enroll=True)
    first = spec_for(home, specs)
    assert first["prompt"] == "owner prompt"
    assert first["model"] == "owner-selected-model"
    assert first["tools"] == ["read"]
    assert set(first["mcpServers"]) == {"kirocrew-core"}
    assert first["mcpServers"]["kirocrew-core"]["command"] == sys.executable
    assert first["mcpServers"]["kirocrew-core"]["env"]["KIROCREW_HOME"] == str(home)
    assert first["mcpServers"]["kirocrew-core"]["env"]["KEEP"] == "custom"
    assert first["hooks"] != parent["hooks"]
    path = specs / (first["name"] + ".json")
    old_bytes = path.read_bytes()
    monkeypatch.setattr(
        agent,
        "_MANAGED_MCP_SERVERS",
        {"kirocrew-core": {"command": sys.executable, "args": ["mcp-core", "--new"]}},
    )
    reconcile_member_capabilities("A")
    assert path.read_bytes() == old_bytes
    assert spec_for(home, specs)["mcpServers"]["kirocrew-core"]["args"] == ["mcp-core", "--new"]


def test_retained_secret_is_bound_to_member_revision(editor):
    service, _, specs, parent = editor
    current = service.get("A")
    row = next(r for r in current["rows"] if r["section"] == "mcpServers")
    body = {
        "revision": current["revision"],
        "enroll": True,
        "operations": [
            {
                "section": "mcpServers",
                "id": "search",
                "action": "set",
                "value": {**row["value"], "command": "new"},
                "retain_paths": ["/env/API_KEY"],
            }
        ],
    }
    preview = service.preview("A", body)
    parent["mcpServers"]["search"]["env"]["API_KEY"] = "rotated-secret"
    (specs / "parent.json").write_text(json.dumps(parent))
    with pytest.raises(CapabilityError, match="stale_revision"):
        service.put("A", {**body, "preview_token": preview["preview_token"]})


@pytest.mark.parametrize(
    "section,action", [("prompt", "set"), ("mcpServers", "inherit"), ("tools", "remove")]
)
def test_retain_paths_only_belong_to_transport_set(section, action):
    with pytest.raises(CapabilityError, match="invalid_retain_paths"):
        validate_request(
            {
                "revision": "r",
                "operations": [
                    {
                        "section": section,
                        "id": "x",
                        "action": action,
                        "value": {},
                        "retain_paths": [],
                    }
                ],
            }
        )


def test_mixed_layer_parent_acceptance_is_one_overlay_delta(editor):
    service, home, specs, parent = editor
    save(service, enroll=True)
    body = {"revision": service.get("B")["revision"], "enroll": True}
    preview = service.preview("B", body)
    service.put("B", {**body, "preview_token": preview["preview_token"]})
    local = home / "config.local.json"
    local.write_text(json.dumps({"agents": {"B": {"description": "local"}}}))
    loader._invalidate_config_cache()
    parent["tools"].append("write")
    (specs / "parent.json").write_text(json.dumps(parent))
    save(service, accept_parent=[{"section": "tools", "id": "write"}], accept_members=["B"])
    cfg = loader.KiroCrewConfig.load()
    for member in ("A", "B"):
        effective = json.loads((specs / (cfg.agents[member].kiro_agent + ".json")).read_text())
        assert effective["tools"] == ["read", "write"]
    assert set(json.loads(local.read_text())["agents"]) == {"A", "B"}


def test_prepare_detects_governance_change_during_spec_read(editor, monkeypatch):
    import kiro_crew.agent_capabilities as capabilities
    from kiro_crew.platform.context import current_context, set_context

    service, home, specs, _ = editor
    save(service, enroll=True)
    name = spec_for(home, specs)["name"]
    real_read = capabilities._read_spec
    original = current_context()

    def read_then_change(path):
        result = real_read(path)
        if path.name == name + ".json":
            set_context(original)
        return result

    monkeypatch.setattr(capabilities, "_read_spec", read_then_change)
    with pytest.raises(CapabilityError, match="governance_reconciliation_pending"):
        prepare_member_capabilities("A")


@pytest.mark.parametrize("stage", ["receipt", "spec", "binding", "completion", "crash"])
def test_publish_interruption_is_recoverable_with_same_name(editor, monkeypatch, stage):
    import kiro_crew.agent_capabilities as capabilities

    class Interrupted(BaseException):
        pass

    service, home, specs, _ = editor
    save(service, enroll=True)
    source = spec_for(home, specs)
    original_config = json.loads((home / "config.json").read_text())
    source_bytes = (specs / (source["name"] + ".json")).read_bytes()
    with monkeypatch.context() as patch:
        if stage in ("receipt", "completion", "crash"):
            write = agent_state._write
            calls = 0

            def fail_write(data):
                nonlocal calls
                calls += 1
                if calls == (1 if stage == "receipt" else 2):
                    if stage == "crash":
                        raise Interrupted()
                    raise OSError("test publication storage failure")
                write(data)

            patch.setattr(agent_state, "_write", fail_write)
        elif stage == "spec":

            def fail_spec(*args, **kwargs):
                raise OSError("test spec storage failure")

            patch.setattr(capabilities, "atomic_write", fail_spec)
        else:

            def fail_binding(*args, **kwargs):
                raise OSError("test config storage failure")

            patch.setattr(loader, "write_config_atomically", fail_binding)
        if stage == "completion":
            result = service.publish("A", source["name"], "published")
            assert result["warning"] == "publish_incomplete"
        else:
            with pytest.raises(Interrupted if stage == "crash" else OSError):
                service.publish("A", source["name"], "published")
    current_config = json.loads((home / "config.json").read_text())
    if stage in ("completion", "crash"):
        assert current_config["agents"]["A"]["kiro_agent"] == "published"
    else:
        assert current_config == original_config
    if stage != "receipt":
        assert agent_state.get_fork_info("published")["private_to"] == "A"
        assert agent_state.get_publish_info("published")["source"] == source["name"]
    assert (specs / (source["name"] + ".json")).read_bytes() == source_bytes
    # A new service simulates a restarted gateway with no in-memory receipt/key.
    retry = CapabilityService()
    result = retry.publish("A", source["name"], "published")
    assert result == {"ok": True, "template": "published", "filename": "published.json"}
    assert agent_state.get_fork_info("published") is None
    published = spec_for(home, specs)
    assert {k: v for k, v in published.items() if k != "name"} == {
        k: v for k, v in source.items() if k != "name"
    }
    config = json.loads((home / "config.json").read_text())
    assert config["agents"]["B"] == original_config["agents"]["B"]
    assert {k: v for k, v in config["agents"]["A"].items() if k != "kiro_agent"} == {
        k: v for k, v in original_config["agents"]["A"].items() if k != "kiro_agent"
    }
    before = {p: p.read_bytes() for p in home.rglob("*") if p.is_file()}
    assert retry.publish("A", "published", "published")["ok"]
    assert {p: p.read_bytes() for p in home.rglob("*") if p.is_file()} == before


@pytest.mark.parametrize("change", ["source", "destination", "binding", "owner"])
def test_publish_retry_does_not_override_a_concurrent_change(editor, monkeypatch, change):
    service, home, specs, _ = editor
    save(service, enroll=True)
    source = spec_for(home, specs)
    with monkeypatch.context() as patch:

        def fail_binding(*args, **kwargs):
            raise OSError("test config failure")

        patch.setattr(loader, "write_config_atomically", fail_binding)
        with pytest.raises(OSError):
            service.publish("A", source["name"], "published")
    if change in ("source", "destination"):
        path = specs / ((source["name"] if change == "source" else "published") + ".json")
        edited = json.loads(path.read_text())
        edited["prompt"] = "a concurrent owner edit"
        path.write_text(json.dumps(edited))
    elif change == "owner":
        agent_state.set_fork_info("published", "parent", "B")
    else:

        def rebind(document):
            document["agents"]["A"]["kiro_agent"] = "parent"
            return document

        loader.update_config_locked(mutate=rebind)
    before = {p: p.read_bytes() for p in home.rglob("*") if p.is_file()}
    with pytest.raises(CapabilityError):
        service.publish("A", source["name"], "published")
    assert {p: p.read_bytes() for p in home.rglob("*") if p.is_file()} == before
    assert agent_state.get_fork_info("published") is not None


def test_publish_rechecks_binding_at_finalization(editor, monkeypatch):
    service, home, specs, _ = editor
    save(service, enroll=True)
    source = spec_for(home, specs)
    finish = service._finish_publish

    def rebind_then_finish(member, expected, name):
        def rebind(document):
            document["agents"][member]["kiro_agent"] = "parent"
            return document

        loader.update_config_locked(mutate=rebind)
        return finish(member, expected, name)

    monkeypatch.setattr(service, "_finish_publish", rebind_then_finish)
    with pytest.raises(CapabilityError, match="stale_binding"):
        service.publish("A", source["name"], "published")
    assert spec_for(home, specs)["name"] == "parent"
    assert agent_state.get_fork_info("published")["private_to"] == "A"


def test_publish_completion_preserves_other_sidecar_metadata(editor, monkeypatch):
    service, _, specs, _ = editor
    save(service, enroll=True)
    source = prepare_member_capabilities("A")["template"]
    finish = service._finish_publish

    def add_metadata_then_finish(member, expected, name):
        agent_state.set_model_managed(name, False)
        agent_state.set_cc_model(name, "owner-selection")
        return finish(member, expected, name)

    monkeypatch.setattr(service, "_finish_publish", add_metadata_then_finish)
    service.publish("A", source, "published")
    assert agent_state.get_model_managed("published") is False
    assert agent_state.get_cc_model("published") == "owner-selection"
    assert agent_state.get_fork_info("published") is None


@pytest.mark.asyncio
async def test_publish_http_retries_pending_and_completed_receipts(editor, monkeypatch):
    from types import SimpleNamespace

    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    from kiro_crew.dashboard.handlers.agent_capabilities import _SERVICE
    from kiro_crew.dashboard.routes.agents import register

    service, home, specs, _ = editor
    await asyncio.to_thread(save, service, enroll=True)
    source = (await asyncio.to_thread(spec_for, home, specs))["name"]

    @web.middleware
    async def owner(request, handler):
        request["user"], request["app"] = "owner", ""
        return await handler(request)

    app = web.Application(middlewares=[owner])
    app["state"] = SimpleNamespace(owner_id="owner", push_refresh=lambda _: None)
    app[_SERVICE] = service
    register(app)
    write = agent_state._write
    calls = 0

    def fail_completion_once(data):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("test completion failure")
        write(data)

    async with TestClient(TestServer(app)) as client:
        with monkeypatch.context() as patch:
            patch.setattr(agent_state, "_write", fail_completion_once)
            response = await client.post(
                f"/api/agents/detail/{source}/publish", json={"crew": "A", "name": "published"}
            )
            assert response.status == 200
            assert (await response.json())["warning"] == "publish_incomplete"
        for target in ("published", source, "published"):
            response = await client.post(
                f"/api/agents/detail/{target}/publish", json={"crew": "A", "name": "published"}
            )
            assert response.status == 200
            assert await response.json() == {
                "ok": True,
                "template": "published",
                "filename": "published.json",
            }
        response = await client.get("/api/agents/A/capabilities")
        assert (await response.json())["mode"] == "shared"


def test_publish_rechecks_binding_before_staging(editor, monkeypatch):
    service, home, specs, _ = editor
    save(service, enroll=True)
    source = spec_for(home, specs)["name"]
    publish_bindings = service._write_bindings

    def race_rebind(prepared, mutate):
        def rebind(document):
            document["agents"]["A"]["kiro_agent"] = "parent"
            return document

        loader.update_config_locked(mutate=rebind)
        return publish_bindings(prepared, mutate)

    monkeypatch.setattr(service, "_write_bindings", race_rebind)
    with pytest.raises(CapabilityError, match="stale_binding"):
        service.publish("A", source, "published")
    assert not (specs / "published.json").exists()
    assert agent_state.get_publish_info("published") is None
    assert spec_for(home, specs)["name"] == "parent"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path,replacement,delete",
    [
        *(
            (("capabilities", field, section), None, True)
            for field in ("accepted", "overrides")
            for section in agent_state.CAPABILITY_SECTIONS
        ),
        *(
            (("capabilities", field, section), [], False)
            for field in ("accepted", "overrides")
            for section in agent_state.CAPABILITY_SECTIONS
        ),
        *((("capabilities", field), {}, False) for field in ("accepted", "overrides")),
        *(
            (
                ("capabilities", field, "mcpServers"),
                {
                    "search": (
                        transport if field == "accepted" else {"action": "set", "value": transport}
                    )
                },
                False,
            )
            for field in ("accepted", "overrides")
            for transport in (
                {"command": []},
                {"command": ""},
                {"url": []},
                {"url": ""},
                {"args": "--read"},
                {"args": [1]},
                {"env": []},
                {"env": {"TOKEN": []}},
                {"headers": []},
                {"headers": {"Authorization": 1}},
                {"type": False},
                {"type": ""},
                {"timeout": "30"},
                {"timeout": True},
                {"timeout": 0},
                {"timeout": -1},
                {"timeout": float("nan")},
                {"timeout": float("inf")},
                {"timeout": float("-inf")},
                {"disabled": "false"},
                {"disabledTools": "read"},
                {"disabledTools": [1]},
                {"oauthScopes": "read"},
                {"oauthScopes": [1]},
                {"oauth": []},
                {"oauth": {"clientId": []}},
                {"oauth": {"clientSecret": False}},
                {"oauth": {"redirectUri": []}},
                {"oauth": {"clientMetadataUrl": 1}},
                {"oauth": {"oauthScopes": "read"}},
                {"oauth": {"oauthScopes": [1]}},
            )
        ),
        *(
            (
                ("capabilities", field, section),
                {key: value if field == "accepted" else {"action": "set", "value": value}},
                False,
            )
            for field in ("accepted", "overrides")
            for section, key, value in (
                ("mcpServers", "search", []),
                ("mcpServers", "search", {"command": "search", "autoApprove": ["read"]}),
                ("resources", "file://guide.md", "file://different.md"),
                ("tools", "read", False),
                ("allowedTools", "read", 1),
                ("autoApprove", "@search/read", "true"),
                ("skills", "catalog/skill", True),
                ("prompt", "prompt", None),
                ("model", "model", {}),
                ("resources", "file://guide.md", True),
            )
        ),
        *(
            (("capabilities", "overrides", "tools"), {"read": override}, False)
            for override in (
                None,
                [],
                {},
                {"action": "inherit"},
                {"action": "unknown"},
                {"action": "set"},
                {"action": "remove", "value": True},
            )
        ),
        (("capabilities", "accepted", "unknown"), {}, False),
        (("capabilities", "overrides", "unknown"), {}, False),
        (("capabilities", "accepted", "prompt"), {"wrong": "text"}, False),
        (("capabilities", "overrides", "model"), {"wrong": {"action": "remove"}}, False),
        (("capabilities",), None, False),
        ((), None, False),
        ((), [], False),
    ],
)
async def test_nested_corruption_fails_closed_without_writes(editor, path, replacement, delete):
    from types import SimpleNamespace

    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    from kiro_crew.dashboard.handlers.agent_capabilities import _SERVICE
    from kiro_crew.dashboard.routes.agents import register

    service, home, specs, _ = editor

    def corrupt():
        saved = save(service, enroll=True)
        name = spec_for(home, specs)["name"]
        state_path = home / "agent_model_state.json"
        state = json.loads(state_path.read_text())
        # Start with the real writer's schema, not a hand-built legacy format.
        assert agent_state.get_capabilities(name) == state[name]["capabilities"]
        target = state
        keys = (name, *path)
        for key in keys[:-1]:
            target = target[key]
        if delete:
            del target[keys[-1]]
        else:
            target[keys[-1]] = replacement
        state_path.write_text(json.dumps(state))
        before = {p: p.read_bytes() for p in home.rglob("*") if p.is_file()}
        with pytest.raises(ValueError, match="^capability_state_invalid$"):
            agent_state.get_capabilities(name)
        with pytest.raises(ValueError, match="^capability_state_invalid$"):
            service.get("A")
        from kiro_crew.agent_capabilities import reconcile_member_capabilities

        with pytest.raises(ValueError, match="^capability_state_invalid$"):
            reconcile_member_capabilities("A")
        return saved["revision"], before

    revision, before = await asyncio.to_thread(corrupt)

    @web.middleware
    async def identity(request, handler):
        request["user"] = "owner"
        request["app"] = ""
        return await handler(request)

    app = web.Application(middlewares=[identity])
    app["state"] = SimpleNamespace(owner_id="owner", push_refresh=lambda _: None)
    app[_SERVICE] = service
    register(app)
    async with TestClient(TestServer(app)) as client:
        for method, suffix, body in (
            ("get", "", None),
            ("post", "/preview", {"revision": revision}),
            ("put", "", {"revision": revision, "preview_token": "untrusted"}),
        ):
            response = await getattr(client, method)(
                "/api/agents/A/capabilities" + suffix, json=body
            )
            assert response.status == 503
            assert await response.json() == {
                "error": "capabilities_unavailable",
                "code": "capabilities_unavailable",
            }
    after = await asyncio.to_thread(
        lambda: {p: p.read_bytes() for p in home.rglob("*") if p.is_file()}
    )
    assert after == before


@pytest.mark.parametrize("legacy", [False, True])
@pytest.mark.parametrize(
    "transport",
    [
        {
            "command": "search",
            "args": ["--read"],
            "env": {"MODE": "read"},
            "type": "stdio",
            "timeout": 30,
            "disabled": False,
            "disabledTools": ["write"],
        },
        {
            "url": "https://example.test/mcp",
            "headers": {"Authorization": "opaque-secret"},
            "timeout": 0.5,
            "oauthScopes": [],
            "oauth": {
                "clientId": "client",
                "clientSecret": "opaque-secret",
                "redirectUri": "http://localhost/callback",
                "clientMetadataUrl": "https://example.test/client.json",
                "oauthScopes": ["read"],
                "issuer": "https://example.test",
            },
        },
        {"command": "search", "mountOnly": True},
        {"disabled": True},
        {"type": "registry"},
        {},
    ],
)
def test_source_transport_fields_survive_enrollment_and_reconciliation(editor, legacy, transport):
    from kiro_crew.agent_capabilities import reconcile_member_capabilities

    service, home, specs, parent = editor
    parent["mcpServers"] = {"search": {**transport, "autoApprove": ["read"]}}
    (specs / "parent.json").write_text(json.dumps(parent))
    if legacy:
        (specs / "private.json").write_text(json.dumps({**parent, "name": "private"}))
        agent_state.set_fork_info("private", "parent", "A")
        config = json.loads((home / "config.json").read_text())
        config["agents"]["A"]["kiro_agent"] = "private"
        (home / "config.json").write_text(json.dumps(config))
        loader._invalidate_config_cache()
    save(service, enroll=True)
    first = spec_for(home, specs)
    intent = agent_state.get_capabilities(first["name"])
    assert intent["accepted"]["mcpServers"]["search"] == transport
    assert intent["accepted"]["autoApprove"]["@search/read"] is True
    if legacy:
        assert intent["overrides"]["mcpServers"]["search"] == {"action": "set", "value": transport}
    reconcile_member_capabilities("A")
    assert spec_for(home, specs)["mcpServers"]["search"] == first["mcpServers"]["search"]
    row = next(row for row in service.get("A")["rows"] if row["section"] == "mcpServers")
    assert row["value"].get("headers", {}) == {
        key: "[REDACTED]" for key in transport.get("headers", {})
    }
    assert "opaque-secret" not in json.dumps(service.get("A"))


@pytest.mark.parametrize("name", ["kirocrew-core", "test-app:search"])
def test_resolved_managed_and_app_transports_keep_metadata(editor, monkeypatch, name):
    service, home, specs, parent = editor
    transport = {"command": "search", "args": [], "env": {"MODE": "read"}, "mountOnly": True}
    parent["mcpServers"] = {name: transport}
    (specs / "parent.json").write_text(json.dumps(parent))
    monkeypatch.setattr(agent, "_collect_app_mcp_servers", lambda **kwargs: {name: transport})
    save(
        service,
        [{"section": "mcpServers", "id": name, "action": "set", "value": {"disabled": True}}],
        enroll=True,
    )
    spec = spec_for(home, specs)
    expected = {**transport, "disabled": True}
    intent = agent_state.get_capabilities(spec["name"])
    assert intent["overrides"]["mcpServers"][name]["value"] == expected
    assert spec["mcpServers"][name] == expected
    with pytest.raises(CapabilityError, match="managed_transport_locked|app_transport_locked"):
        save(
            service,
            [
                {
                    "section": "mcpServers",
                    "id": name,
                    "action": "set",
                    "value": {"command": "replacement"},
                }
            ],
        )


@pytest.mark.parametrize("timeout", [float("nan"), float("inf"), float("-inf")])
def test_editor_rejects_nonfinite_transport_timeout_without_writes(editor, timeout):
    service, home, _, _ = editor
    body = {
        "revision": service.get("A")["revision"],
        "enroll": True,
        "operations": [
            {
                "section": "mcpServers",
                "id": "search",
                "action": "set",
                "value": {"command": "search", "timeout": timeout},
            }
        ],
    }
    before = {p: p.read_bytes() for p in home.rglob("*") if p.is_file()}
    with pytest.raises(CapabilityError, match="^invalid_body$"):
        service.preview("A", body)
    assert {p: p.read_bytes() for p in home.rglob("*") if p.is_file()} == before


@pytest.mark.asyncio
@pytest.mark.parametrize("secret", ["opaque-oauth-credential", "q7!", ""])
async def test_oauth_secret_http_projection_and_retention(editor, secret):
    from types import SimpleNamespace

    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    from kiro_crew.dashboard.handlers.agent_capabilities import _SERVICE
    from kiro_crew.dashboard.routes.agents import register

    _, home, specs, parent = editor
    configured_secret = "configured-oauth-credential"
    rotated_secret = "r8?" if secret == "q7!" else "rotated-oauth-credential"
    service = CapabilityService(
        connections=lambda: {
            "configured": {
                "url": "https://example.test/mcp",
                "oauth": {"clientSecret": configured_secret},
            }
        }
    )
    original = {
        "command": "search",
        "args": ["--credential=" + secret],
        "oauth": {"clientId": "public-client", "clientSecret": secret},
    }

    def setup():
        parent["mcpServers"] = {"search": original}
        (specs / "parent.json").write_text(json.dumps(parent))

    await asyncio.to_thread(setup)

    @web.middleware
    async def identity(request, handler):
        request["user"] = "owner"
        request["app"] = ""
        return await handler(request)

    app = web.Application(middlewares=[identity])
    app["state"] = SimpleNamespace(owner_id="owner", push_refresh=lambda _: None)
    app[_SERVICE] = service
    register(app)
    async with TestClient(TestServer(app)) as client:

        async def view(method, suffix="", body=None):
            response = await getattr(client, method)(
                "/api/agents/A/capabilities" + suffix, json=body
            )
            assert response.status == 200
            data = await response.json()
            serialized = json.dumps(data)
            for credential in (secret, configured_secret, rotated_secret):
                if credential:
                    assert credential not in serialized
            assert str(home) not in serialized
            assert "source_digest" not in serialized
            return data

        current = await view("get")
        assert current["connections"] == [
            {"id": "configured", "label": "configured", "managed": False}
        ]
        row = next(row for row in current["rows"] if row["section"] == "mcpServers")
        assert row["value"]["oauth"]["clientSecret"] == ("[REDACTED]" if secret else "")
        assert row["value"]["oauth"]["clientId"] == "public-client"
        edited = {**row["value"], "command": "search-updated"}
        paths = ["/oauth/clientSecret", "/args/0"] if secret else []
        body = {
            "revision": current["revision"],
            "enroll": True,
            "operations": [
                {
                    "section": "mcpServers",
                    "id": "search",
                    "action": "set",
                    "value": edited,
                    "retain_paths": paths,
                }
            ],
        }
        preview = await view("post", "/preview", body)
        saved = await view("put", body={**body, "preview_token": preview["preview_token"]})
        actual = await asyncio.to_thread(spec_for, home, specs)
        assert actual["mcpServers"]["search"] == {**original, "command": "search-updated"}
        assert "[REDACTED]" not in json.dumps(actual)

        def change_parent():
            parent["mcpServers"]["search"] = {
                **original,
                "oauth": {"clientId": "public-client", "clientSecret": rotated_secret},
                "args": [rotated_secret],
                "metadata": {"nested": ["prefix:" + rotated_secret]},
            }
            (specs / "parent.json").write_text(json.dumps(parent))

        await asyncio.to_thread(change_parent)
        current = await view("get")
        change = next(
            change for change in current["parent_changes"] if change["section"] == "mcpServers"
        )
        assert change["conflict"] is True
        assert change["before"]["oauth"]["clientSecret"] == ("[REDACTED]" if secret else "")
        assert change["after"]["oauth"]["clientSecret"] == "[REDACTED]"
        assert change["after"]["metadata"] == {"nested": ["[REDACTED]"]}
        body = {
            "revision": current["revision"],
            "operations": [
                {
                    "section": "mcpServers",
                    "id": "configured",
                    "action": "set",
                    "connection_id": "configured",
                }
            ],
        }
        preview = await view("post", "/preview", body)
        saved = await view("put", body={**body, "preview_token": preview["preview_token"]})
        actual = await asyncio.to_thread(spec_for, home, specs)
        assert actual["mcpServers"]["search"]["oauth"]["clientSecret"] == secret
        assert actual["mcpServers"]["configured"]["oauth"]["clientSecret"] == configured_secret
        assert "[REDACTED]" not in json.dumps(actual)
        await view("get")

        # Retention still authorizes only a currently masked scalar leaf.
        for path in ("/oauth/clientId", "/oauth/missing", "/oauth/clientSecret~2", "/oauth"):
            value = {
                "command": "search",
                "oauth": {
                    "clientId": "[REDACTED]",
                    "missing": "[REDACTED]",
                    "clientSecret": "[REDACTED]",
                },
            }
            bad = {
                "revision": saved["revision"],
                "operations": [
                    {
                        "section": "mcpServers",
                        "id": "search",
                        "action": "set",
                        "value": value,
                        "retain_paths": [path],
                    }
                ],
            }
            before = await asyncio.to_thread(
                lambda: {p: p.read_bytes() for p in home.rglob("*") if p.is_file()}
            )
            response = await client.post("/api/agents/A/capabilities/preview", json=bad)
            assert response.status == 400
            assert (
                await asyncio.to_thread(
                    lambda: {p: p.read_bytes() for p in home.rglob("*") if p.is_file()}
                )
                == before
            )

        # A valid signed preview cannot retain an old credential after disk changes.
        current = await view("get")
        row = next(
            row
            for row in current["rows"]
            if row["section"] == "mcpServers" and row["id"] == "search"
        )
        body = {
            "revision": current["revision"],
            "operations": [
                {
                    "section": "mcpServers",
                    "id": "search",
                    "action": "set",
                    "value": {**row["value"], "command": "another-command"},
                    "retain_paths": paths,
                }
            ],
        }
        preview = await view("post", "/preview", body)
        await asyncio.to_thread(
            lambda: (specs / "parent.json").write_text(json.dumps({**parent, "prompt": "changed"}))
        )
        before = await asyncio.to_thread(
            lambda: {p: p.read_bytes() for p in home.rglob("*") if p.is_file()}
        )
        response = await client.put(
            "/api/agents/A/capabilities", json={**body, "preview_token": preview["preview_token"]}
        )
        assert response.status == 409
        assert (
            await asyncio.to_thread(
                lambda: {p: p.read_bytes() for p in home.rglob("*") if p.is_file()}
            )
            == before
        )
