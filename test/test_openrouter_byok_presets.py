import pytest
import json
from unittest.mock import MagicMock
from aiohttp import web
from kiro_crew.openrouter_byok import OpenRouterBYOKManager
from kiro_crew.dashboard.chat_handlers import _model_rejected_reason
from kiro_crew.dashboard.handlers.agents import _append_openrouter_presets, api_models
from kiro_crew.dashboard.handlers.core import _validate_role_model

def test_openrouter_preset_management(tmp_path):
    mgr = OpenRouterBYOKManager(base_dir=tmp_path)

    # Initially empty
    assert mgr.list_presets() == []
    assert mgr.find_preset("Fast Cron") is None

    # Add preset
    preset = mgr.add_preset(
        name="Fast Cron",
        key_id="or_key_123",
        model_name="anthropic/claude-3.5-sonnet",
        workspace_id="default"
    )
    assert preset["name"] == "Fast Cron"
    assert preset["model_name"] == "anthropic/claude-3.5-sonnet"

    # Find preset by name, id, or model_name
    assert mgr.find_preset("Fast Cron") is not None
    assert mgr.find_preset("anthropic/claude-3.5-sonnet") is not None
    assert mgr.find_preset("openrouter::anthropic/claude-3.5-sonnet") is not None
    assert mgr.find_preset(preset["id"]) is not None

    # Test resolve_model with preset
    # Case 1: task_override with preset model_name and empty key_id
    res_key, res_raw_key, res_model = mgr.resolve_model(
        task_override=("", "Fast Cron"),
        workspace_id="default",
        system_default="auto"
    )
    assert res_key == "or_key_123"
    assert res_model == "anthropic/claude-3.5-sonnet"

    # Delete preset
    assert mgr.delete_preset(preset["id"]) is True
    assert mgr.list_presets() == []


def test_append_openrouter_presets(tmp_path, monkeypatch):
    mgr = OpenRouterBYOKManager(base_dir=tmp_path)
    mgr.add_preset(
        name="Custom Preset",
        key_id="key_1",
        model_name="google/gemini-2.0-flash-001"
    )

    # Monkeypatch OpenRouterBYOKManager in handlers
    monkeypatch.setattr(
        "kiro_crew.openrouter_byok.OpenRouterBYOKManager",
        lambda *args, **kwargs: mgr
    )

    initial_models = [
        {"model_name": "auto", "description": "Auto"},
        {"model_name": "claude-3-5-sonnet", "description": "Claude 3.5 Sonnet"}
    ]

    res = _append_openrouter_presets(list(initial_models))
    model_names = [m["model_name"] for m in res]

    assert "Custom Preset" in model_names
    assert "google/gemini-2.0-flash-001" in model_names


def test_model_rejected_reason_allows_presets(tmp_path, monkeypatch):
    mgr = OpenRouterBYOKManager(base_dir=tmp_path)
    mgr.add_preset(
        name="Custom Preset",
        key_id="key_1",
        model_name="google/gemini-2.0-flash-001"
    )
    monkeypatch.setattr(
        "kiro_crew.openrouter_byok.OpenRouterBYOKManager",
        lambda *args, **kwargs: mgr
    )

    # Preset name and model name should be allowed
    assert _model_rejected_reason("Custom Preset") is None
    assert _model_rejected_reason("google/gemini-2.0-flash-001") is None
    assert _model_rejected_reason("openrouter::some/other-model") is None


def test_validate_role_model_allows_presets(tmp_path, monkeypatch):
    mgr = OpenRouterBYOKManager(base_dir=tmp_path)
    mgr.add_preset(
        name="Custom Preset",
        key_id="key_1",
        model_name="google/gemini-2.0-flash-001"
    )
    monkeypatch.setattr(
        "kiro_crew.openrouter_byok.OpenRouterBYOKManager",
        lambda *args, **kwargs: mgr
    )

    req = MagicMock()
    assert _validate_role_model("Custom Preset", req) is None
    assert _validate_role_model("openrouter::google/gemini-2.0-flash-001", req) is None
