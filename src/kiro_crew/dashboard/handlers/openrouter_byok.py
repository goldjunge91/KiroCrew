"""API Handlers for OpenRouter BYOK settings, presets, and connection testing."""

from __future__ import annotations

import json
from typing import Any

from kiro_crew.openrouter_byok import (
    OpenRouterBYOKManager,
    test_openrouter_connection,
)


def handle_list_openrouter_keys(workspace_id: str = "default") -> dict[str, Any]:
    mgr = OpenRouterBYOKManager()
    return {"success": True, "keys": mgr.list_keys(workspace_id)}


def handle_add_openrouter_key(payload: dict[str, Any]) -> dict[str, Any]:
    name = str(payload.get("name", ""))
    api_key = str(payload.get("api_key", ""))
    workspace_id = str(payload.get("workspace_id", "default"))

    if not api_key:
        return {"success": False, "error": "api_key is required"}

    mgr = OpenRouterBYOKManager()
    try:
        key_data = mgr.add_key(name=name, api_key=api_key, workspace_id=workspace_id)
        return {"success": True, "key": key_data}
    except Exception as e:
        return {"success": False, "error": str(e)}


def handle_delete_openrouter_key(key_id: str) -> dict[str, Any]:
    mgr = OpenRouterBYOKManager()
    success = mgr.delete_key(key_id)
    return {"success": success, "message": "Key deleted" if success else "Key not found"}


def handle_test_openrouter_key(payload: dict[str, Any]) -> dict[str, Any]:
    api_key = payload.get("api_key")
    key_id = payload.get("key_id")

    mgr = OpenRouterBYOKManager()
    if not api_key and key_id:
        api_key = mgr.get_raw_key(str(key_id))

    if not api_key:
        return {"success": False, "error": "No API key provided or found for testing"}

    return test_openrouter_connection(str(api_key))


def handle_list_presets(workspace_id: str = "default") -> dict[str, Any]:
    mgr = OpenRouterBYOKManager()
    return {"success": True, "presets": mgr.list_presets(workspace_id)}


def handle_add_preset(payload: dict[str, Any]) -> dict[str, Any]:
    name = str(payload.get("name", ""))
    key_id = str(payload.get("key_id", ""))
    model_name = str(payload.get("model_name", ""))
    workspace_id = str(payload.get("workspace_id", "default"))

    if not name or not model_name:
        return {"success": False, "error": "name and model_name are required"}

    mgr = OpenRouterBYOKManager()
    preset = mgr.add_preset(name=name, key_id=key_id, model_name=model_name, workspace_id=workspace_id)
    return {"success": True, "preset": preset}


def handle_delete_preset(preset_id: str) -> dict[str, Any]:
    mgr = OpenRouterBYOKManager()
    success = mgr.delete_preset(preset_id)
    return {"success": success, "message": "Preset deleted" if success else "Preset not found"}


def handle_get_workspace_model_settings(workspace_id: str = "default") -> dict[str, Any]:
    mgr = OpenRouterBYOKManager()
    return {"success": True, "settings": mgr.get_workspace_settings(workspace_id)}


def handle_update_workspace_model_settings(payload: dict[str, Any]) -> dict[str, Any]:
    workspace_id = str(payload.get("workspace_id", "default"))
    default_key_id = str(payload.get("default_key_id", ""))
    default_model_name = str(payload.get("default_model_name", ""))

    mgr = OpenRouterBYOKManager()
    settings = mgr.update_workspace_settings(
        workspace_id=workspace_id,
        default_key_id=default_key_id,
        default_model_name=default_model_name,
    )
    return {"success": True, "settings": settings}
