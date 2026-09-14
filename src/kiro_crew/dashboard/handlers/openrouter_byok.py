"""API Handlers for OpenRouter BYOK settings, presets, and connection testing."""

from __future__ import annotations

import json
from typing import Any

from aiohttp import web

from kiro_crew.openrouter_byok import (
    OpenRouterBYOKManager,
    test_openrouter_connection,
)


async def api_openrouter_keys_list(request: web.Request) -> web.Response:
    workspace_id = request.query.get("workspace_id", "default")
    mgr = OpenRouterBYOKManager()
    return web.json_response({"success": True, "keys": mgr.list_keys(workspace_id)})


async def api_openrouter_keys_add(request: web.Request) -> web.Response:
    try:
        payload = await request.json()
    except Exception:
        return web.json_response({"success": False, "error": "Invalid JSON body"}, status=400)

    name = str(payload.get("name", ""))
    api_key = str(payload.get("api_key", ""))
    workspace_id = str(payload.get("workspace_id", "default"))

    if not api_key:
        return web.json_response({"success": False, "error": "api_key is required"}, status=400)

    mgr = OpenRouterBYOKManager()
    try:
        key_data = mgr.add_key(name=name, api_key=api_key, workspace_id=workspace_id)
        return web.json_response({"success": True, "key": key_data})
    except Exception as e:
        return web.json_response({"success": False, "error": str(e)}, status=400)


async def api_openrouter_keys_delete(request: web.Request) -> web.Response:
    key_id = request.match_info.get("key_id", "")
    mgr = OpenRouterBYOKManager()
    success = mgr.delete_key(key_id)
    if success:
        return web.json_response({"success": True, "message": "Key deleted"})
    return web.json_response({"success": False, "error": "Key not found"}, status=404)


async def api_openrouter_keys_test(request: web.Request) -> web.Response:
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    api_key = payload.get("api_key")
    key_id = payload.get("key_id")

    mgr = OpenRouterBYOKManager()
    if not api_key and key_id:
        api_key = mgr.get_raw_key(str(key_id))

    if not api_key:
        return web.json_response({"success": False, "error": "No API key provided or found for testing"}, status=400)

    res = test_openrouter_connection(str(api_key))
    return web.json_response(res)


async def api_openrouter_presets_list(request: web.Request) -> web.Response:
    workspace_id = request.query.get("workspace_id", "default")
    mgr = OpenRouterBYOKManager()
    return web.json_response({"success": True, "presets": mgr.list_presets(workspace_id)})


async def api_openrouter_presets_add(request: web.Request) -> web.Response:
    try:
        payload = await request.json()
    except Exception:
        return web.json_response({"success": False, "error": "Invalid JSON body"}, status=400)

    name = str(payload.get("name", ""))
    key_id = str(payload.get("key_id", ""))
    model_name = str(payload.get("model_name", ""))
    workspace_id = str(payload.get("workspace_id", "default"))

    if not name or not model_name:
        return web.json_response({"success": False, "error": "name and model_name are required"}, status=400)

    mgr = OpenRouterBYOKManager()
    preset = mgr.add_preset(name=name, key_id=key_id, model_name=model_name, workspace_id=workspace_id)
    return web.json_response({"success": True, "preset": preset})


async def api_openrouter_presets_delete(request: web.Request) -> web.Response:
    preset_id = request.match_info.get("preset_id", "")
    mgr = OpenRouterBYOKManager()
    success = mgr.delete_preset(preset_id)
    if success:
        return web.json_response({"success": True, "message": "Preset deleted"})
    return web.json_response({"success": False, "error": "Preset not found"}, status=404)


async def api_openrouter_settings_get(request: web.Request) -> web.Response:
    workspace_id = request.query.get("workspace_id", "default")
    mgr = OpenRouterBYOKManager()
    return web.json_response({"success": True, "settings": mgr.get_workspace_settings(workspace_id)})


async def api_openrouter_settings_update(request: web.Request) -> web.Response:
    try:
        payload = await request.json()
    except Exception:
        return web.json_response({"success": False, "error": "Invalid JSON body"}, status=400)

    workspace_id = str(payload.get("workspace_id", "default"))
    default_key_id = str(payload.get("default_key_id", ""))
    default_model_name = str(payload.get("default_model_name", ""))

    mgr = OpenRouterBYOKManager()
    settings = mgr.update_workspace_settings(
        workspace_id=workspace_id,
        default_key_id=default_key_id,
        default_model_name=default_model_name,
    )
    return web.json_response({"success": True, "settings": settings})
