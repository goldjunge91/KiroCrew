"""OpenRouter BYOK (Bring Your Own Key) & Preset Management.

Provides:
- Secure key storage via SecretVault (AES-256-GCM encryption).
- Key masking utility (`sk-or-••••1234`).
- OpenRouter connection testing via `https://openrouter.ai/api/v1/auth/key`.
- Workspace-wide Model Presets management.
- Fallback model resolution logic across Task -> Agent -> Workspace Settings -> System Default.
"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Optional

from kiro_crew.atomic_write import atomic_write
from kiro_crew.config.loader import config_dir
from kiro_crew.secrets.vault import SecretVault

logger = logging.getLogger(__name__)

_KEYS_FILE = "openrouter_keys.json"
_PRESETS_FILE = "openrouter_presets.json"
_WORKSPACE_SETTINGS_FILE = "workspace_model_settings.json"


def mask_api_key(key: str) -> str:
    """Mask an API key for safe UI display (e.g., sk-or-••••1234)."""
    if not key:
        return ""
    clean = key.strip()
    if len(clean) <= 8:
        return "••••" + clean[-4:] if len(clean) >= 4 else "••••"
    prefix = clean[:6] if clean.startswith("sk-or-") else clean[:4]
    suffix = clean[-4:]
    return f"{prefix}••••{suffix}"


def test_openrouter_connection(api_key: str) -> dict[str, Any]:
    """Test OpenRouter API key validity against https://openrouter.ai/api/v1/auth/key."""
    clean_key = api_key.strip()
    if not clean_key:
        return {"success": False, "error": "API key cannot be empty"}

    url = "https://openrouter.ai/api/v1/auth/key"
    if not url.startswith("https://"):
        return {"success": False, "error": "Invalid target URL scheme"}

    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {clean_key}",
            "User-Agent": "KiroCrew-BYOK/1.0",  # brand-ok
        },
        method="GET",
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return {
                "success": True,
                "message": "Connection successful",
                "data": data.get("data", {}),
            }
    except urllib.error.HTTPError as e:
        err_msg = f"HTTP {e.code}: {e.reason}"
        try:
            body = json.loads(e.read().decode("utf-8"))
            if isinstance(body, dict) and "error" in body:
                err_msg = body["error"].get("message", err_msg)
        except Exception:
            pass
        return {"success": False, "error": err_msg}
    except Exception as e:
        return {"success": False, "error": f"Connection failed: {str(e)}"}


@dataclass
class OpenRouterKeyInstance:
    id: str
    name: str
    masked_key: str
    created_at: float = field(default_factory=time.time)
    workspace_id: str = "default"


class OpenRouterBYOKManager:
    """Manages OpenRouter API key instances, presets, and workspace settings securely."""

    def __init__(self, base_dir: Path | None = None) -> None:
        self._dir = base_dir if base_dir is not None else config_dir()
        self._keys_path = self._dir / _KEYS_FILE
        self._presets_path = self._dir / _PRESETS_FILE
        self._settings_path = self._dir / _WORKSPACE_SETTINGS_FILE
        self._vault = SecretVault(self._dir)

    # ── Key Management ──

    def list_keys(self, workspace_id: str = "default") -> list[dict[str, Any]]:
        keys = self._load_keys()
        return [
            {
                "id": k.id,
                "name": k.name,
                "masked_key": k.masked_key,
                "created_at": k.created_at,
                "workspace_id": k.workspace_id,
            }
            for k in keys
            if k.workspace_id == workspace_id or workspace_id == "*"
        ]

    def add_key(self, name: str, api_key: str, workspace_id: str = "default") -> dict[str, Any]:
        clean_key = api_key.strip()
        if not clean_key:
            raise ValueError("API key is required")

        key_id = f"or_key_{uuid.uuid4().hex[:12]}"
        masked = mask_api_key(clean_key)

        # Securely store in SecretVault (AES-256-GCM)
        self._vault.set_sync(f"openrouter_key_{key_id}", clean_key)

        instance = OpenRouterKeyInstance(
            id=key_id,
            name=name.strip() or "OpenRouter Key",
            masked_key=masked,
            workspace_id=workspace_id,
        )

        keys = self._load_keys()
        keys.append(instance)
        self._save_keys(keys)
        return {
            "id": instance.id,
            "name": instance.name,
            "masked_key": instance.masked_key,
            "created_at": instance.created_at,
            "workspace_id": instance.workspace_id,
        }

    def delete_key(self, key_id: str) -> bool:
        keys = self._load_keys()
        filtered = [k for k in keys if k.id != key_id]
        if len(filtered) == len(keys):
            return False

        self._save_keys(filtered)
        self._vault.delete_sync(f"openrouter_key_{key_id}")
        return True

    def get_raw_key(self, key_id: str) -> str | None:
        secret = self._vault.get(f"openrouter_key_{key_id}")
        return secret.reveal() if secret else None

    # ── Preset Management ──

    def list_presets(self, workspace_id: str = "default") -> list[dict[str, Any]]:
        presets = self._load_presets()
        return [
            p
            for p in presets
            if p.get("workspace_id", "default") == workspace_id or workspace_id == "*"
        ]

    def add_preset(
        self, name: str, key_id: str, model_name: str, workspace_id: str = "default"
    ) -> dict[str, Any]:
        preset_id = f"preset_{uuid.uuid4().hex[:12]}"
        preset = {
            "id": preset_id,
            "name": name.strip(),
            "key_id": key_id.strip(),
            "model_name": model_name.strip(),
            "workspace_id": workspace_id,
            "created_at": time.time(),
        }
        presets = self._load_presets()
        presets.append(preset)
        self._save_presets(presets)
        return preset

    def delete_preset(self, preset_id: str) -> bool:
        presets = self._load_presets()
        filtered = [p for p in presets if p.get("id") != preset_id]
        if len(filtered) == len(presets):
            return False
        self._save_presets(filtered)
        return True

    def get_preset_by_name_or_id(
        self, name_or_id: str, workspace_id: str = "default"
    ) -> dict[str, Any] | None:
        """Lookup a preset by matching its 'name' or 'id'."""
        if not name_or_id:
            return None
        target = name_or_id.strip()
        presets = self.list_presets(workspace_id=workspace_id)
        for p in presets:
            if p.get("name") == target or p.get("id") == target:
                return p
        return None

    # ── Workspace Settings ──

    def get_workspace_settings(self, workspace_id: str = "default") -> dict[str, Any]:
        all_settings = self._load_settings()
        return all_settings.get(
            workspace_id,
            {
                "workspace_id": workspace_id,
                "default_key_id": "",
                "default_model_name": "",
            },
        )

    def update_workspace_settings(
        self, workspace_id: str, default_key_id: str, default_model_name: str
    ) -> dict[str, Any]:
        all_settings = self._load_settings()
        setting = {
            "workspace_id": workspace_id,
            "default_key_id": default_key_id.strip(),
            "default_model_name": default_model_name.strip(),
        }
        all_settings[workspace_id] = setting
        self._save_settings(all_settings)
        return setting

    # ── Model Resolution Hierarchy ──

    def resolve_model(
        self,
        task_override: tuple[str, str] | None = None,  # (key_id, model_name)
        agent_override: tuple[str, str] | None = None,  # (key_id, model_name)
        workspace_id: str = "default",
        system_default: str = "auto",
    ) -> tuple[str, str, str]:
        """Resolves (key_id, raw_api_key, model_name) following priority:

        1. Task / Cron Job Override (Highest)
        2. Agent Override
        3. Workspace Settings Default
        4. System Default (Lowest)
        """

        # Helper to check if model is a preset
        def _resolve_override(override: tuple[str, str] | None) -> tuple[str, str, str] | None:
            if not override or not override[1]:
                return None
            key_id, model = override
            preset = self.get_preset_by_name_or_id(model, workspace_id=workspace_id)
            if preset:
                p_key_id = preset.get("key_id", "") or key_id
                p_model = preset.get("model_name", "")
                raw_key = self.get_raw_key(p_key_id) if p_key_id else None
                return (p_key_id, raw_key or "", p_model)
            raw_key = self.get_raw_key(key_id) if key_id else None
            return (key_id, raw_key or "", model)

        # 1. Task
        resolved_task = _resolve_override(task_override)
        if resolved_task:
            return resolved_task

        # 2. Agent
        resolved_agent = _resolve_override(agent_override)
        if resolved_agent:
            return resolved_agent

        # 3. Workspace Default
        ws_settings = self.get_workspace_settings(workspace_id)
        ws_key_id = ws_settings.get("default_key_id", "")
        ws_model = ws_settings.get("default_model_name", "")
        if ws_model:
            raw_key = self.get_raw_key(ws_key_id) if ws_key_id else None
            return (ws_key_id, raw_key or "", ws_model)

        # 4. System Default
        return ("", "", system_default)

    # ── Persistence Internal Helpers ──

    def _load_keys(self) -> list[OpenRouterKeyInstance]:
        if not self._keys_path.exists():
            return []
        try:
            data = json.loads(self._keys_path.read_text(encoding="utf-8"))
            return [OpenRouterKeyInstance(**item) for item in data if isinstance(item, dict)]
        except Exception:
            logger.warning("Failed to read openrouter_keys.json", exc_info=True)
            return []

    def _save_keys(self, keys: list[OpenRouterKeyInstance]) -> None:
        data = [asdict(k) for k in keys]
        atomic_write(self._keys_path, json.dumps(data, indent=2))

    def _load_presets(self) -> list[dict[str, Any]]:
        if not self._presets_path.exists():
            return []
        try:
            data = json.loads(self._presets_path.read_text(encoding="utf-8"))
            return data if isinstance(data, list) else []
        except Exception:
            logger.warning("Failed to read openrouter_presets.json", exc_info=True)
            return []

    def _save_presets(self, presets: list[dict[str, Any]]) -> None:
        atomic_write(self._presets_path, json.dumps(presets, indent=2))

    def _load_settings(self) -> dict[str, dict[str, Any]]:
        if not self._settings_path.exists():
            return {}
        try:
            data = json.loads(self._settings_path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
        except Exception:
            logger.warning("Failed to read workspace_model_settings.json", exc_info=True)
            return {}

    def _save_settings(self, settings: dict[str, dict[str, Any]]) -> None:
        atomic_write(self._settings_path, json.dumps(settings, indent=2))
