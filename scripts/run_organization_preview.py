"""Run the organization prototype with the project's pod isolation, in the foreground.

For hosts without a systemd user bus. No service is installed or live data
reused. Stop with Ctrl+C; .kirocrew-dev retains the prototype team for next time.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=6784)
    args = parser.parse_args()
    checkout = Path(__file__).resolve().parents[1]
    preview_home = checkout / ".kirocrew-dev"
    os.environ["KIROCREW_HOME"] = str(preview_home)

    from kiro_crew.acp_backends import ACP_BACKEND_KIRO
    from kiro_crew.atomic_write import atomic_write
    from kiro_crew.pod.config import PodConfig
    from kiro_crew.pod.runtime import _ensure_pod_dir, _seed_pod_os_home, build_pod_env

    _ensure_pod_dir(preview_home, what="organization preview home")
    config_path = preview_home / "config.json"
    if config_path.is_symlink():
        raise RuntimeError("The preview config must not be a symbolic link")
    if not config_path.exists():
        project = preview_home / "workspace" / "project"
        project.mkdir(parents=True, exist_ok=True)
        config = {
            "agent": {
                "sandbox": "auto",
                "model": "auto",
                "acp_backend": ACP_BACKEND_KIRO,
                "member_acp_backend": ACP_BACKEND_KIRO,
                "dangerously_skip_permissions": True,
            },
            "memory": {"enabled": True, "private_provisioning_enabled": True},
            "workspaces": {"default": {"dir": str(project.parent)}},
            "default_workspace": "default",
            "dashboard": {"onboarded": True, "theme_mode": "dark", "theme_color": "kiro"},
            "tunnel": {"enabled": False},
            "timezone": "UTC",
        }
        atomic_write(config_path, json.dumps(config, indent=2), restrict_to_owner=True)
    _seed_pod_os_home(preview_home / "os-home")
    env = build_pod_env(
        PodConfig.load(), preview_home, args.port, checkout, skip_model_download=True
    )
    env["PYTHONPATH"] = str(checkout / "src")
    print(
        f"Organization preview: http://localhost:{args.port}/capabilities?tab=organization",
        flush=True,
    )
    print(f"Private preview data: {preview_home}", flush=True)
    return subprocess.run(
        [sys.executable, "-m", "kiro_crew", "gateway", "--no-crons", "--approval", "yolo"],
        cwd=checkout,
        env=env,
        check=False,
    ).returncode


if __name__ == "__main__":
    raise SystemExit(main())
