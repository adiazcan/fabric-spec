#!/usr/bin/env python3
"""Repair notebook content in Fabric by posting updateDefinition with local files."""

from __future__ import annotations

import argparse
import base64
import json
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

API_ROOT = "https://api.powerbi.com/v1"


def run_cmd(cmd: list[str]) -> str:
    proc = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return proc.stdout.strip()


def get_access_token() -> str:
    return run_cmd(
        [
            "az",
            "account",
            "get-access-token",
            "--resource",
            "https://analysis.windows.net/powerbi/api",
            "--query",
            "accessToken",
            "-o",
            "tsv",
        ]
    )


def api_request(token: str, method: str, path: str, body: dict | None = None) -> dict:
    url = f"{API_ROOT}/{path.lstrip('/')}"
    data = None
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    if body is not None:
        data = json.dumps(body, separators=(",", ":")).encode("utf-8")

    req = urllib.request.Request(url=url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {url} failed: HTTP {exc.code} {detail}") from exc


def b64_text(path: Path) -> str:
    return base64.b64encode(path.read_text(encoding="utf-8").encode("utf-8")).decode("ascii")


def find_notebook_items(token: str, workspace_id: str) -> dict[str, str]:
    payload = api_request(token, "GET", f"workspaces/{workspace_id}/items")
    result: dict[str, str] = {}
    for item in payload.get("value", []):
        if item.get("type") == "Notebook":
            result[item.get("displayName", "")] = item.get("id", "")
    return result


def update_notebook_definition(token: str, workspace_id: str, item_id: str, notebook_dir: Path) -> None:
    """Push the local notebook-content.py directly using Fabric's native .py format."""
    body = {
        "definition": {
            "parts": [
                {
                    "path": "notebook-content.py",
                    "payload": b64_text(notebook_dir / "notebook-content.py"),
                    "payloadType": "InlineBase64",
                },
                {
                    "path": ".platform",
                    "payload": b64_text(notebook_dir / ".platform"),
                    "payloadType": "InlineBase64",
                },
            ]
        }
    }

    api_request(
        token,
        "POST",
        f"workspaces/{workspace_id}/items/{item_id}/updateDefinition?updateMetadata=true",
        body,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Repair notebook content in Fabric workspace")
    parser.add_argument("--workspace-id", required=True, help="Fabric workspace GUID")
    parser.add_argument(
        "--repo-root",
        default=".",
        help="Repository root path containing workspace/* notebooks",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = Path(args.repo_root).resolve()

    mapping = {
        "00_watermarks": repo_root / "workspace/00_watermarks.Notebook",
        "helpers": repo_root / "workspace/helpers.Notebook",
    }

    token = get_access_token()
    notebook_ids = find_notebook_items(token, args.workspace_id)

    for display_name, notebook_dir in mapping.items():
        item_id = notebook_ids.get(display_name)
        if not item_id:
            raise RuntimeError(f"Notebook '{display_name}' not found in workspace")
        update_notebook_definition(token, args.workspace_id, item_id, notebook_dir)
        print(f"Updated {display_name} ({item_id})")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
