#!/usr/bin/env python3
"""Post-deploy script: bind the default lakehouse to all notebooks in a Fabric workspace.

fabric-cicd's updateDefinition API does not persist the lakehouse dependency.
This script uses the ipynb-format updateDefinition endpoint with
``metadata.trident.lakehouse`` which Fabric honours and persists.

Usage:
    python scripts/bind_lakehouse.py <workspace_id> <lakehouse_name>

Prerequisites:
    - ``az login`` with access to the target workspace
    - The lakehouse must already be deployed in the workspace
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import subprocess
import sys
import time
import urllib.error
import urllib.request

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

API_ROOT = "https://api.fabric.microsoft.com/v1"
POLL_INTERVAL = 3          # seconds between polls
MAX_POLLS = 40             # ~2 min max wait per notebook


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def _get_token() -> str:
    return subprocess.check_output(
        [
            "az", "account", "get-access-token",
            "--resource", "https://analysis.windows.net/powerbi/api",
            "--query", "accessToken", "-o", "tsv",
        ],
        text=True,
    ).strip()


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _request(method: str, url: str, token: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(req) as resp:
        location = resp.headers.get("Location", "")
        raw = resp.read()
        return {
            "status": resp.status,
            "location": location,
            "body": json.loads(raw) if raw else {},
        }


def _poll(location: str, token: str) -> dict:
    for i in range(MAX_POLLS):
        time.sleep(POLL_INTERVAL)
        result = _request("GET", location, token)
        status = result["body"].get("status", "").lower()
        if status == "succeeded":
            return result
        if status == "failed":
            err = result["body"].get("error", result["body"])
            raise RuntimeError(f"Operation failed: {err}")
    raise TimeoutError("Operation did not complete in time")


def _get_lro_result(location: str, token: str) -> dict:
    _poll(location, token)
    return _request("GET", location + "/result", token)


# ---------------------------------------------------------------------------
# Workspace helpers
# ---------------------------------------------------------------------------

def list_items(workspace_id: str, token: str) -> list[dict]:
    url = f"{API_ROOT}/workspaces/{workspace_id}/items"
    items: list[dict] = []
    while url:
        r = _request("GET", url, token)
        items.extend(r["body"].get("value", []))
        url = r["body"].get("continuationUri")
    return items


# ---------------------------------------------------------------------------
# Core: bind lakehouse via ipynb trident metadata
# ---------------------------------------------------------------------------

def bind_lakehouse(
    workspace_id: str,
    notebook_id: str,
    notebook_name: str,
    lakehouse_id: str,
    lakehouse_name: str,
    token: str,
) -> None:
    base = f"{API_ROOT}/workspaces/{workspace_id}/notebooks/{notebook_id}"

    # 1. Fetch current definition (to preserve notebook code)
    r = _request("POST", f"{base}/getDefinition", token, {})
    result = _get_lro_result(r["location"], token)
    current_parts = result["body"]["definition"]["parts"]

    # 2. Get existing notebook-content.py payload
    nb_part = next(p for p in current_parts if p["path"] == "notebook-content.py")
    nb_content = base64.b64decode(nb_part["payload"]).decode("utf-8")

    # 3. Convert to ipynb, injecting trident metadata
    cells = _py_to_ipynb_cells(nb_content)
    ipynb = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "language_info": {"name": "python"},
            "kernel_info": {"name": "synapse_pyspark"},
            "trident": {
                "lakehouse": {
                    "default_lakehouse": lakehouse_id,
                    "default_lakehouse_name": lakehouse_name,
                    "default_lakehouse_workspace_id": workspace_id,
                    "known_lakehouses": [{"id": lakehouse_id}],
                },
            },
        },
        "cells": cells,
    }

    # 4. Build .platform (metadata-only, no dependencies — API ignores them here)
    platform = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": {"type": "Notebook", "displayName": notebook_name, "description": ""},
        "config": {"version": "2.0", "logicalId": "00000000-0000-0000-0000-000000000000"},
    }

    parts = [
        _b64_part(f"{notebook_name}.ipynb", json.dumps(ipynb, indent=2)),
        _b64_part(".platform", json.dumps(platform, indent=2)),
    ]

    # 5. Update definition via ipynb format
    r = _request("POST", f"{base}/updateDefinition?updateMetadata=True", token,
                 {"definition": {"format": "ipynb", "parts": parts}})
    if r["location"]:
        _poll(r["location"], token)

    log.info("Bound lakehouse '%s' → notebook '%s'", lakehouse_name, notebook_name)


def _b64_part(path: str, content: str) -> dict:
    return {
        "path": path,
        "payload": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
        "payloadType": "InlineBase64",
    }


def _py_to_ipynb_cells(content: str) -> list[dict]:
    """Convert Fabric notebook-content.py to a list of ipynb cells.

    Handles ``# CELL``, ``# MARKDOWN``, ``# METADATA`` markers.
    """
    cells: list[dict] = []
    current_lines: list[str] = []
    cell_type = "code"
    in_metadata = False
    metadata_lines: list[str] = []

    def _flush():
        nonlocal current_lines, cell_type
        if not current_lines and not cells:
            return  # skip leading empty
        # strip trailing blank lines
        while current_lines and current_lines[-1].strip() == "":
            current_lines.pop()
        cell = {
            "cell_type": cell_type,
            "source": [line + "\n" for line in current_lines],
            "metadata": {},
        }
        if cell_type == "code":
            cell["outputs"] = []
            cell["execution_count"] = None
        cells.append(cell)
        current_lines = []

    for line in content.splitlines():
        if line.startswith("# Fabric notebook source"):
            continue

        if "# METADATA **" in line:
            in_metadata = True
            metadata_lines = []
            continue

        if in_metadata:
            metadata_lines.append(line)
            if line.strip() == "# META }":
                in_metadata = False
                # metadata_lines are per-notebook metadata; skip them
                # (trident metadata is injected separately)
            continue

        if "# CELL **" in line:
            _flush()
            cell_type = "code"
            continue

        if "# MARKDOWN **" in line:
            _flush()
            cell_type = "markdown"
            continue

        current_lines.append(line)

    _flush()

    if not cells:
        cells.append({
            "cell_type": "code",
            "source": ["# empty notebook\n"],
            "metadata": {},
            "outputs": [],
            "execution_count": None,
        })

    return cells


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Bind lakehouse to all notebooks in a Fabric workspace")
    parser.add_argument("workspace_id", help="Fabric workspace GUID")
    parser.add_argument("lakehouse_name", help="Display name of the target lakehouse")
    args = parser.parse_args()

    token = _get_token()
    items = list_items(args.workspace_id, token)

    # Resolve lakehouse GUID
    lakehouses = [i for i in items if i["type"] == "Lakehouse" and i["displayName"] == args.lakehouse_name]
    if not lakehouses:
        log.error("Lakehouse '%s' not found in workspace %s", args.lakehouse_name, args.workspace_id)
        sys.exit(1)
    lakehouse_id = lakehouses[0]["id"]
    log.info("Lakehouse '%s' → %s", args.lakehouse_name, lakehouse_id)

    # Bind to every notebook
    notebooks = sorted(
        [i for i in items if i["type"] == "Notebook"],
        key=lambda i: i["displayName"],
    )
    if not notebooks:
        log.warning("No notebooks found in workspace %s", args.workspace_id)
        return

    errors = []
    for nb in notebooks:
        try:
            bind_lakehouse(
                workspace_id=args.workspace_id,
                notebook_id=nb["id"],
                notebook_name=nb["displayName"],
                lakehouse_id=lakehouse_id,
                lakehouse_name=args.lakehouse_name,
                token=token,
            )
        except Exception as exc:
            log.error("Failed to bind '%s': %s", nb["displayName"], exc)
            errors.append(nb["displayName"])

    log.info("Done: %d/%d notebooks bound", len(notebooks) - len(errors), len(notebooks))
    if errors:
        log.error("Failed: %s", ", ".join(errors))
        sys.exit(1)


if __name__ == "__main__":
    main()
