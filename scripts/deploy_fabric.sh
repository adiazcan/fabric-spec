#!/usr/bin/env bash
set -euo pipefail

# Deploy all Fabric items using the project virtual environment and script file.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PYTHON="$ROOT_DIR/myenv/bin/python"

if [[ ! -x "$VENV_PYTHON" ]]; then
  echo "Virtual environment python not found: $VENV_PYTHON" >&2
  exit 1
fi

TARGET="${1:-}"
ENVIRONMENT="${2:-DEV}"
shift 2 || true
ITEM_TYPES=("$@")

if [[ -z "$TARGET" ]]; then
  echo "Usage: $0 <workspace-id|workspace-name> [DEV|PROD]" >&2
  exit 2
fi

cd "$ROOT_DIR"
if [[ "$TARGET" =~ ^[0-9a-fA-F-]{36}$ ]]; then
  if [[ ${#ITEM_TYPES[@]} -gt 0 ]]; then
    "$VENV_PYTHON" scripts/deploy_fabric.py --workspace-id "$TARGET" --environment "$ENVIRONMENT" --item-types "${ITEM_TYPES[@]}"
  else
    "$VENV_PYTHON" scripts/deploy_fabric.py --workspace-id "$TARGET" --environment "$ENVIRONMENT"
  fi
else
  if [[ ${#ITEM_TYPES[@]} -gt 0 ]]; then
    "$VENV_PYTHON" scripts/deploy_fabric.py --workspace-name "$TARGET" --environment "$ENVIRONMENT" --item-types "${ITEM_TYPES[@]}"
  else
    "$VENV_PYTHON" scripts/deploy_fabric.py --workspace-name "$TARGET" --environment "$ENVIRONMENT"
  fi
fi
