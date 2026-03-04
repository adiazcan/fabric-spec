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

run_deploy() {
  local -a types=("$@")
  if [[ "$TARGET" =~ ^[0-9a-fA-F-]{36}$ ]]; then
    if [[ ${#types[@]} -gt 0 ]]; then
      "$VENV_PYTHON" scripts/deploy_fabric.py --workspace-id "$TARGET" --environment "$ENVIRONMENT" --item-types "${types[@]}"
    else
      "$VENV_PYTHON" scripts/deploy_fabric.py --workspace-id "$TARGET" --environment "$ENVIRONMENT"
    fi
  else
    if [[ ${#types[@]} -gt 0 ]]; then
      "$VENV_PYTHON" scripts/deploy_fabric.py --workspace-name "$TARGET" --environment "$ENVIRONMENT" --item-types "${types[@]}"
    else
      "$VENV_PYTHON" scripts/deploy_fabric.py --workspace-name "$TARGET" --environment "$ENVIRONMENT"
    fi
  fi
}

# First-time workspace bootstrap:
# 1) Publish Lakehouse first so it exists in target workspace
# 2) Publish all default item types so notebook metadata can resolve lakehouse GUIDs
if [[ ${#ITEM_TYPES[@]} -eq 0 ]]; then
  echo "Bootstrap pass 1/2: publishing Lakehouse first"
  run_deploy "Lakehouse"
  echo "Bootstrap pass 2/2: publishing default item types"
  run_deploy
  echo "Post-deploy: binding lakehouse to notebooks"
  "$VENV_PYTHON" scripts/bind_lakehouse.py "$TARGET" CopilotUsageLakehouse
  exit 0
fi

run_deploy "${ITEM_TYPES[@]}"

# Bind lakehouse after any deploy that includes notebooks
for t in "${ITEM_TYPES[@]}"; do
  if [[ "${t,,}" == "notebook" ]]; then
    echo "Post-deploy: binding lakehouse to notebooks"
    "$VENV_PYTHON" scripts/bind_lakehouse.py "$TARGET" CopilotUsageLakehouse
    break
  fi
done
