#!/usr/bin/env bash

set -euo pipefail

APP_NAME="copilot-analytics-graph"
SECRET_YEARS=1
KEY_VAULT_NAME=""
ADMIN_CONSENT=false

GRAPH_RESOURCE_APP_ID="00000003-0000-0000-c000-000000000000"
REQUIRED_PERMISSIONS=(
  "User.Read.All"
  "Organization.Read.All"
  "Reports.Read.All"
  "AuditLogsQuery.Read.All"
)

usage() {
  cat <<EOF
Usage:
  $(basename "$0") [options]

Options:
  --app-name <name>          App registration display name (default: ${APP_NAME})
  --secret-years <years>     Client secret validity in years (default: ${SECRET_YEARS})
  --key-vault <name>         Optional Key Vault name to store secrets
  --admin-consent            Attempt to grant admin consent automatically
  -h, --help                 Show this help message

Examples:
  $(basename "$0")
  $(basename "$0") --app-name "copilot-analytics-graph-dev" --admin-consent
  $(basename "$0") --key-vault "kv-copilot-analytics"
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --app-name)
      APP_NAME="$2"
      shift 2
      ;;
    --secret-years)
      SECRET_YEARS="$2"
      shift 2
      ;;
    --key-vault)
      KEY_VAULT_NAME="$2"
      shift 2
      ;;
    --admin-consent)
      ADMIN_CONSENT=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

clean_tsv() {
  printf '%s' "$1" | tr -d '\r\n'
}

require_command az

if ! az account show >/dev/null 2>&1; then
  echo "Azure CLI is not logged in. Run: az login" >&2
  exit 1
fi

if [[ -n "${KEY_VAULT_NAME}" ]]; then
  require_command jq
fi

echo "Creating app registration: ${APP_NAME}"

EXISTING_APP_ID=$(az ad app list --display-name "${APP_NAME}" --query "[0].appId" -o tsv || true)
EXISTING_APP_ID=$(clean_tsv "${EXISTING_APP_ID}")

if [[ -n "${EXISTING_APP_ID}" ]]; then
  APP_ID="${EXISTING_APP_ID}"
  echo "App already exists. Using existing appId: ${APP_ID}"
else
  APP_ID=$(az ad app create --display-name "${APP_NAME}" --query appId -o tsv)
  APP_ID=$(clean_tsv "${APP_ID}")
  echo "Created app registration with appId: ${APP_ID}"
fi

OBJECT_ID=""
for _ in {1..10}; do
  OBJECT_ID=$(az ad app list --all --query "[?appId=='${APP_ID}'].id | [0]" -o tsv || true)
  OBJECT_ID=$(clean_tsv "${OBJECT_ID}")
  if [[ -n "${OBJECT_ID}" && "${OBJECT_ID}" != "None" ]]; then
    break
  fi
  sleep 3
done

if [[ -z "${OBJECT_ID}" || "${OBJECT_ID}" == "None" ]]; then
  echo "Unable to resolve app object ID for appId: ${APP_ID}" >&2
  exit 1
fi

if ! az ad sp show --id "${APP_ID}" >/dev/null 2>&1; then
  echo "Creating service principal..."
  az ad sp create --id "${APP_ID}" >/dev/null
else
  echo "Service principal already exists."
fi

echo "Adding Microsoft Graph application permissions..."
for permission in "${REQUIRED_PERMISSIONS[@]}"; do
  ROLE_ID=$(az ad sp show --id "${GRAPH_RESOURCE_APP_ID}" \
    --query "appRoles[?value=='${permission}' && contains(allowedMemberTypes, 'Application')].id | [0]" \
    -o tsv)

  if [[ -z "${ROLE_ID}" ]]; then
    echo "Could not resolve Graph application role for permission: ${permission}" >&2
    exit 1
  fi

  echo "  - ${permission}"
  az ad app permission add \
    --id "${APP_ID}" \
    --api "${GRAPH_RESOURCE_APP_ID}" \
    --api-permissions "${ROLE_ID}=Role" >/dev/null
done

if [[ "${ADMIN_CONSENT}" == true ]]; then
  echo "Attempting admin consent..."
  if az ad app permission admin-consent --id "${APP_ID}" >/dev/null 2>&1; then
    echo "Admin consent granted."
  else
    echo "Admin consent failed. Run manually with sufficient privileges:" >&2
    echo "  az ad app permission admin-consent --id ${APP_ID}" >&2
  fi
else
  echo "Admin consent not requested. Run manually when ready:"
  echo "  az ad app permission admin-consent --id ${APP_ID}"
fi

echo "Creating client secret..."
CLIENT_SECRET=$(az ad app credential reset \
  --id "${APP_ID}" \
  --append \
  --display-name "copilot-analytics-secret" \
  --years "${SECRET_YEARS}" \
  --query password -o tsv)

TENANT_ID=$(az account show --query tenantId -o tsv)

if [[ -n "${KEY_VAULT_NAME}" ]]; then
  echo "Writing secrets to Key Vault: ${KEY_VAULT_NAME}"
  az keyvault secret set --vault-name "${KEY_VAULT_NAME}" --name "graph-tenant-id" --value "${TENANT_ID}" >/dev/null
  az keyvault secret set --vault-name "${KEY_VAULT_NAME}" --name "graph-client-id" --value "${APP_ID}" >/dev/null
  az keyvault secret set --vault-name "${KEY_VAULT_NAME}" --name "graph-client-secret" --value "${CLIENT_SECRET}" >/dev/null
  echo "Key Vault secrets updated: graph-tenant-id, graph-client-id, graph-client-secret"
fi

echo
echo "Completed."
echo "App display name : ${APP_NAME}"
echo "Application (client) ID : ${APP_ID}"
echo "Object ID : ${OBJECT_ID}"
echo "Tenant ID : ${TENANT_ID}"

if [[ -z "${KEY_VAULT_NAME}" ]]; then
  echo "Client secret : ${CLIENT_SECRET}"
  echo
  echo "Store the values in Key Vault as:"
  echo "  graph-tenant-id"
  echo "  graph-client-id"
  echo "  graph-client-secret"
else
  echo "Client secret stored in Key Vault (not echoed)."
fi
