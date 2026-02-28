# Quickstart: M365 Copilot License Usage Analytics

**Feature Branch**: `001-copilot-license-usage`
**Date**: 2026-02-25

This guide provisions the entire Fabric workspace from the CLI. No Fabric UI interaction is required.

## Prerequisites

- Python 3.11+
- Azure AD Service Principal with:
  - **Graph API permissions** (application): `User.Read.All`, `Reports.Read.All`, `AuditLogsQuery.Read.All`
  - **Fabric workspace role**: Admin or Member on target workspace
- Azure Key Vault with secrets: `sp-client-id`, `sp-client-secret`, `sp-tenant-id`
- Git repository cloned locally with the `workspace/` directory

## 1. Install CLI Tools

```bash
pip install ms-fabric-cli fabric-cicd
```

Verify:
```bash
fab --version   # Expected: 1.4.0+
python -c "import fabric_cicd; print(fabric_cicd.__version__)"   # Expected: 0.2.0+
```

## 2. Authenticate Fabric CLI

```bash
export CLIENT_ID="<service-principal-client-id>"
export CLIENT_SECRET="<service-principal-client-secret>"
export TENANT_ID="<azure-ad-tenant-id>"

fab auth login -u "$CLIENT_ID" -p "$CLIENT_SECRET" --tenant "$TENANT_ID"
```

Verify authentication:
```bash
fab ls   # Should list accessible workspaces
```

## 3. Create Workspace (if needed)

```bash
# Create the DEV workspace
fab create CopilotUsageDev.Workspace

# Create the PROD workspace
fab create CopilotUsageProd.Workspace
```

## 4. Provision Lakehouse

```bash
fab create CopilotUsageDev.Workspace/CopilotUsageLakehouse.Lakehouse
```

Verify:
```bash
fab ls CopilotUsageDev.Workspace
# Should show: CopilotUsageLakehouse.Lakehouse
```

## 5. Provision Environment

```bash
fab create CopilotUsageDev.Workspace/CopilotUsageEnv.Environment
```

The Environment's `Setting.yml` (imported via `fabric-cicd`) will install Python dependencies:
- `msgraph-sdk`
- `msgraph-beta-sdk`
- `azure-identity`

## 6. Deploy Notebooks, Pipeline, and Environment via fabric-cicd

This is the primary deployment method — `fabric-cicd` deplishes all workspace items from the Git repository structure.

```bash
python -c "
from fabric_cicd import FabricWorkspace

ws = FabricWorkspace(
    workspace_id='<dev-workspace-id>',
    environment='DEV',
    repository_directory='./workspace',
    item_type_in_scope=['Notebook', 'DataPipeline', 'Environment', 'Lakehouse']
)
ws.publish_all_items()
"
```

This reads the `workspace/` directory, processes `parameter.yml` for DEV environment substitutions, and deploys all items.

## 7. Wire Notebook → Lakehouse Bindings

After deployment, bind each notebook to the Lakehouse:

```bash
WORKSPACE="CopilotUsageDev.Workspace"
LAKEHOUSE_ID="<lakehouse-guid>"
WORKSPACE_ID="<workspace-guid>"

for NB in 00_watermarks 01_ingest_users 02_ingest_usage 03_ingest_audit_logs \
          04_transform_silver 05_transform_gold 06_compute_scores 99_dq_checks helpers; do
  fab set "$WORKSPACE/$NB.Notebook" \
    -q definition.parts[0].payload.metadata.dependencies.lakehouse \
    -i "{\"known_lakehouses\": [{\"id\": \"$LAKEHOUSE_ID\"}], \"default_lakehouse\": \"$LAKEHOUSE_ID\", \"default_lakehouse_name\": \"CopilotUsageLakehouse\", \"default_lakehouse_workspace_id\": \"$WORKSPACE_ID\"}"
done
```

## 8. Wire Notebook → Environment Bindings

```bash
ENV_ID="<environment-guid>"

for NB in 00_watermarks 01_ingest_users 02_ingest_usage 03_ingest_audit_logs \
          04_transform_silver 05_transform_gold 06_compute_scores 99_dq_checks helpers; do
  fab set "$WORKSPACE/$NB.Notebook" \
    -q environment \
    -i "{\"environmentId\": \"$ENV_ID\", \"workspaceId\": \"$WORKSPACE_ID\"}"
done
```

## 9. Trigger a Test Pipeline Run

```bash
fab run CopilotUsageDev.Workspace/CopilotUsagePipeline.DataPipeline
```

Monitor completion:
```bash
fab jobs CopilotUsageDev.Workspace/CopilotUsagePipeline.DataPipeline
```

## 10. Verify Data in Lakehouse

```bash
# List tables created
fab ls CopilotUsageDev.Workspace/CopilotUsageLakehouse.Lakehouse

# Check specific table
fab table CopilotUsageDev.Workspace/CopilotUsageLakehouse.Lakehouse -t gold_copilot_license_summary
```

## 11. Deploy to Production

After DEV validation and PR approval:

```bash
python -c "
from fabric_cicd import FabricWorkspace

ws = FabricWorkspace(
    workspace_id='<prod-workspace-id>',
    environment='PROD',
    repository_directory='./workspace',
    item_type_in_scope=['Notebook', 'DataPipeline', 'Environment', 'Lakehouse']
)
ws.publish_all_items()
"
```

`parameter.yml` automatically substitutes PROD Lakehouse GUIDs, workspace IDs, and enables the daily schedule.

## CI/CD via GitHub Actions

The `.github/workflows/deploy.yml` automates the full cycle:

```
push to main → ruff lint → gitleaks secret-scan → fab auth login → fabric-cicd publish → fab run smoke-test
```

See [plan.md](plan.md) for the complete CI/CD pipeline specification.

## parameter.yml Reference

Key parameterisation entries:

| What | find_replace / key_value_replace | DEV Value | PROD Value |
|------|--------------------------------|-----------|------------|
| Lakehouse GUID | regex on `default_lakehouse` in `# META` | `$items.Lakehouse.CopilotUsageLakehouse.$id` | `$items.Lakehouse.CopilotUsageLakehouse.$id` |
| Workspace GUID | regex on `default_lakehouse_workspace_id` | `$workspace.$id` | `$workspace.$id` |
| Notebook IDs in pipeline | JSONPath on `typeProperties.notebookId` | `$items.Notebook.<name>.$id` | `$items.Notebook.<name>.$id` |
| Schedule enabled | JSONPath on `$.schedules[*].enabled` | `false` | `true` |
| Key Vault URL | `find_replace` string | `https://kv-copilot-dev.vault.azure.net` | `https://kv-copilot-prod.vault.azure.net` |
