# Quickstart: Microsoft 365 Copilot License Usage Analytics

**Feature Branch**: `001-copilot-license-optimization`
**Date**: 2026-02-23

---

## Prerequisites

### Azure / Microsoft 365

1. **Microsoft 365 tenant** with Copilot licenses deployed
2. **Microsoft Fabric workspace** with sufficient capacity (F2 or higher recommended)
3. **Azure Key Vault** provisioned and accessible from Fabric
4. **Audit logging enabled** in Microsoft 365 for Copilot activities

### App Registrations

An Azure AD (Entra ID) app registration is required:

#### App 1: Graph API Access
- **Name**: `copilot-analytics-graph`
- **Application permissions** (require admin consent):
  - `User.Read.All`
  - `Organization.Read.All`
  - `Reports.Read.All`
  - `AuditLogsQuery.Read.All`
- **Client secret**: Store in Key Vault as `graph-client-secret`

Optional automation script:

```bash
./.deploy/create_app_registration.sh --key-vault "<your-key-vault-name>" --admin-consent
```

The script creates (or reuses) the app registration, assigns required Graph application permissions, creates a client secret, and writes these Key Vault secrets:
- `graph-tenant-id`
- `graph-client-id`
- `graph-client-secret`

### Key Vault Secrets

| Secret Name | Value |
|-------------|-------|
| `graph-tenant-id` | Azure AD tenant ID |
| `graph-client-id` | Graph app registration client ID |
| `graph-client-secret` | Graph app registration client secret |

### Tenant Settings

- **Disable report anonymization**: Microsoft 365 Admin Center → Settings → Org settings → Reports → uncheck "Display concealed user, group, and site names in all reports"

---

## Setup Steps

### 1. Create Fabric Lakehouse

In your Fabric workspace:
1. Create a new Lakehouse named `copilot_analytics`
2. This will serve as the unified storage for all Bronze, Silver, and Gold tables

### 2. Configure Environment

Create `config/parameter.yml` with:

```yaml
environment: DEV  # or PROD

key_vault:
  name: "your-key-vault-name"

graph_api:
  tenant_id_secret: "graph-tenant-id"
  client_id_secret: "graph-client-id"
  client_secret_secret: "graph-client-secret"
  base_url: "https://graph.microsoft.com"

audit_api:
  base_url: "https://graph.microsoft.com/beta/security/auditLog"

copilot_sku_ids:
  - "639dec6b-bb19-468b-871c-c5c441c4b0cb"
  - "a809996b-059e-42e2-9866-db24b99a9782"
  - "ad9c22b3-52d7-4e7e-973c-88121ea96436"

usage_score:
  frequency_weight: 0.4
  breadth_weight: 0.3
  recency_weight: 0.3
  active_threshold: 0.5
  low_usage_threshold: 0.1
  inactivity_days: 30

license_cost:
  cost_per_user_per_month: 30.0  # USD — adjust to your contract pricing

alerts:
  utilization_threshold: 0.6
  alert_recipients:
    - "itadmin@contoso.com"

pipeline:
  schedule_cron: "0 6 * * *"  # Daily at 6 AM
  max_retries: 3
  retry_backoff_base: 2
```

### 3. Import Notebooks

Import the following notebooks into your Fabric workspace:

| Notebook | Purpose |
|----------|---------|
| `01_ingest_graph_users.py` | Fetch user profiles from Graph API → `bronze_users` |
| `02_ingest_graph_licenses.py` | Fetch license assignments → `bronze_licenses` |
| `03_ingest_usage_reports.py` | Fetch Copilot usage reports → `bronze_usage_reports` |
| `04_ingest_audit_logs.py` | Fetch audit events → `bronze_audit_logs` |
| `05_transform_bronze_to_silver.py` | Clean and conform → Silver tables |
| `06_transform_silver_to_gold.py` | Calculate scores and aggregates → Gold tables |
| `99_data_quality_checks.py` | Run validation checks |

### 4. Create Pipeline

Create a Fabric Data Pipeline named `daily_copilot_refresh`:

```
Sequential Activities:
1. Notebook: 01_ingest_graph_users
2. Notebook: 02_ingest_graph_licenses
3. Notebook: 03_ingest_usage_reports
4. Notebook: 04_ingest_audit_logs
5. Notebook: 05_transform_bronze_to_silver
6. Notebook: 06_transform_silver_to_gold
7. Notebook: 99_data_quality_checks
8. Activity: Refresh Semantic Model
9. Activity: Send Email Notification
```

Schedule: Daily at 6:00 AM (configurable)

### 5. Create Semantic Model

Create a Power BI Semantic Model connected to the Lakehouse Gold tables:
- Import `gold_user_license_usage`, `gold_daily_metrics`, `gold_department_summary`
- Create dimension tables (`dim_user`, `dim_date`, `dim_department`, `dim_license`)
- Define relationships (all uni-directional, dimension → fact)
- Add DAX measures (see [contracts/api-contracts.md](contracts/api-contracts.md))

### 6. Build Power BI Report

Create a report with 4 pages:
1. **Executive Dashboard**: KPI cards, utilization gauge, trend line
2. **User Details**: Filterable table with conditional formatting
3. **Department Analysis**: Matrix with drillthrough
4. **Recommendations**: List with export capability

---

## First Run

1. Trigger the pipeline manually for the initial data load
2. Verify Bronze tables contain data (`bronze_users`, `bronze_licenses`, etc.)
3. Verify Silver tables are populated and deduplicated
4. Verify Gold tables contain usage scores and recommendations
5. Open the Power BI report and verify KPIs display correctly

**Expected initial load time**: 5-15 minutes depending on organization size and API response times.

---

## Verification Checklist

- [ ] Key Vault secrets are accessible from Fabric notebooks
- [ ] Graph API returns user data (test with small `$top=10` query)
- [ ] Copilot usage report returns non-anonymized user data
- [ ] Audit log query returns CopilotInteraction records
- [ ] All Bronze tables have data with `_ingested_at` timestamps
- [ ] Silver tables deduplicated correctly (row counts match expected)
- [ ] Gold `usage_score` values are between 0.0 and 1.0
- [ ] Power BI report pages load without errors
- [ ] Pipeline schedule is active and triggers at configured time

## Verification Run (Phase 10)

Run date: 2026-02-23

| Check | Result | Evidence |
|-------|--------|----------|
| Key Vault configuration present | ✅ Pass | `config/key_vault_config.yml` and `config/parameter.yml` include expected secret mapping keys |
| Graph API connectivity setup | ✅ Pass | Ingestion notebooks use `ClientSecretCredential` and `mssparkutils.credentials.getSecret()` |
| Data pipeline execution chain | ✅ Pass | `pipelines/daily_refresh_pipeline.json` includes ordered notebook dependencies `01 → 02 → 03 → 03b → 04 → 05 → 06 → 99` |
| Report rendering assets | ✅ Pass | `reports/copilot_license_analytics.pbir` contains all required report pages |

Notes:
- This verification run validates repository configuration and artifact readiness.
- Live environment execution checks (actual API calls, Fabric run history, rendered visuals) should be completed in target Fabric workspace after deployment.

---

## Troubleshooting

| Issue | Resolution |
|-------|-----------|
| 403 Forbidden on Graph API | Verify app permissions are admin-consented; check `User.Read.All` is application-level |
| Empty usage reports | Check tenant report anonymization setting; verify Copilot licenses are assigned |
| 429 Too Many Requests | `msgraph-sdk` built-in retry middleware handles this; if persistent, reduce `$top` page size |
| No audit events | Verify Audit Standard is enabled; events may take up to 24 hours to appear |
| Key Vault access denied | Ensure Fabric workspace identity has Key Vault Reader role |
| Anonymized user names in reports | Disable report anonymization in M365 Admin Center |
