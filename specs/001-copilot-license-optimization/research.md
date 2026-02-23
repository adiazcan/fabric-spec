# Research: Microsoft 365 Copilot License Usage Analytics

**Feature Branch**: `001-copilot-license-optimization`
**Date**: 2026-02-23
**Purpose**: Resolve all technical unknowns identified in the Technical Context

---

## R-001: Microsoft Graph API — User Profiles

**Decision**: Use `GET /v1.0/users` with `$select` and `$top=999` pagination via `@odata.nextLink`.

**Rationale**: The v1.0 endpoint is stable and supports application permissions (`User.Read.All`). Using `$select` reduces payload and avoids fetching unnecessary properties. The `$top=999` is the maximum page size.

**Alternatives considered**:
- `/beta/users`: Rejected — unnecessary risk from breaking changes; v1.0 has all needed properties.
- Differential queries (`/delta`): Considered for incremental ingestion — **recommended** for Silver layer updates. Use `GET /v1.0/users/delta?$select=id,displayName,mail,department,officeLocation,jobTitle` to fetch only changed records since last sync.

**Key details**:
- Endpoint: `GET https://graph.microsoft.com/v1.0/users?$select=id,displayName,mail,userPrincipalName,department,officeLocation,jobTitle,accountEnabled&$top=999`
- Permission: `User.Read.All` (application)
- Pagination: Follow `@odata.nextLink`; `$skip` is NOT supported
- To include `signInActivity`, max page size drops to 500
- `$count` and `$search` require `ConsistencyLevel: eventual` header

---

## R-002: Microsoft Graph API — License Assignments

**Decision**: Use `GET /v1.0/users?$select=id,assignedLicenses` to get license data alongside user profiles, and `GET /v1.0/subscribedSkus` for SKU metadata.

**Rationale**: The `/users/{id}/licenseDetails` endpoint does NOT support application permissions, making it unusable for Service Principal (daemon) flows. The `assignedLicenses` property on the `/users` list endpoint works with `User.Read.All` (application).

**Alternatives considered**:
- `/users/{id}/licenseDetails`: Rejected — requires delegated permissions only; incompatible with Service Principal auth.
- PowerShell `Get-MgUserLicenseDetail`: Rejected — would introduce a separate runtime; Python SDK is the chosen stack.

**Microsoft 365 Copilot SKU identifiers**:

| Product Name | String ID | SKU GUID |
|---|---|---|
| Copilot for Microsoft 365 | `Microsoft_365_Copilot` | `639dec6b-bb19-468b-871c-c5c441c4b0cb` |
| Microsoft Copilot for Microsoft 365 | `M365_Copilot` | `a809996b-059e-42e2-9866-db24b99a9782` |
| Copilot for Sales | `Microsoft_Copilot_for_Sales` | `15f2e9fc-b782-4f73-bf51-81d8b7fff6f4` |
| Copilot for Microsoft 365 (EDU) | `Microsoft_365_Copilot_EDU` | `ad9c22b3-52d7-4e7e-973c-88121ea96436` |

**Key Copilot service plan names**:
- `M365_COPILOT_APPS` — Copilot in Word, Excel, PowerPoint
- `M365_COPILOT_TEAMS` — Copilot in Teams
- `M365_COPILOT_BUSINESS_CHAT` — Graph-grounded Copilot chat
- `M365_COPILOT_SHAREPOINT` — Copilot in SharePoint

**Implementation note**: Filter users with `$filter=assignedLicenses/any(l:l/skuId eq '639dec6b-bb19-468b-871c-c5c441c4b0cb')` to get only Copilot-licensed users. Check BOTH known SKU GUIDs to catch all variants.

---

## R-003: Microsoft 365 Copilot Usage Reports API

**Decision**: Use the `/beta/reports/getMicrosoft365CopilotUsageUserDetail` endpoint for per-user usage data, and `getMicrosoft365CopilotUserCountTrend` for daily trend aggregates.

**Rationale**: These are the only endpoints providing Copilot-specific usage metrics. Although in `/beta`, they are the only option — no v1.0 equivalent exists yet. Microsoft is migrating these APIs to a `/copilot` URL path segment (see [Copilot report root](https://learn.microsoft.com/en-us/microsoft-365-copilot/extensibility/api/admin-settings/reports/resources/copilotreportroot)) but the beta `/reports/` path remains functional.

**Alternatives considered**:
- v1.0 reports: Not available for Copilot — only general M365 usage reports exist in v1.0.
- Microsoft Purview Data Explorer: Rejected — requires additional licensing and manual configuration; not suitable for automated pipeline.

**Available endpoints**:

| Endpoint | Purpose | Permission |
|---|---|---|
| `GET /beta/reports/getMicrosoft365CopilotUsageUserDetail(period='D30')` | Per-user usage detail | `Reports.Read.All` |
| `GET /beta/reports/getMicrosoft365CopilotUserCountSummary(period='D30')` | Summary counts | `Reports.Read.All` |
| `GET /beta/reports/getMicrosoft365CopilotUserCountTrend(period='D30')` | Daily trend data | `Reports.Read.All` |

**Per-user detail columns** (JSON field names): `reportRefreshDate`, `reportPeriod`, `userPrincipalName`, `displayName`, `lastActivityDate`, `microsoftTeamsCopilotLastActivityDate`, `wordCopilotLastActivityDate`, `excelCopilotLastActivityDate`, `powerPointCopilotLastActivityDate`, `outlookCopilotLastActivityDate`, `oneNoteCopilotLastActivityDate`, `loopCopilotLastActivityDate`, `copilotChatLastActivityDate`. Nested: `copilotActivityUserDetailsByPeriod[].reportPeriod`.

**Supported periods**: `D7`, `D30`, `D90`, `D180`, `ALL` (ALL returns usage for all four periods).

**Pagination**: JSON responses include `@odata.nextLink` for paginated results.

**Rate limits** (critical):
- CSV format: **14 requests per 10 minutes** per report API per app per tenant
- JSON format (beta): **100 requests per 10 minutes**
- CSV returns `302 Found` redirect to pre-authenticated download URL
- JSON returns `200 OK` directly

**Data freshness**: Usage reports are typically delayed by **24-48 hours**.

**Important**: User identities may be anonymized by default in the tenant's admin center (`Settings → Org settings → Reports`). This setting must be toggled off to get user-identifiable data.

---

## R-004: Copilot Audit Logs

**Decision**: Use the **Microsoft Graph Security Audit Log API** (`https://graph.microsoft.com/beta/security/auditLog/queries`) for Copilot interaction events via the Microsoft Purview Audit Search API.

**Rationale**: The Graph Security Audit Log API provides access to Microsoft Purview Audit logs through a unified Graph endpoint. It uses an asynchronous query pattern — you POST a query with filters (e.g., `recordTypeFilters: ["CopilotInteraction"]`), poll for completion, then retrieve records. This eliminates the need for a separate app registration and authentication flow that was required by the legacy Office 365 Management Activity API. All audit log calls go through the same Graph API as users/licenses/reports.

**Alternatives considered**:
- Office 365 Management Activity API: Viable but requires a separate app registration, separate OAuth scope (`https://manage.office.com/.default`), and subscription management. The Graph Security API consolidates everything under a single Graph app registration.
- Graph `/auditLogs/directoryAudits`: Rejected — covers only Entra ID directory operations, not Copilot interaction events.
- Microsoft Purview portal (manual): Rejected — not suitable for automated pipeline ingestion.

**Graph Security Audit Log API details**:

**Step 1 — Create query**:
```http
POST https://graph.microsoft.com/beta/security/auditLog/queries
Content-Type: application/json

{
  "displayName": "CopilotInteraction-Daily",
  "filterStartDateTime": "2026-02-22T00:00:00Z",
  "filterEndDateTime": "2026-02-23T00:00:00Z",
  "recordTypeFilters": ["CopilotInteraction"],
  "operationFilters": ["CopilotInteraction"],
  "serviceFilter": "Copilot"
}
```
Response: `201 Created` with `id` and `status: "notStarted"`.

**Step 2 — Poll for completion**:
```http
GET https://graph.microsoft.com/beta/security/auditLog/queries/{queryId}
```
Poll until `status` changes to `"succeeded"`.

**Step 3 — Retrieve records**:
```http
GET https://graph.microsoft.com/beta/security/auditLog/queries/{queryId}/records
```
Response: Collection of `auditLogRecord` objects with `@odata.nextLink` pagination.

**Record schema** (`auditLogRecord`):
| Field | Description |
|---|---|
| `id` | Unique record identifier |
| `createdDateTime` | Event timestamp |
| `auditLogRecordType` | Record type (e.g., `CopilotInteraction`) |
| `operation` | Operation name |
| `userId` | User identifier |
| `userPrincipalName` | User principal name |
| `service` | Service name (e.g., `Copilot`) |
| `clientIp` | Client IP address |
| `auditData` | Full audit event payload (JSON) — contains `AppHost`, `AccessedResources`, etc. |

**Required permissions** (Application):
- `AuditLogsQuery.Read.All` — broadest scope, covers all workloads
- Or use workload-specific: `AuditLogsQuery-Entra.Read.All`, `AuditLogsQuery-Exchange.Read.All`, etc.

**Retention**:
- Audit Standard (E3/E5): **180 days**
- Audit Premium (E5 Compliance): **up to 1 year** (configurable to 10 years)

**Latency**: Audit events can have up to **24-hour latency** before being available via the API. The query itself runs asynchronously — poll for `status: "succeeded"`.

**Architecture impact**: This uses the SAME `GraphServiceClient` instance as users/licenses/reports. No separate app registration needed. Add `AuditLogsQuery.Read.All` to the existing Graph app registration. The notebook `04_ingest_audit_logs.py` reuses the same `GraphServiceClient`.

---

## R-005: API Rate Limiting and Throttling Strategy

**Decision**: Rely on the `msgraph-sdk` built-in retry handler middleware, which automatically handles HTTP 429 and 503 responses with exponential backoff honoring the `Retry-After` header. Configure `max_retries=3`.

**Rationale**: The `msgraph-sdk` Python package includes `RetryHandler` middleware that automatically retries on 429/503/504 with exponential backoff and respects the `Retry-After` header — no custom retry code needed. The Reports API has particularly tight limits (14 CSV requests/10min).

**Rate limit summary**:

| API Category | Limit | Scope |
|---|---|---|
| Users / Directory (read) | 8,000 resource units / 10s | App + tenant (large) |
| Reports (CSV) | 14 requests / 10 min | Per report API per app per tenant |
| Reports (JSON, beta) | 100 requests / 10 min | Per app per tenant |
| Audit Log Queries (Security) | 5 requests / 10s | Per app per tenant |
| Global | 130,000 requests / 10s | Per app across all tenants |

**SDK retry configuration**:
```python
from msgraph import GraphServiceClient
from kiota_http.middleware import RetryHandler

# The SDK configures retry middleware by default (max 3 retries).
# To customize, pass middleware options when creating the client.
# Default behavior: retries on 429, 503, 504 with Retry-After header.
```

**Note**: For the Reports API CSV format (returns 302 redirect), the SDK may need the native HTTP adapter to follow redirects manually. Use the JSON format (`$format=application/json`) where possible to stay within the typed SDK response model.

---

## R-006: Authentication Architecture

**Decision**: Use `msgraph-sdk` with `azure-identity` `ClientSecretCredential` for Graph API access. Single app registration for all Graph endpoints including Security Audit Log API. All credentials from Azure Key Vault.

**Rationale**: The `msgraph-sdk` (Microsoft Graph SDK for Python) provides typed models, built-in pagination, automatic retry handling, and a fluent API — eliminating manual HTTP calls, token management, and JSON parsing. `azure-identity` handles the `client_credentials` grant transparently via `ClientSecretCredential`.

**Packages**:
- `msgraph-sdk` — Microsoft Graph SDK for Python (typed client)
- `azure-identity` — Azure credential providers (handles OAuth token lifecycle)

**Graph client initialization**:
```python
from azure.identity import ClientSecretCredential
from msgraph import GraphServiceClient

credential = ClientSecretCredential(
    tenant_id=tenant_id,
    client_id=client_id,
    client_secret=client_secret,
)

scopes = ["https://graph.microsoft.com/.default"]
client = GraphServiceClient(credentials=credential, scopes=scopes)
```

**Usage examples**:
```python
# List users with selected properties
from msgraph.generated.users.users_request_builder import UsersRequestBuilder

query_params = UsersRequestBuilder.UsersRequestBuilderGetQueryParameters(
    select=["id", "displayName", "userPrincipalName", "mail", "department", "assignedLicenses"],
    top=999,
)
config = UsersRequestBuilder.UsersRequestBuilderGetRequestConfiguration(
    query_parameters=query_params,
)
users = await client.users.get(request_configuration=config)

# List subscribed SKUs
skus = await client.subscribed_skus.get()
```

**Required Application Permissions (Graph API)**:

| Permission | Type | Used By |
|---|---|---|
| `User.Read.All` | Application | User profiles, license assignments |
| `Organization.Read.All` | Application | Subscribed SKUs |
| `Reports.Read.All` | Application | Copilot usage reports |
| `AuditLogsQuery.Read.All` | Application | Copilot audit logs via Security Audit Log API |

**Key Vault integration in Fabric**:
- Store `client_id`, `client_secret`, `tenant_id` in Azure Key Vault
- Retrieve via `mssparkutils.credentials.getSecret()` and pass to `ClientSecretCredential`
- Fabric notebooks have built-in `mssparkutils` for Key Vault access

**Async considerations**: The `msgraph-sdk` uses `async/await`. In Fabric notebooks, use `asyncio.run()` or `nest_asyncio` to run async calls in the synchronous notebook context.

---

## R-007: PII Handling and GDPR Compliance

**Decision**: Store PII in clear text across all layers (Bronze, Silver, Gold). Access controlled via Fabric workspace RBAC and Power BI Row-Level Security. No pseudonymization applied.

**Rationale**: The primary use case requires IT administrators to identify specific users by name and email for license reallocation. Pseudonymization in Silver would break join integrity between tables and require complex re-identification flows, negating the core value of the solution. This is a documented exception to constitution Principle I (see plan.md Complexity Tracking).

**Implementation approach**:
- Bronze: Raw data stored as received from APIs (append-only, immutable)
- Silver: Clear-text user identifiers preserved for join integrity and downstream consumption
- Gold: Full user details surfaced for IT admin consumption; Power BI Row-Level Security restricts access to authorized roles
- Data retention: 90-day minimum per spec; align with organization's GDPR retention policy
- Right to erasure: Implement "soft delete" marking for user records; hard delete via separate maintenance process if required
- Access control: Fabric workspace RBAC limits access to IT administrators; Power BI RLS further restricts row-level data visibility

**Alternatives considered**:
- Full anonymization in all layers: Rejected — defeats the purpose of identifying specific users for license actions.
- Pseudonymized Silver layer (SHA-256 hash): Rejected — breaks join performance, requires complex re-identification for the core use case, and adds unnecessary complexity given that workspace access is already restricted to IT admins.

---

## R-008: Incremental Loading Pattern

**Decision**: Use watermark-based incremental loading with a `last_refresh_timestamp` stored in a control Delta table.

**Rationale**: The constitution prohibits full refreshes. The Graph API supports delta queries for users. Reports API returns period-based data. Audit API returns time-windowed content blobs.

**Patterns by data source**:

| Source | Incremental Strategy |
|---|---|
| Users (`/users`) | Delta query (`/users/delta`) — returns only changed records since `deltaLink` |
| Licenses (`/users?$select=assignedLicenses`) | Full snapshot with merge (MERGE INTO) — no delta support; compare with previous snapshot |
| Usage Reports (`/reports/...`) | Period-based (`D7`, `D30`); overwrite last period window; use `Report Refresh Date` as watermark |
| Audit Logs (Security API) | Time-windowed queries via `filterStartDateTime`/`filterEndDateTime`; track `lastProcessedDateTime` |

**Control table schema** (`control_pipeline_state`):
- `source_name` (STRING): e.g., "graph_users", "usage_reports"
- `last_refresh_timestamp` (TIMESTAMP): last successful extract time
- `delta_link` (STRING, nullable): Graph delta query continuation token
- `records_processed` (LONG): count from last run
- `status` (STRING): "SUCCESS" | "PARTIAL" | "FAILED"

---

## R-009: Fabric-Specific Patterns

**Decision**: Use `mssparkutils` for Key Vault access, Fabric-native Lakehouse tables for Delta storage, and Fabric Pipelines for orchestration.

**Rationale**: Fabric provides built-in utilities that simplify development and follow platform conventions.

**Key patterns**:

| Pattern | Implementation |
|---|---|
| Key Vault access | `mssparkutils.credentials.getSecret(keyVaultName, secretName)` |
| Delta table writes | `df.write.format("delta").mode("append").saveAsTable("lakehouse.bronze_users")` |
| Delta MERGE | `DeltaTable.forName(spark, "table").alias("t").merge(source, condition).whenMatched...execute()` |
| Configuration | `parameter.yml` loaded via `mssparkutils.notebook.run()` or Spark config |
| Pipeline parameters | Pass parameters from Pipeline to Notebook via `mssparkutils.notebook.exit(value)` |
| Error logging | Write to `logs_execution_errors` Delta table with structured schema |

**Gotchas**:
- Fabric Spark runtime version determines available PySpark/Delta Lake features
- `mssparkutils` is Fabric-specific; not available in local development
- Notebook outputs are limited to 2KB when returned to Pipeline via `mssparkutils.notebook.exit()`

---

## R-010: Usage Score Calculation

**Decision**: Calculate a composite usage score based on interaction frequency, feature breadth, and recency, to classify users as "active", "low usage", or "inactive".

**Rationale**: FR-005 requires a usage score per user. A composite score captures multiple engagement dimensions rather than relying on a single metric.

**Formula**:
```
usage_score = (frequency_weight * frequency_score) +
              (breadth_weight * breadth_score) +
              (recency_weight * recency_score)
```

| Component | Weight | Calculation |
|---|---|---|
| Frequency | 0.4 | `min(interaction_count / 20, 1.0)` — normalized to 0-1 with 20 interactions as "full" |
| Breadth | 0.3 | `apps_used_count / total_copilot_apps` — ratio of distinct Copilot apps used (7 total: Word, Excel, PPT, Teams, Outlook, OneNote, Copilot Chat) |
| Recency | 0.3 | `max(0, 1 - (days_since_last_activity / 30))` — linear decay over 30 days |

**Classification thresholds** (configurable via `parameter.yml`):

| Category | Score Range | Definition |
|---|---|---|
| Active | ≥ 0.5 | Regular, multi-app usage within the last 30 days |
| Low Usage | 0.1 – 0.49 | Some activity but below engagement expectations |
| Inactive | < 0.1 | No meaningful Copilot activity in the last 30 days |

**License recommendation logic**:
- Licensed + Inactive → "Recommend Remove"
- Licensed + Low Usage → "Monitor" (flag for follow-up)
- Licensed + Active → "Retain"
- Unlicensed + High M365 Activity → "Candidate for License" (scored by M365 app usage)

---

## R-011: Fabric CLI (`fab`)

**Decision**: Use Fabric CLI (`ms-fabric-cli`) for workspace management, pipeline execution, and OneLake file operations in CI/CD workflows and local development.

**Rationale**: Fabric CLI provides command-line access to Microsoft Fabric, enabling automation of workspace operations, pipeline execution, and data file management without writing custom REST API client code. It supports Service Principal authentication, making it suitable for CI/CD pipelines. It complements `fabric-cicd` (which handles item deployment) by providing operational commands like running pipelines and copying files.

**Documentation**: https://microsoft.github.io/fabric-cli/

**Key details**:

| Aspect | Detail |
|---|---|
| Package | `pip install ms-fabric-cli` |
| Command | `fab` |
| Python requirement | 3.10, 3.11, 3.12, or 3.13 |
| Version (current) | v1.4.0 |
| Repo | https://github.com/microsoft/fabric-cli |

**Authentication methods**:
- Interactive (user): `fab auth login` → browser-based
- Service Principal (secret): `fab auth login -u <client_id> -p <client_secret> --tenant <tenant_id>`
- Service Principal (certificate): `fab auth login -u <client_id> --certificate <path> --tenant <tenant_id>`
- Service Principal (federated token): `fab auth login -u <client_id> --federated-token <token> --tenant <tenant_id>`
- Managed Identity: `fab auth login --identity`

**Key commands for this project**:

| Command | Purpose | Example |
|---|---|---|
| `fab ls` | List workspaces and items | `fab ls "Copilot Analytics.Workspace"` |
| `fab run` | Run a pipeline | `fab run "Copilot Analytics.Workspace/daily_copilot_refresh.DataPipeline"` |
| `fab start` | Start a notebook | `fab start "Copilot Analytics.Workspace/01_ingest_graph_users.Notebook"` |
| `fab cp` | Upload/download files to OneLake | `fab cp ./config/parameter.yml "Copilot Analytics.Workspace/copilot_analytics.Lakehouse/Files/config/parameter.yml"` |
| `fab import` | Import items into workspace | `fab import "Copilot Analytics.Workspace/copilot_analytics.Lakehouse" -i ./artifacts/` |
| `fab export` | Export items from workspace | `fab export "Copilot Analytics.Workspace/01_ingest_graph_users.Notebook"` |
| `fab jobs` | Manage job runs | List, monitor, cancel running jobs |

**GitHub Actions integration**:
```yaml
- name: Run Daily Refresh Pipeline
  run: |
    pip install ms-fabric-cli
    fab auth login -u ${{ secrets.CLIENT_ID }} -p ${{ secrets.CLIENT_SECRET }} --tenant ${{ secrets.TENANT_ID }}
    fab run "Copilot Analytics.Workspace/daily_copilot_refresh.DataPipeline"
```

**Use cases for this project**:
- **Local development**: Browse workspace items, test-run individual notebooks, upload test data files to OneLake
- **CI/CD**: Run pipelines after deployment, upload configuration files, verify workspace state
- **Operations**: Ad-hoc pipeline execution, monitor job status, copy data files between environments

**Relationship with `fabric-cicd`**: Fabric CLI handles **operational** commands (run, start, copy, list), while `fabric-cicd` handles **deployment** of item definitions (publish notebooks, pipelines, semantic models from Git). Both are used together in CI/CD workflows — `fabric-cicd` deploys items, then `fab` can trigger pipeline execution or upload supplementary files.

---

## R-012: Fabric CICD (`fabric-cicd`)

**Decision**: Use `fabric-cicd` Python library for automated deployment of Fabric items (notebooks, pipelines, semantic models, etc.) from Git to target workspaces, with environment-specific parameterization via `parameter.yml`.

**Rationale**: `fabric-cicd` is the official Microsoft open-source library for code-first CI/CD with Fabric workspaces. It handles the deployment lifecycle — publishing source-controlled items to workspaces, replacing environment-specific values (lakehouse IDs, workspace IDs, connection strings), and cleaning up orphaned items. This aligns with the constitution's requirement for automated deployments via GitHub Actions (Principle V).

**Documentation**: https://microsoft.github.io/fabric-cicd/latest/

**Key details**:

| Aspect | Detail |
|---|---|
| Package | `pip install fabric-cicd` |
| Python requirement | 3.9 to 3.12 |
| Version (current) | v0.2.0 |
| Repo | https://github.com/microsoft/fabric-cicd |

**Supported item types** (relevant to this project):
- `Notebook` — All 7 ingestion/transformation notebooks
- `DataPipeline` — Daily refresh pipeline
- `SemanticModel` — Power BI semantic model
- `Report` — Power BI report
- `Lakehouse` — Lakehouse definition
- `Environment` — Spark environment configuration

**Core API**:
```python
from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

target_workspace = FabricWorkspace(
    workspace_id="your-workspace-id",
    environment="PROD",  # Must match key in parameter.yml
    repository_directory="./workspace",
    item_type_in_scope=["Notebook", "DataPipeline", "Environment", "SemanticModel", "Report", "Lakehouse"],
)

# Deploy all items in scope
publish_all_items(target_workspace)

# Remove items in workspace not found in repository
unpublish_all_orphan_items(target_workspace)
```

**Authentication**:
- Default: Uses `DefaultAzureCredential` (Azure CLI, Managed Identity, etc.)
- Custom: Pass a `TokenCredential`-compatible object
- For CI/CD: Azure CLI login or explicit SPN with `ClientSecretCredential`

**Parameterization (`parameter.yml`)**:

The `parameter.yml` file sits in the root of the repository directory and supports environment-specific value replacement:

```yaml
find_replace:
    # Replace DEV lakehouse GUID with target environment GUID
    - find_value: "dev-lakehouse-id"
      replace_value:
          PPE: "ppe-lakehouse-id"
          PROD: "prod-lakehouse-id"
      item_type: "Notebook"

    # Dynamic replacement: resolve lakehouse ID at deploy time
    - find_value: \#\s*META\s+"default_lakehouse":\s*"([0-9a-fA-F-]+)"
      replace_value:
          _ALL_: "$items.Lakehouse.copilot_analytics.$id"
      is_regex: "true"
      item_type: "Notebook"

    # Replace workspace ID dynamically
    - find_value: \#\s*META\s+"default_lakehouse_workspace_id":\s*"([0-9a-fA-F-]+)"
      replace_value:
          _ALL_: "$workspace.$id"
      is_regex: "true"
      item_type: "Notebook"

key_value_replace:
    # Enable/disable pipeline schedule per environment
    - find_key: $.schedules[?(@.jobType=="Execute")].enabled
      replace_value:
          DEV: false
          PROD: true
      file_path: "**/.schedules"

semantic_model_binding:
    default:
        connection_id:
            DEV: "dev-connection-id"
            PROD: "prod-connection-id"
```

**Parameter types**:

| Type | Purpose | Use Case |
|---|---|---|
| `find_replace` | Generic string replacement across all files | Lakehouse IDs, workspace IDs in notebooks |
| `key_value_replace` | JSONPath-based key replacement in JSON/YAML files | Pipeline connection strings, schedule settings |
| `spark_pool` | Spark custom pool replacement in Environment items | Pool name/type per environment |
| `semantic_model_binding` | Bind semantic models to data source connections | Lakehouse connection per environment |

**Dynamic replacement variables**:
- `$workspace.$id` — Target workspace ID
- `$items.<type>.<name>.$id` — Deployed item ID (resolved at deploy time)
- `$items.Lakehouse.<name>.$sqlendpoint` — SQL endpoint URL
- `$ENV:<var_name>` — Environment variable (when feature flag enabled)

**Directory structure** (expected by `fabric-cicd`):
```
workspace/
    01_ingest_graph_users.Notebook/
        notebook-content.py
        .platform
    02_ingest_graph_licenses.Notebook/
        notebook-content.py
        .platform
    ...
    daily_copilot_refresh.DataPipeline/
        pipeline-content.json
        .platform
    copilot_analytics.Lakehouse/
        .platform
    Copilot License Analytics.SemanticModel/
        model.bim
        .platform
    Copilot License Report.Report/
        definition.pbir
        .platform
    parameter.yml
```

**GIT Flow** (recommended):
- Feature branches connected to DEV workspaces via Fabric Git Sync
- Deployed branches (main) NOT connected to workspaces — only updated through `fabric-cicd`
- Merge to main → GitHub Actions → `fabric-cicd` deploys to DEV → smoke tests → promotes to PROD with approval gate

**GitHub Actions deployment example**:
```yaml
name: Deploy to Fabric
on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'
      - run: pip install fabric-cicd
      - uses: azure/login@v2
        with:
          creds: ${{ secrets.AZURE_CREDENTIALS }}
      - run: python .deploy/fabric_workspace.py
```

**Deployment script** (`.deploy/fabric_workspace.py`):
```python
from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

workspace = FabricWorkspace(
    workspace_id="target-workspace-id",
    environment="PROD",
    repository_directory="./workspace",
    item_type_in_scope=[
        "Notebook", "DataPipeline", "Environment",
        "SemanticModel", "Report", "Lakehouse"
    ],
)

publish_all_items(workspace)
unpublish_all_orphan_items(workspace)
```

**Key behaviors**:
- Full deployment every time (no commit diff consideration)
- Deploys into the tenant of the executing identity
- Only supports items that have Source Control and public Create/Update APIs
- Report `byPath` references to semantic models in the same repo are auto-converted to `byConnection`
- `parameter.yml` validation runs automatically before deployment; invalid files block deployment

**Impacts on project structure**:
The constitution references `parameter.yml` for environment config and GitHub Actions for CI/CD. `fabric-cicd` is the concrete tool that fulfills these requirements. The repository directory layout must follow Fabric's Source Control conventions (item folders named `<name>.<type>`), with `parameter.yml` at the root of the repository directory.

---

## Resolved Unknowns Summary

| Unknown | Resolution |
|---|---|
| Graph API endpoints | v1.0 for users/licenses; beta for Copilot usage reports |
| Copilot SKU IDs | `639dec6b-...` and `a809996b-...` — check both variants |
| Audit log source | Graph Security Audit Log API (`/security/auditLog/queries`) with `AuditLogsQuery.Read.All` permission |
| License details endpoint | Use `$select=assignedLicenses` on `/users` (not `/licenseDetails` — no app permissions) |
| Rate limits | Reports API is the bottleneck (14 CSV/10min); use JSON format where possible |
| PII handling | Bronze raw, Silver pseudonymized, Gold full with RLS |
| Incremental pattern | Delta queries for users, MERGE for licenses, period windows for reports, time windows for audit |
| Usage score formula | Composite: frequency (0.4) + breadth (0.3) + recency (0.3) |
| Fabric CLI | `ms-fabric-cli` (`fab`) for workspace ops, pipeline execution, OneLake file management; SPN auth for CI/CD |
| Fabric CICD | `fabric-cicd` for code-first deployments; `parameter.yml` for env-specific replacement of lakehouse IDs, connections, schedules |
