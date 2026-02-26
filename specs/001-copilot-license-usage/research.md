# Research: M365 Copilot License Usage Analytics

**Feature Branch**: `001-copilot-license-usage`
**Date**: 2026-02-25

## Research Task 1: Microsoft Graph API — Copilot Usage Reports

### Decision
Use `GET /beta/reports/getMicrosoft365CopilotUsageUserDetail(period='D7')` to retrieve per-user Copilot usage data.

### Rationale
This is the only Microsoft Graph endpoint that provides per-user, per-application Copilot usage data. It returns last-activity dates across all Copilot-enabled M365 apps, which directly maps to the three score components (recency, frequency, breadth).

### Key Findings
- **Endpoint**: `GET /beta/reports/getMicrosoft365CopilotUsageUserDetail(period='D7')`
- **Permission**: `Reports.Read.All` (application)
- **Response format**: CSV (default) or JSON (via `Accept: application/json`)
- **Columns returned**:
  - `reportRefreshDate`, `reportPeriod`
  - `userPrincipalName`, `displayName`
  - `lastActivityDate`
  - Per-app last-activity dates: `microsoftTeamsCopilotLastActivityDate`, `wordCopilotLastActivityDate`, `excelCopilotLastActivityDate`, `powerPointCopilotLastActivityDate`, `outlookCopilotLastActivityDate`, `oneNoteCopilotLastActivityDate`, `loopCopilotLastActivityDate`, `copilotChatLastActivityDate`
- **Period values**: `D7`, `D30`, `D90`, `D180`
- **Beta status**: This endpoint is in `/beta` — not yet GA. Schema may change.
- **Pagination**: Supports `$top` and `$skipToken` for large result sets.
- **Data latency**: Microsoft reports a 24-48 hour delay before usage data appears.

### Alternatives Considered
- **Activity Reports API** (`getOffice365ActiveUserDetail`): Only shows general M365 active users, no Copilot-specific data.
- **Audit Logs only**: Audit logs provide event-level data but lack the pre-aggregated per-app last-activity dates needed for efficient scoring.

### SDK Integration
- **Package**: `msgraph-beta-sdk` v1.56.0 (beta endpoint — not available in v1.0 SDK)
- **Import**: `from msgraph_beta import GraphServiceClient`
- **Call pattern**: `await client.reports.get_microsoft365_copilot_usage_user_detail_with_period('D7').get()`
- **Pagination**: Follow `odata_next_link` property on response until `None`.
- **Error handling**: Catch `APIError` from `kiota_abstractions.api_error`.

### Impact on Design
- Bronze layer stores raw response per refresh date.
- Silver layer parses per-app columns into a normalized structure for scoring.
- The `period='D7'` parameter should be used for daily runs; `D30` can be used for backfill.
- Must handle the `reportRefreshDate` as the watermark column for incremental loading.

---

## Research Task 2: Microsoft Graph API — User Profiles & License Assignments

### Decision
Use `GET /v1.0/users` with `$select` and `$expand=licenseDetails` to retrieve user profiles and license assignments in a single call.

### Rationale
The `/v1.0/users` endpoint is GA, stable, and supports filtering and expansion to include license details in one request, minimising API calls and simplifying incremental loading.

### Key Findings
- **Endpoint**: `GET /v1.0/users`
- **Permissions**: `User.Read.All` (application)
- **Key `$select` fields**: `id`, `displayName`, `mail`, `userPrincipalName`, `department`, `jobTitle`, `accountEnabled`, `companyName`, `officeLocation`
- **License data**: `$expand=licenseDetails` returns all assigned license SKUs per user.
- **Copilot SKU identification**: Filter `licenseDetails` for `skuPartNumber` containing `MICROSOFT_365_COPILOT` or known Copilot-enabled SKU GUIDs.
- **Pagination**: Returns 100 users by default; supports `$top` (max 999) and `@odata.nextLink` pagination.
- **Delta queries**: Supports `$deltaLink` for incremental sync — only returns users changed since last call. Ideal for FR-006.
- **Disabled accounts**: `accountEnabled=false` identifies departed users for FR-014.

### SDK Integration
- **Package**: `msgraph-sdk` v1.55.0 (v1.0 endpoint)
- **Import**: `from msgraph import GraphServiceClient`
- **Call pattern**: `await client.users.get(request_configuration)` with query parameters for `$select`, `$expand`, `$top`.
- **Delta queries**: `await client.users.delta.get()` for incremental sync; store `odata_delta_link` token.
- **Pagination**: Follow `odata_next_link` property on response; use `client.users.with_url(next_link).get()`.
- **Error handling**: Catch `APIError` from `kiota_abstractions.api_error`.

### Alternatives Considered
- **Separate license assignment API** (`/v1.0/users/{id}/licenseDetails`): Requires one call per user — prohibitive at 50K+ users.
- **Organization API** (`/v1.0/organization`): Returns org-level license counts but not per-user assignments.
- **Raw `msal` + `requests`**: Lower-level but requires manual HTTP handling, pagination, serialization. The official SDK provides typed models and built-in pagination.

### Impact on Design
- Use delta queries for daily incremental sync (watermark = `deltaLink` token).
- Store the full `licenseDetails` array in Bronze; extract Copilot SKU presence in Silver.
- Must handle pagination loop for organizations with >999 users.

---

## Research Task 3: Microsoft Graph API — Audit Logs (Purview Audit Search)

### Decision
Use `POST /beta/security/auditLog/queries` (Purview Audit Search API) to create audit log queries and retrieve records via the async query pattern.

### Rationale
The Purview Audit Search API (`/beta/security/auditLog/queries`) is the recommended approach for querying Microsoft 365 audit data. It provides richer filtering (by service, operation, record type, UPN, IP), covers all M365 workloads including Copilot activities, and supports longer retention (up to 180 days with E5/G5). The older `directoryAudits` endpoint is limited to Azure AD/Entra activities only.

### Key Findings
- **Create query**: `POST /beta/security/auditLog/queries` with JSON body defining filters
- **Poll status**: `GET /beta/security/auditLog/queries/{id}` until `status == "succeeded"`
- **Retrieve records**: `GET /beta/security/auditLog/queries/{id}/records` with pagination
- **Permission**: `AuditLogsQuery.Read.All` (application) — covers all workloads. Service-specific alternatives: `AuditLogsQuery-Entra.Read.All`, `AuditLogsQuery-Exchange.Read.All`, etc.
- **Query filters** (set at creation time):
  - `filterStartDateTime` / `filterEndDateTime` — date range
  - `recordTypeFilters` — e.g., `["copilotInteraction"]` or specific record types
  - `serviceFilter` — e.g., `"MicrosoftCopilot"`
  - `operationFilters` — specific operation names
  - `userPrincipalNameFilters` — restrict to specific users
  - `keywordFilter` — free-text search on non-indexed properties
- **Record schema** (`auditLogRecord`):
  - `id`, `createdDateTime`, `operation`, `service`, `organizationId`
  - `userPrincipalName`, `userId`, `userType`, `clientIp`
  - `objectId` — target resource path
  - `auditData` — JSON object with service-specific detail
  - `auditLogRecordType` — enum (e.g., `copilotInteraction`, `azureActiveDirectory`)
  - `administrativeUnits` — tagged admin units
- **Async pattern**: Query creation returns immediately with `status: "notStarted"`. Must poll until `status: "succeeded"` before listing records. Possible statuses: `notStarted`, `running`, `succeeded`, `failed`, `cancelled`.
- **Pagination**: Records endpoint supports `@odata.nextLink`.
- **Volume**: Large tenants may have millions of audit records; date-range filtering at query creation is essential.
- **Retention**: 90 days (E3/G3) or 180 days (E5/G5) for Microsoft 365 audit records. Our indefinite retention in Lakehouse fills this gap.

### SDK Integration
- **Package**: `msgraph-beta-sdk` v1.56.0 (beta endpoint)
- **Import**: `from msgraph_beta import GraphServiceClient`
- **Create query**:
  ```python
  from msgraph_beta.generated.models.security import AuditLogQuery

  query_body = AuditLogQuery(
      display_name="CopilotAuditIngestion",
      filter_start_date_time=watermark_dt,
      filter_end_date_time=now_dt,
      service_filter="MicrosoftCopilot",
  )
  query = await client.security.audit_log.queries.post(query_body)
  ```
- **Poll status**: `await client.security.audit_log.queries.by_audit_log_query_id(query.id).get()` — repeat until `status == "succeeded"`.
- **Retrieve records**: `await client.security.audit_log.queries.by_audit_log_query_id(query.id).records.get()` — follow `odata_next_link` for pagination.
- **Error handling**: Catch `APIError` from `kiota_abstractions.api_error`.

### Alternatives Considered
- **Directory Audits** (`/beta/auditLogs/directoryAudits`): Only covers Azure AD/Entra events. Does not include Copilot interaction events or other M365 workload audit data.
- **Microsoft 365 Management Activity API**: Provides Office 365 audit activities but requires separate subscription setup and webhook-based delivery. More complex to implement.

### Impact on Design
- Ingestion notebook uses a **3-step async pattern**: create query → poll status → retrieve records.
- Watermark-based incremental loading using `filterStartDateTime` / `filterEndDateTime` at query creation.
- Bronze stores raw `auditLogRecord` objects; Silver normalises and enriches with user context.
- Rate limiting: poll interval should use exponential backoff (start 5s, max 60s) to avoid excessive polling.
- Query results include all M365 workload audit data; `serviceFilter` and `recordTypeFilters` narrow to Copilot-relevant events.

---

## Research Task 4: Fabric CI/CD Library (fabric-cicd v0.2.0)

### Decision
Use `fabric-cicd` v0.2.0 Python library for automated deployment of all Fabric items via GitHub Actions.

### Rationale
`fabric-cicd` is the official Microsoft-supported deployment library for Fabric, supporting all required item types (Notebook, DataPipeline, Lakehouse, Environment, SemanticModel). It handles parameterisation natively via `parameter.yml`, aligning perfectly with Constitution Principle VIII.

### Key Findings
- **Package**: `pip install fabric-cicd` (v0.2.0)
- **Core API**:
  ```python
  from fabric_cicd import FabricWorkspace
  ws = FabricWorkspace(
      workspace_id="<guid>",
      environment="PROD",
      repository_directory="/path/to/workspace/items",
      item_type_in_scope=["Notebook", "DataPipeline", "Environment", "Lakehouse"]
  )
  ws.publish_all_items()
  ws.unpublish_all_orphan_items()
  ```
- **Supported item types**: Notebook, DataPipeline, Environment, Lakehouse, SemanticModel, Report
- **Parameterisation** (`parameter.yml`):
  - `find_replace`: String-level find-and-replace across all files. Supports regex via `is_regex: "true"`.
  - `key_value_replace`: JSONPath-based replacement in JSON/YAML files (pipelines, schedules).
  - `spark_pool`: Spark pool configuration replacement per environment.
  - `semantic_model_binding`: Connection rebinding for Semantic Models per environment.
  - Dynamic variables: `$items.<type>.<name>.$id`, `$workspace.$id` for target-environment GUIDs.
  - File filters: `item_type`, `item_name`, `file_path` for scoped replacements.
  - Template extension: `extend` key to split large parameter files into modular templates.
- **Notebook parameterisation pattern**:
  - Use regex `find_value` to match `default_lakehouse` GUID in `# META` headers.
  - Replace with `$items.Lakehouse.<name>.$id` for dynamic resolution at deploy time.
  - Also parameterise `default_lakehouse_workspace_id` with `$workspace.$id`.
- **Pipeline parameterisation pattern**:
  - Use `key_value_replace` to update Notebook references in pipeline JSON: `$.properties.activities[?(@.name=="...")].typeProperties.notebookId` → `$items.Notebook.<name>.$id`.
  - Schedule enabling/disabling per environment via `$.schedules[?(@.jobType=="Execute")].enabled`.
- **Authentication**: Uses `DefaultAzureCredential` or explicit `TokenCredential`.
- **CI/CD integration**: GitHub Actions workflows call the Python API in a deployment script.

### Alternatives Considered
- **Fabric REST API directly**: More flexible but requires implementing publish/unpublish orchestration, parameter substitution logic, and item dependency ordering ourselves.
- **Fabric Deployment Pipelines (UI)**: Violates the "no Fabric UI" constraint.

### Impact on Design
- Repository structure must follow `<ItemName>.<ItemType>/` convention (e.g., `01_ingest_users.Notebook/notebook-content.py`).
- `parameter.yml` at workspace items root handles all DEV→PROD environment swaps.
- Regex-based Lakehouse GUID replacement is the recommended pattern for notebooks.
- GitHub Actions workflow triggers `publish_all_items()` on merge to `main`.

---

## Research Task 5: Fabric CLI (ms-fabric-cli v1.4.0)

### Decision
Use Fabric CLI (`fab`) for workspace provisioning, item creation, notebook import, pipeline execution, and operational management — all from the command line.

### Rationale
The Fabric CLI provides full item management capability without requiring the Fabric UI, directly satisfying the user's requirement that "everything must be created from CLI." It supports Service Principal authentication for headless CI/CD execution.

### Key Findings
- **Package**: `pip install ms-fabric-cli` (v1.4.0)
- **Authentication**:
  ```bash
  fab auth login -u <client_id> -p <client_secret> --tenant <tenant_id>
  ```
- **Workspace operations**:
  - `fab create <workspace>.Workspace` — create workspace
  - `fab ls` — list workspaces
  - `fab cd <workspace>.Workspace` — navigate to workspace
- **Item creation**:
  - `fab create <ws>.Workspace/<name>.Lakehouse` — create Lakehouse
  - `fab create <ws>.Workspace/<name>.Lakehouse -P enableSchemas=true` — with parameters
  - `fab create <ws>.Workspace/<name>.Notebook` — create Notebook
  - `fab create <ws>.Workspace/<name>.DataPipeline` — create Data Pipeline
  - `fab create <ws>.Workspace/<name>.Environment` — create Environment
- **Import/Export**:
  - `fab import <ws>.Workspace/<name>.Notebook -i /local/path/notebook.Notebook` — import notebook definition
  - `fab export <ws>.Workspace/<name>.Notebook -o /tmp` — export notebook definition
  - Supported import formats: `.ipynb` (default) and `.py`
- **Item properties**:
  - `fab set <ws>.Workspace/<nb>.Notebook -q definition.parts[0].payload.metadata.dependencies.lakehouse -i '<json>'` — set default lakehouse
  - `fab set <ws>.Workspace/<nb>.Notebook -q environment -i '<json>'` — set default environment
- **Execution**:
  - `fab run <ws>.Workspace/<pipeline>.DataPipeline` — trigger pipeline run
  - `fab start <ws>.Workspace/<notebook>.Notebook` — start notebook session
- **Listing & querying**:
  - `fab ls <ws>.Workspace -l` — detailed listing
  - `fab ls <ws>.Workspace -q [?contains(name, 'Notebook')]` — JMESPath filtering
  - `fab get <ws>.Workspace/<item> -q <property>` — query specific properties
- **Deletion**:
  - `fab rm <ws>.Workspace/<item> -f` — force remove without confirmation

### Alternatives Considered
- **Fabric REST API (raw HTTP)**: More granular control but requires coding HTTP client logic, auth token management, and API versioning.
- **Fabric UI**: Violates the zero-UI constraint.
- **fabric-cicd publish**: Handles deployment but not initial workspace provisioning or ad-hoc operations.

### Impact on Design
- `quickstart.md` will use `fab` commands for all setup steps.
- Workspace scaffolding script uses `fab create` for Lakehouse, Environment, Notebooks.
- `fab import` used to push exported notebook definitions from Git into workspace.
- `fab run` used in GitHub Actions smoke-test step to trigger pipeline validation.
- `fab set` used to wire notebook→lakehouse and notebook→environment bindings.

---

## Research Task 6: Authentication Architecture

### Decision
Use Azure AD Service Principal with client credentials flow via `azure-identity` + Microsoft Graph Python SDKs (`msgraph-sdk`, `msgraph-beta-sdk`) for Graph API authentication, and `DefaultAzureCredential` for Fabric CI/CD deployment.

### Rationale
Service Principal is the only authentication method that satisfies Constitution Principle I (zero-trust, non-interactive) for automated pipelines. The official Microsoft Graph SDKs provide typed clients, built-in pagination, and automatic token management via `azure-identity`, eliminating raw HTTP handling.

### Key Findings
- **Graph API auth** (notebooks):
  ```python
  from azure.identity.aio import ClientSecretCredential
  from msgraph import GraphServiceClient          # v1.0 endpoints
  from msgraph_beta import GraphServiceClient as BetaGraphServiceClient  # beta endpoints

  credential = ClientSecretCredential(
      tenant_id=tenant_id,
      client_id=client_id,
      client_secret=client_secret
  )
  scopes = ['https://graph.microsoft.com/.default']

  # v1.0 client (users, licenses)
  client = GraphServiceClient(credential, scopes=scopes)

  # Beta client (usage reports, audit logs)
  beta_client = BetaGraphServiceClient(credential, scopes=scopes)
  ```
- **SDK packages**:
  - `msgraph-sdk` v1.55.0 — v1.0 Graph endpoints (users, licenses)
  - `msgraph-beta-sdk` v1.56.0 — beta Graph endpoints (Copilot usage reports, audit logs)
  - `azure-identity` — authentication provider (`ClientSecretCredential`)
- **Required Graph permissions** (application, not delegated):
  - `User.Read.All` — user profiles and license details
  - `Reports.Read.All` — Copilot usage reports
  - `AuditLogsQuery.Read.All` — Purview Audit Search queries and records
  - `Organization.Read.All` — org-level license counts (optional)
- **Fabric CLI auth**:
  ```bash
  fab auth login -u $CLIENT_ID -p $CLIENT_SECRET --tenant $TENANT_ID
  ```
- **fabric-cicd auth**: Uses `DefaultAzureCredential` → works with GitHub Actions OIDC federation or environment variables (`AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_CLIENT_SECRET`).
- **Key Vault integration**: Notebooks retrieve secrets at runtime via `notebookutils.credentials.getSecret(vault_url, secret_name)`.
- **Token management**: `azure-identity` handles token acquisition, caching, and refresh automatically. No manual token lifecycle code needed. Token lifetime is typically 60 minutes.
- **Async-first**: Both SDKs use async/await. In Fabric notebooks, wrap calls with `asyncio.run()` or use `nest_asyncio` if the event loop is already running.

### Alternatives Considered
- **`msal` + `requests` (raw HTTP)**: Lower-level approach; requires manual HTTP calls, JSON parsing, pagination loops, and token refresh logic. The official SDK handles all of this natively.
- **Delegated permissions (user context)**: Violates P-I; requires interactive login, not suitable for scheduled pipelines.
- **Managed Identity**: Ideal for Azure-hosted services but Fabric notebooks currently have limited support for workspace-level managed identity for Graph API calls.

### Impact on Design
- Three secrets stored in Key Vault: `graph-client-id`, `graph-client-secret`, `graph-tenant-id`.
- `parameter.yml` stores Key Vault URL and secret names (never values).
- Each ingestion notebook creates a `ClientSecretCredential` and the appropriate `GraphServiceClient` at the start of execution.
- `helpers.Notebook` provides a shared factory function to build v1.0 and beta Graph clients from Key Vault secrets.
- GitHub Actions uses environment secrets for `fab auth login` and `DefaultAzureCredential`.

---

## Research Task 7: Incremental Loading Patterns for Delta Lake

### Decision
Use watermark-based incremental strategies per source:
- **Users**: Microsoft Graph delta queries (`$deltaLink`).
- **Usage reports**: `reportRefreshDate` as high-watermark; append new periods.
- **Audit logs**: `filterEndDateTime` as high-watermark; create query with date range, poll, retrieve new records.
- **Silver/Gold**: Delta `MERGE INTO` with surrogate/natural keys.

### Rationale
Each source has a different change-tracking mechanism. Using the native change-tracking per API (delta queries for users, date-based watermarks for reports and audit logs) minimises API calls and data transfer while satisfying FR-006 and Constitution Principle IV.

### Key Findings
- **Graph delta queries**: Initial sync returns full dataset + `deltaLink`. Subsequent calls with `deltaLink` return only changed users. Stores `deltaLink` token in a watermark table.
- **Usage reports watermark**: Store `max(reportRefreshDate)` per source table. Next run fetches reports where `reportRefreshDate > watermark`.
- **Audit log watermark**: Store `max(filterEndDateTime)` of last successful query. Next run creates a new query with `filterStartDateTime = watermark`.
- **Delta MERGE for Silver**: Match on natural key (e.g., `userPrincipalName`), update changed fields, insert new records.
- **Delta MERGE for Gold**: Match on surrogate key + date, update scores/flags, insert new date records.
- **Watermark table**: A dedicated `_watermarks` Delta table stores `{source, last_watermark_value, last_run_timestamp}`.

### Alternatives Considered
- **Full refresh daily**: Simple but violates Constitution Principle IV and is inefficient at 50K users.
- **CDC with Spark Structured Streaming**: Overkill for daily batch; adds operational complexity.

### Impact on Design
- Bronze layer: Append-only; each run adds new partition (`ingestion_date=YYYY-MM-DD`).
- Silver layer: `MERGE INTO` using natural keys; upsert pattern.
- Gold layer: `MERGE INTO` for dimension tables; insert-only for fact tables (score history).
- Watermark management notebook (`00_watermarks.py`) tracks all source watermarks.

---

## Research Task 8: Usage Score Computation Model

### Decision
Implement a three-component scoring model: 50% recency, 30% frequency, 20% breadth, producing a 0–100 integer score.

### Rationale
Per clarification, the stakeholder chose Option B (recency-weighted). This model rewards recent activity most heavily, which aligns with the primary goal of identifying licenses that should be reallocated.

### Key Findings
- **Recency (50%)**: Days since last Copilot activity across any app. Score = `max(0, 100 - (days_since_last_activity * 100 / 30))`. 0 days = 100, 30+ days = 0.
- **Frequency (30%)**: Number of active days in the reporting period (last 30 days). Score = `(active_days / 30) * 100`.
- **Breadth (20%)**: Number of distinct Copilot-enabled apps used in the period. Score = `(apps_used / total_copilot_apps) * 100`. Total apps = 8 (Teams, Word, Excel, PowerPoint, Outlook, OneNote, Loop, Copilot Chat).
- **Final score**: `round(recency * 0.50 + frequency * 0.30 + breadth * 0.20)`
- **Underutilization threshold**: Score ≤ 20 (configurable via `parameter.yml`).
- **New assignment grace**: Users with license assigned < 14 days get flagged `is_new_assignment=true` and excluded from underutilization alerts (FR-010).
- **Score history**: One record per user per score_date, retained indefinitely (FR-007).

### Alternatives Considered
- **Equal weighting (33/33/33)**: Doesn't prioritise recency, which is the strongest signal for license reallocation.
- **Machine learning model**: Excessive complexity for this phase; no training data available.

### Impact on Design
- Gold-layer `gold_copilot_usage_scores` table stores daily scores with component breakdowns.
- Scoring logic in `06_transform_gold_scores.py` notebook.
- Threshold and weight parameters stored in `parameter.yml` for tunability.
- Grace period (14 days) calculated from `licenseAssignmentDate` in Silver user-license table.
