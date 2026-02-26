# Data Model: M365 Copilot License Usage Analytics

**Feature Branch**: `001-copilot-license-usage`
**Date**: 2026-02-25
**Lakehouse**: `CopilotUsageLakehouse`
**Format**: Delta Lake (all tables)

## Layer Overview

| Layer | Purpose | Loading Strategy | Retention |
|-------|---------|-----------------|-----------|
| Bronze | Raw API responses, append-only | Append per `ingestion_date` partition | Indefinite |
| Silver | Validated, cleansed, deduplicated | Delta MERGE on natural keys | Indefinite |
| Gold | Business aggregates, scoring, reporting | Delta MERGE (dims) / Insert (facts) | Indefinite |
| System | Watermarks | Upsert | Indefinite |

## Naming Convention

Tables follow `<layer>_<domain>_<entity>` per Constitution Data Standards.

---

## Bronze Layer

### `bronze_graph_users`

Raw user profile + license details response from Microsoft Graph.

| Column | Type | Nullable | Source |
|--------|------|----------|--------|
| `ingestion_date` | `DATE` | No | Pipeline run date |
| `ingestion_run_id` | `STRING` | No | Unique pipeline run identifier |
| `user_id` | `STRING` | No | `id` from Graph API |
| `raw_json` | `STRING` | No | Full JSON object per user (profile + licenseDetails) |

**Partitioned by**: `ingestion_date`
**Source**: `GET /v1.0/users?$select=id,displayName,mail,userPrincipalName,department,jobTitle,accountEnabled,companyName,officeLocation&$expand=licenseDetails`

### `bronze_graph_usage_reports`

Raw Copilot usage report rows from Microsoft Graph beta.

| Column | Type | Nullable | Source |
|--------|------|----------|--------|
| `ingestion_date` | `DATE` | No | Pipeline run date |
| `ingestion_run_id` | `STRING` | No | Unique pipeline run identifier |
| `report_refresh_date` | `DATE` | No | `reportRefreshDate` from API |
| `report_period` | `STRING` | No | `reportPeriod` (e.g., `D7`) |
| `raw_json` | `STRING` | No | Full JSON object per user-report row |

**Partitioned by**: `ingestion_date`
**Source**: `GET /beta/reports/getMicrosoft365CopilotUsageUserDetail(period='D7')`

### `bronze_graph_audit_logs`

Raw audit log records from Microsoft Graph Purview Audit Search API.

| Column | Type | Nullable | Source |
|--------|------|----------|--------|
| `ingestion_date` | `DATE` | No | Pipeline run date |
| `ingestion_run_id` | `STRING` | No | Unique pipeline run identifier |
| `event_id` | `STRING` | No | `id` from `auditLogRecord` |
| `created_date_time` | `TIMESTAMP` | No | `createdDateTime` from `auditLogRecord` |
| `raw_json` | `STRING` | No | Full JSON object per audit record |

**Partitioned by**: `ingestion_date`
**Source**: `POST /beta/security/auditLog/queries` → poll → `GET /beta/security/auditLog/queries/{id}/records`

---

## Silver Layer

### `silver_users`

Validated, deduplicated user profile records. One row per user (latest state).

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `user_id` | `STRING` | No | Azure AD object ID (natural key) |
| `user_principal_name` | `STRING` | No | UPN (email-style identifier) |
| `display_name` | `STRING` | No | Full display name |
| `mail` | `STRING` | Yes | Primary email address |
| `department` | `STRING` | Yes | Organizational department |
| `job_title` | `STRING` | Yes | Job title |
| `company_name` | `STRING` | Yes | Company name |
| `office_location` | `STRING` | Yes | Office location |
| `account_enabled` | `BOOLEAN` | No | `true` = active, `false` = departed (FR-014) |
| `is_departed` | `BOOLEAN` | No | Derived: `true` when `account_enabled = false` |
| `_ingestion_date` | `DATE` | No | Date this record was last updated |
| `_run_id` | `STRING` | No | Run ID of last update |

**Merge key**: `user_id`
**Update strategy**: Delta MERGE — update all fields on match, insert on no match.

### `silver_user_licenses`

Validated license assignment records. One row per user (current license state).

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `user_id` | `STRING` | No | FK → `silver_users.user_id` |
| `has_copilot_license` | `BOOLEAN` | No | `true` if Copilot SKU found in `licenseDetails` |
| `copilot_sku_id` | `STRING` | Yes | SKU GUID (null if no Copilot license) |
| `license_assignment_date` | `DATE` | Yes | Earliest observed date with Copilot license |
| `license_removal_date` | `DATE` | Yes | Date license was first observed as removed |
| `all_license_skus` | `ARRAY<STRING>` | No | All assigned SKU part numbers |
| `_ingestion_date` | `DATE` | No | Date this record was last updated |
| `_run_id` | `STRING` | No | Run ID of last update |

**Merge key**: `user_id`
**Update strategy**: Delta MERGE — update license fields on change, track `license_assignment_date` (retain earliest), set `license_removal_date` when Copilot SKU disappears.

### `silver_copilot_usage`

Validated per-user Copilot usage records. One row per user per report refresh date.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `user_principal_name` | `STRING` | No | UPN (join key to `silver_users`) |
| `report_refresh_date` | `DATE` | No | Report data as-of date |
| `report_period` | `STRING` | No | Reporting period (D7, D30, etc.) |
| `last_activity_date` | `DATE` | Yes | Most recent Copilot activity (any app) |
| `teams_last_activity_date` | `DATE` | Yes | Last Teams Copilot activity |
| `word_last_activity_date` | `DATE` | Yes | Last Word Copilot activity |
| `excel_last_activity_date` | `DATE` | Yes | Last Excel Copilot activity |
| `powerpoint_last_activity_date` | `DATE` | Yes | Last PowerPoint Copilot activity |
| `outlook_last_activity_date` | `DATE` | Yes | Last Outlook Copilot activity |
| `onenote_last_activity_date` | `DATE` | Yes | Last OneNote Copilot activity |
| `loop_last_activity_date` | `DATE` | Yes | Last Loop Copilot activity |
| `copilot_chat_last_activity_date` | `DATE` | Yes | Last Copilot Chat activity |
| `_ingestion_date` | `DATE` | No | Date this record was ingested |
| `_run_id` | `STRING` | No | Run ID of ingestion |

**Merge key**: `user_principal_name` + `report_refresh_date`
**Update strategy**: Delta MERGE — upsert on composite key.

### `silver_audit_events`

Validated, normalised audit log records from Purview Audit Search. One row per event.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `event_id` | `STRING` | No | Unique audit record identifier (natural key) |
| `created_date_time` | `TIMESTAMP` | No | When the event occurred |
| `operation` | `STRING` | No | Operation name (user/admin activity) |
| `service` | `STRING` | No | Microsoft service where the activity occurred |
| `record_type` | `STRING` | No | Audit log record type (e.g., `copilotInteraction`) |
| `user_principal_name` | `STRING` | Yes | Actor UPN |
| `user_id` | `STRING` | Yes | Actor user identifier |
| `user_type` | `STRING` | Yes | User type (regular, admin, servicePrincipal, etc.) |
| `client_ip` | `STRING` | Yes | IP address of the device used |
| `object_id` | `STRING` | Yes | Target resource path |
| `organization_id` | `STRING` | Yes | Organization GUID |
| `audit_data_json` | `STRING` | Yes | Service-specific audit detail (serialized JSON) |
| `_ingestion_date` | `DATE` | No | Date this record was ingested |
| `_run_id` | `STRING` | No | Run ID of ingestion |

**Merge key**: `event_id`
**Update strategy**: Delta MERGE — insert only (audit events are immutable); skip existing `event_id` values.

### `silver_users_rejected`

Quarantine table for user records failing schema validation (FR-012, P-III).

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `rejection_date` | `DATE` | No | Date the record was rejected |
| `run_id` | `STRING` | No | Pipeline run ID |
| `source_table` | `STRING` | No | Source bronze table name |
| `rule_name` | `STRING` | No | Validation rule that failed |
| `error_message` | `STRING` | No | Structured error description |
| `record_count` | `INT` | No | Number of records affected |
| `sample_payload` | `STRING` | Yes | Sample of rejected data (truncated) |

**Partitioned by**: `rejection_date`
**Note**: One `_rejected` table per Silver domain. Usage and audit also have `silver_usage_rejected` and `silver_audit_rejected` with the same schema.

---

## Gold Layer

### `gold_copilot_license_summary` (Dimension)

Current-state summary of each user's license and activity status. One row per user.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `user_id` | `STRING` | No | PK — Azure AD object ID |
| `user_principal_name` | `STRING` | No | UPN |
| `display_name` | `STRING` | No | Full name |
| `department` | `STRING` | Yes | Org department |
| `job_title` | `STRING` | Yes | Job title |
| `account_status` | `STRING` | No | `active` / `departed` (FR-014) |
| `has_copilot_license` | `BOOLEAN` | No | Current license state |
| `license_assignment_date` | `DATE` | Yes | When Copilot was assigned |
| `license_days_held` | `INT` | Yes | Days since license assignment |
| `is_new_assignment` | `BOOLEAN` | No | `true` if assigned < 14 days (FR-010) |
| `last_activity_date` | `DATE` | Yes | Most recent Copilot activity (any app) |
| `days_since_last_activity` | `INT` | Yes | Days between today and last activity |
| `apps_used_count` | `INT` | Yes | Distinct Copilot apps used (0–8) |
| `latest_usage_score` | `INT` | Yes | Most recent score (0–100) |
| `is_underutilized` | `BOOLEAN` | No | `true` if score ≤ threshold AND not new (FR-009) |
| `_last_updated` | `TIMESTAMP` | No | Last refresh timestamp |

**Merge key**: `user_id`
**Source**: Joins `silver_users` + `silver_user_licenses` + latest `silver_copilot_usage` + latest `gold_copilot_usage_scores`.

### `gold_copilot_usage_scores` (Fact — Insert-Only)

Daily usage score per licensed user. One row per user per score date. Retained indefinitely for trend analysis (FR-007).

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `user_id` | `STRING` | No | FK → `gold_copilot_license_summary.user_id` |
| `score_date` | `DATE` | No | Date the score was calculated |
| `usage_score` | `INT` | No | Final score (0–100) |
| `recency_score` | `DOUBLE` | No | Recency component (0–100) |
| `frequency_score` | `DOUBLE` | No | Frequency component (0–100) |
| `breadth_score` | `DOUBLE` | No | Breadth component (0–100) |
| `recency_weight` | `DOUBLE` | No | 0.50 (from parameter.yml) |
| `frequency_weight` | `DOUBLE` | No | 0.30 (from parameter.yml) |
| `breadth_weight` | `DOUBLE` | No | 0.20 (from parameter.yml) |
| `days_since_last_activity` | `INT` | Yes | Input: days since last activity |
| `active_days_in_period` | `INT` | Yes | Input: count of distinct dates in `silver_copilot_usage` (last 30 days) where `last_activity_date` changed from the prior snapshot |
| `apps_used_count` | `INT` | Yes | Input: distinct apps used |
| `is_underutilized` | `BOOLEAN` | No | Score ≤ threshold AND license held ≥ 14 days |
| `is_new_assignment` | `BOOLEAN` | No | License held < 14 days |
| `_run_id` | `STRING` | No | Pipeline run ID |

**Composite key**: `user_id` + `score_date`
**Loading strategy**: Insert-only — one new row per user per day. Historical scores are never updated.

**Score Component Formulas** (see FR-008):
- `recency_score = max(0, 100 − (days_since_last_activity × 100 / 30))` clamped to [0, 100]
- `frequency_score = min(100, active_days_in_period × 100 / 30)`
- `breadth_score = apps_used_count × 100 / 8`
- Final: `usage_score = round(0.50 × recency_score + 0.30 × frequency_score + 0.20 × breadth_score)`

### `gold_copilot_audit_summary` (Fact — Upsert)

Daily aggregation of audit events per user per application. One row per user per app per date.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `user_id` | `STRING` | No | FK → `gold_copilot_license_summary.user_id` |
| `activity_date` | `DATE` | No | Date of audit events |
| `application_name` | `STRING` | No | Application context |
| `event_count` | `INT` | No | Number of events |
| `success_count` | `INT` | No | Number of successful events |
| `failure_count` | `INT` | No | Number of failed events |
| `_run_id` | `STRING` | No | Pipeline run ID |

**Composite key**: `user_id` + `activity_date` + `application_name`
**Loading strategy**: Delta MERGE — upsert to handle late-arriving audit events.

### `gold_copilot_dq_results` (Operational — Append)

Data quality check results per pipeline run (P-VII, SEC-004). Placed in Gold layer per Constitution Principle VII.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `check_id` | `STRING` | No | Unique check identifier |
| `run_id` | `STRING` | No | Pipeline run ID |
| `check_date` | `TIMESTAMP` | No | When the check ran |
| `table_name` | `STRING` | No | Table being checked |
| `check_name` | `STRING` | No | e.g., `row_count`, `null_pct_email`, `freshness` |
| `check_result` | `STRING` | No | `PASS` / `WARN` / `FAIL` |
| `metric_value` | `DOUBLE` | Yes | Numeric result (count, percentage, hours) |
| `threshold` | `DOUBLE` | Yes | Threshold value |
| `details` | `STRING` | Yes | Additional context |

**Loading strategy**: Append-only — one row per check per run.

### `gold_copilot_audit_log` (Operational — Append)

Audit log for all data access and API operations (P-VII, SEC-003). Placed in Gold layer per Constitution Principle VII.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `log_id` | `STRING` | No | Unique log entry ID |
| `run_id` | `STRING` | No | Pipeline run ID |
| `timestamp` | `TIMESTAMP` | No | When the operation occurred |
| `notebook_name` | `STRING` | No | Source notebook |
| `operation` | `STRING` | No | `API_CALL`, `TABLE_READ`, `TABLE_WRITE`, `ERROR` |
| `target` | `STRING` | No | API endpoint or table name |
| `status` | `STRING` | No | `SUCCESS` / `FAILURE` / `RETRY` |
| `records_affected` | `LONG` | Yes | Number of records |
| `duration_ms` | `LONG` | Yes | Operation duration |
| `error_message` | `STRING` | Yes | Error details if failed |
| `http_status_code` | `INT` | Yes | HTTP response code for API calls |

**Loading strategy**: Append-only — one row per operation.

---

## System Tables

### `sys_watermarks`

Tracks incremental loading state per source.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `source_name` | `STRING` | No | PK — e.g., `graph_users`, `graph_usage`, `graph_audit` |
| `last_watermark_value` | `STRING` | No | Delta link token or max date value |
| `last_run_timestamp` | `TIMESTAMP` | No | When the watermark was last updated |
| `last_run_id` | `STRING` | No | Pipeline run ID |
| `records_processed` | `LONG` | Yes | Records in last run |

**Merge key**: `source_name`

---

## Entity Relationships

```
silver_users (user_id PK)
    ├── 1:1 → silver_user_licenses (user_id FK)
    ├── 1:N → silver_copilot_usage (user_principal_name FK, report_refresh_date)
    └── 1:N → silver_audit_events (user_id FK)

gold_copilot_license_summary (user_id PK)
    ├── 1:N → gold_copilot_usage_scores (user_id FK, score_date)
    ├── 1:N → gold_copilot_audit_summary (user_id FK, activity_date, application_name)
    └── (operational) gold_copilot_dq_results, gold_copilot_audit_log (run-scoped, no user FK)
```

## State Transitions

### User Account Status
```
active → departed       (when account_enabled changes false)
departed → active       (when account re-enabled — rare but supported)
```

### License Status
```
not_assigned → assigned (Copilot SKU appears in licenseDetails)
assigned → removed      (Copilot SKU disappears; license_removal_date set)
removed → assigned      (Copilot SKU reappears; license_assignment_date updated)
```

### Underutilization Flag
```
                        ┌─ is_new_assignment=true ──→ excluded (grace period)
licensed user ──────────┤
                        └─ is_new_assignment=false ──→ score ≤ 20? ──→ is_underutilized=true
                                                       score > 20? ──→ is_underutilized=false
```
