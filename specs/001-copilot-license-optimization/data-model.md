# Data Model: Microsoft 365 Copilot License Usage Analytics

**Feature Branch**: `001-copilot-license-optimization`
**Date**: 2026-02-23
**Source**: [spec.md](spec.md), [research.md](research.md)

---

## Medallion Architecture Overview

```
┌─────────────────────────┐     ┌─────────────────────────┐     ┌─────────────────────────┐
│       BRONZE LAYER      │     │       SILVER LAYER      │     │       GOLD LAYER        │
│   (Raw API Responses)   │────▶│  (Cleaned & Conformed)  │────▶│ (Business Aggregates)   │
│                         │     │                         │     │                         │
│ bronze_users            │     │ silver_users            │     │ gold_user_license_usage │
│ bronze_licenses         │     │ silver_licenses         │     │ gold_daily_metrics      │
│ bronze_usage_reports    │     │ silver_copilot_usage    │     │ gold_department_summary │
│ bronze_m365_activity    │     │ silver_m365_activity    │     │                         │
│ bronze_audit_logs       │     │ silver_copilot_interact │     │                         │
└─────────────────────────┘     └─────────────────────────┘     └─────────────────────────┘
```

---

## Bronze Layer (Raw Data)

### bronze_users

Raw user profile data from Microsoft Graph API `/v1.0/users`.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| id | STRING | NO | Azure AD object ID (primary key from Graph) |
| displayName | STRING | YES | User display name |
| mail | STRING | YES | Primary email address |
| userPrincipalName | STRING | NO | User principal name (UPN) |
| department | STRING | YES | Department from Azure AD |
| officeLocation | STRING | YES | Office location |
| jobTitle | STRING | YES | Job title |
| accountEnabled | BOOLEAN | YES | Whether the account is enabled |
| _ingested_at | TIMESTAMP | NO | Timestamp of ingestion |
| _source_api | STRING | NO | API endpoint used (e.g., "/v1.0/users") |
| _batch_id | STRING | NO | Unique batch/run identifier |

**Write mode**: Append-only (immutable raw layer)
**Partitioning**: `_ingested_at` (date)
**Source notebook**: `01_ingest_graph_users.py`

### bronze_licenses

Raw license assignment data from Microsoft Graph API `/v1.0/users?$select=assignedLicenses`.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| userId | STRING | NO | Azure AD user object ID |
| assignedLicenses | STRING | YES | JSON array of assigned license objects (`[{skuId, disabledPlans}]`) |
| _ingested_at | TIMESTAMP | NO | Timestamp of ingestion |
| _source_api | STRING | NO | API endpoint used |
| _batch_id | STRING | NO | Unique batch/run identifier |

**Write mode**: Append-only
**Partitioning**: `_ingested_at` (date)
**Source notebook**: `02_ingest_graph_licenses.py`

### bronze_usage_reports

Raw Copilot usage report data from Microsoft Graph API `/beta/reports/getMicrosoft365CopilotUsageUserDetail`.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| reportRefreshDate | STRING | YES | Date the report data was last refreshed |
| reportPeriod | STRING | YES | Reporting period (e.g., "D30") |
| userPrincipalName | STRING | YES | User principal name (may be anonymized) |
| displayName | STRING | YES | User display name |
| lastActivityDate | STRING | YES | Most recent Copilot activity date (any app) |
| teamsLastActivityDate | STRING | YES | Last Copilot activity in Teams |
| wordLastActivityDate | STRING | YES | Last Copilot activity in Word |
| excelLastActivityDate | STRING | YES | Last Copilot activity in Excel |
| powerPointLastActivityDate | STRING | YES | Last Copilot activity in PowerPoint |
| outlookLastActivityDate | STRING | YES | Last Copilot activity in Outlook |
| oneNoteLastActivityDate | STRING | YES | Last Copilot activity in OneNote |
| loopLastActivityDate | STRING | YES | Last Copilot activity in Loop |
| copilotChatLastActivityDate | STRING | YES | Last Copilot Chat activity |
| _ingested_at | TIMESTAMP | NO | Timestamp of ingestion |
| _source_api | STRING | NO | API endpoint used |
| _batch_id | STRING | NO | Unique batch/run identifier |

**Write mode**: Append-only
**Partitioning**: `_ingested_at` (date)
**Source notebook**: `03_ingest_usage_reports.py`

### bronze_audit_logs

Raw Copilot interaction audit events from Graph Security Audit Log API (`/security/auditLog/queries`).

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| id | STRING | NO | Unique audit log record identifier |
| createdDateTime | STRING | YES | Event timestamp (ISO 8601) |
| userId | STRING | YES | User identifier |
| userPrincipalName | STRING | YES | User principal name |
| operation | STRING | YES | Operation type (e.g., "CopilotInteraction") |
| auditLogRecordType | STRING | YES | Record type (e.g., "CopilotInteraction") |
| service | STRING | YES | Service name (e.g., "Copilot") |
| clientIp | STRING | YES | Client IP address |
| auditData | STRING | YES | Full JSON audit data payload (contains AppHost, AccessedResources, etc.) |
| _ingested_at | TIMESTAMP | NO | Timestamp of ingestion |
| _source_api | STRING | NO | API endpoint used |
| _batch_id | STRING | NO | Unique batch/run identifier |

**Write mode**: Append-only
**Partitioning**: `_ingested_at` (date)
**Source notebook**: `04_ingest_audit_logs.py`

### bronze_m365_activity

Raw M365 productivity activity data per user from Microsoft Graph API `/v1.0/reports/getOffice365ActiveUserDetail`.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| reportRefreshDate | STRING | YES | Report data freshness date |
| reportPeriod | STRING | YES | Reporting period (e.g., "D30") |
| userPrincipalName | STRING | YES | User principal name |
| displayName | STRING | YES | User display name |
| exchangeLastActivityDate | STRING | YES | Last Exchange/Outlook activity |
| oneDriveLastActivityDate | STRING | YES | Last OneDrive activity |
| sharePointLastActivityDate | STRING | YES | Last SharePoint activity |
| teamsLastActivityDate | STRING | YES | Last Teams activity |
| wordLastActivityDate | STRING | YES | Last Word activity |
| excelLastActivityDate | STRING | YES | Last Excel activity |
| powerPointLastActivityDate | STRING | YES | Last PowerPoint activity |
| _ingested_at | TIMESTAMP | NO | Timestamp of ingestion |
| _source_api | STRING | NO | API endpoint used |
| _batch_id | STRING | NO | Unique batch/run identifier |

**Write mode**: Append-only
**Partitioning**: `_ingested_at` (date)
**Source notebook**: `03b_ingest_m365_activity.py`

---

## Silver Layer (Cleaned & Conformed)

### silver_users

Deduplicated user profiles with standardized fields. Current snapshot (SCD Type 1).

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| user_id | STRING | NO | Azure AD object ID (PK) |
| user_principal_name | STRING | NO | UPN |
| display_name | STRING | YES | Display name |
| email | STRING | YES | Primary email |
| department | STRING | YES | Department (standardized) |
| office_location | STRING | YES | Office location (standardized) |
| job_title | STRING | YES | Job title |
| is_enabled | BOOLEAN | NO | Account enabled flag |
| is_deleted | BOOLEAN | NO | Soft-delete flag for removed accounts |
| first_seen_at | TIMESTAMP | NO | First ingestion timestamp |
| last_updated_at | TIMESTAMP | NO | Most recent update timestamp |

**Write mode**: MERGE (upsert on `user_id`)
**Deduplication**: Latest record per `user_id` from Bronze
**Source notebook**: `05_transform_bronze_to_silver.py`
**Validation rules**:
- `user_id` must not be null
- `user_principal_name` must not be null
- `is_enabled` defaults to `true` if missing

### silver_licenses

Current license assignments with effective dates. One row per user per license SKU.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| user_id | STRING | NO | FK → silver_users.user_id |
| sku_id | STRING | NO | License SKU GUID |
| sku_name | STRING | YES | Human-readable SKU name (e.g., "Microsoft_365_Copilot") |
| is_copilot_license | BOOLEAN | NO | Flag indicating Copilot SKU |
| assigned_date | DATE | YES | Date license was first observed assigned |
| removed_date | DATE | YES | Date license was first observed removed (null if active) |
| is_active | BOOLEAN | NO | Whether the license is currently assigned |
| last_updated_at | TIMESTAMP | NO | Most recent update timestamp |

**Write mode**: MERGE (upsert on `user_id` + `sku_id`)
**Source notebook**: `05_transform_bronze_to_silver.py`
**Validation rules**:
- `user_id` must exist in `silver_users`
- `sku_id` must not be null
- `is_copilot_license` = `true` if `sku_id` in (`639dec6b-bb19-468b-871c-c5c441c4b0cb`, `a809996b-059e-42e2-9866-db24b99a9782`, `ad9c22b3-52d7-4e7e-973c-88121ea96436`)

### silver_copilot_usage

Parsed and normalized usage metrics per user per reporting period.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| user_principal_name | STRING | NO | UPN (join key to silver_users) |
| report_refresh_date | DATE | NO | Report data freshness date |
| report_period | STRING | NO | Reporting period (e.g., "D30") |
| last_activity_date | DATE | YES | Most recent Copilot activity (any app) |
| teams_last_activity | DATE | YES | Last Teams Copilot activity |
| word_last_activity | DATE | YES | Last Word Copilot activity |
| excel_last_activity | DATE | YES | Last Excel Copilot activity |
| powerpoint_last_activity | DATE | YES | Last PowerPoint Copilot activity |
| outlook_last_activity | DATE | YES | Last Outlook Copilot activity |
| onenote_last_activity | DATE | YES | Last OneNote Copilot activity |
| loop_last_activity | DATE | YES | Last Loop Copilot activity |
| copilot_chat_last_activity | DATE | YES | Last Copilot Chat activity |
| apps_used_count | INT | NO | Count of distinct Copilot apps with activity |
| last_updated_at | TIMESTAMP | NO | Processing timestamp |

**Write mode**: MERGE (upsert on `user_principal_name` + `report_refresh_date`)
**Source notebook**: `05_transform_bronze_to_silver.py`
**Validation rules**:
- `user_principal_name` must not be null
- `report_refresh_date` must be a valid date
- Date fields must be parseable from string format
- `apps_used_count` calculated as count of non-null app activity dates

### silver_copilot_interactions

Parsed audit events with normalized timestamps. One row per interaction event.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| event_id | STRING | NO | Unique event ID (PK) |
| user_id | STRING | YES | FK → silver_users.user_id |
| user_principal_name | STRING | YES | UPN |
| event_timestamp | TIMESTAMP | NO | Normalized event timestamp (UTC) |
| event_date | DATE | NO | Event date (derived from timestamp) |
| operation | STRING | NO | Operation type |
| app_host | STRING | YES | Application (Teams, Word, Outlook, etc.) |
| workload | STRING | YES | Workload identifier |
| accessed_resources_count | INT | YES | Number of resources accessed |
| last_updated_at | TIMESTAMP | NO | Processing timestamp |

**Write mode**: MERGE (upsert on `event_id`; deduplicate on `event_id`)
**Partitioning**: `event_date`
**Source notebook**: `05_transform_bronze_to_silver.py`
**Validation rules**:
- `event_id` must not be null
- `event_timestamp` must be parseable as ISO 8601
- `operation` must not be null

### silver_m365_activity

Normalized M365 productivity activity per user per reporting period. Used to compute `m365_activity_score` in Gold.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| user_principal_name | STRING | NO | UPN (join key to silver_users) |
| report_refresh_date | DATE | NO | Report data freshness date |
| report_period | STRING | NO | Reporting period (e.g., "D30") |
| exchange_last_activity | DATE | YES | Last Exchange/Outlook activity |
| onedrive_last_activity | DATE | YES | Last OneDrive activity |
| sharepoint_last_activity | DATE | YES | Last SharePoint activity |
| teams_last_activity | DATE | YES | Last Teams activity |
| word_last_activity | DATE | YES | Last Word activity |
| excel_last_activity | DATE | YES | Last Excel activity |
| powerpoint_last_activity | DATE | YES | Last PowerPoint activity |
| active_services_count | INT | NO | Count of M365 services with activity in period |
| last_updated_at | TIMESTAMP | NO | Processing timestamp |

**Write mode**: MERGE (upsert on `user_principal_name` + `report_refresh_date`)
**Source notebook**: `05_transform_bronze_to_silver.py`
**Validation rules**:
- `user_principal_name` must not be null
- `report_refresh_date` must be a valid date
- `active_services_count` calculated as count of non-null service activity dates

---

## Gold Layer (Business Aggregates)

### gold_user_license_usage

One row per user with comprehensive usage score, license status, and recommendation. This is the primary consumption table for the Power BI report.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| user_id | STRING | NO | FK → silver_users.user_id (PK) |
| user_principal_name | STRING | NO | UPN |
| display_name | STRING | YES | User display name |
| email | STRING | YES | Primary email |
| department | STRING | YES | Department |
| office_location | STRING | YES | Office location |
| job_title | STRING | YES | Job title |
| is_enabled | BOOLEAN | NO | Account enabled |
| has_copilot_license | BOOLEAN | NO | Whether user has any Copilot license |
| license_assigned_date | DATE | YES | Earliest Copilot license assignment date |
| last_activity_date | DATE | YES | Most recent Copilot activity date |
| days_since_last_activity | INT | YES | Days since last activity (null if never active) |
| interaction_count_30d | INT | NO | Count of Copilot interactions in last 30 days |
| apps_used_count | INT | NO | Distinct Copilot apps used in last 30 days |
| frequency_score | DOUBLE | NO | Frequency component (0.0-1.0) |
| breadth_score | DOUBLE | NO | Breadth component (0.0-1.0) |
| recency_score | DOUBLE | NO | Recency component (0.0-1.0) |
| usage_score | DOUBLE | NO | Composite usage score (0.0-1.0) |
| usage_category | STRING | NO | "Active", "Low Usage", or "Inactive" |
| recommendation | STRING | NO | "Retain", "Monitor", "Recommend Remove", or "Candidate for License" |
| m365_activity_score | DOUBLE | YES | M365 productivity activity score (for unlicensed candidates) |
| last_calculated_at | TIMESTAMP | NO | Score calculation timestamp |

**Write mode**: Overwrite (full recalculation from Silver)
**Source notebook**: `06_transform_silver_to_gold.py`

### gold_daily_metrics

Daily aggregate metrics for trend analysis. One row per date.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| metric_date | DATE | NO | Date (PK) |
| total_users | INT | NO | Total enabled users in organization |
| total_copilot_licensed | INT | NO | Users with Copilot license |
| active_users | INT | NO | Users with activity in last 30 days |
| low_usage_users | INT | NO | Users classified as "Low Usage" |
| inactive_users | INT | NO | Users classified as "Inactive" |
| utilization_rate | DOUBLE | NO | active_users / total_copilot_licensed |
| total_interactions | LONG | NO | Sum of all Copilot interactions |
| avg_usage_score | DOUBLE | NO | Average usage score across licensed users |
| estimated_monthly_savings | DOUBLE | YES | inactive_users * license_cost_per_month |
| last_calculated_at | TIMESTAMP | NO | Calculation timestamp |

**Write mode**: MERGE (upsert on `metric_date`)
**Source notebook**: `06_transform_silver_to_gold.py`

### gold_department_summary

Aggregated metrics by department. One row per department per calculation date.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| department | STRING | NO | Department name (PK component) |
| metric_date | DATE | NO | Calculation date (PK component) |
| total_users | INT | NO | Total users in department |
| copilot_licensed_users | INT | NO | Users with Copilot license |
| active_users | INT | NO | Users classified as "Active" |
| low_usage_users | INT | NO | Users classified as "Low Usage" |
| inactive_users | INT | NO | Users classified as "Inactive" |
| utilization_rate | DOUBLE | NO | active_users / copilot_licensed_users |
| avg_usage_score | DOUBLE | NO | Average usage score in department |
| total_interactions | LONG | NO | Total interactions in department |
| last_calculated_at | TIMESTAMP | NO | Calculation timestamp |

**Write mode**: MERGE (upsert on `department` + `metric_date`)
**Source notebook**: `06_transform_silver_to_gold.py`

---

## Control & Logging Tables

### control_pipeline_state

Tracks pipeline execution state for incremental loading.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| source_name | STRING | NO | Data source identifier (PK) |
| last_refresh_timestamp | TIMESTAMP | YES | Last successful extraction time |
| delta_link | STRING | YES | Graph delta query continuation token |
| last_content_uri | STRING | YES | Last processed content URI (audit logs) |
| records_processed | LONG | YES | Records processed in last run |
| status | STRING | NO | "SUCCESS", "PARTIAL", "FAILED" |
| last_updated_at | TIMESTAMP | NO | State update timestamp |

### logs_execution_errors

Error log table for pipeline monitoring.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| error_id | STRING | NO | Unique error identifier (PK) |
| notebook_name | STRING | NO | Source notebook name |
| error_timestamp | TIMESTAMP | NO | When the error occurred |
| error_type | STRING | NO | Error category (e.g., "API_RATE_LIMIT", "SCHEMA_VALIDATION", "TRANSFORM_ERROR") |
| error_message | STRING | YES | Error message text |
| stack_trace | STRING | YES | Stack trace (if available) |
| source_api | STRING | YES | API endpoint that caused the error |
| http_status_code | INT | YES | HTTP status code (for API errors) |
| records_affected | LONG | YES | Number of records affected |
| batch_id | STRING | YES | Batch identifier for correlation |
| is_critical | BOOLEAN | NO | Whether this error should halt pipeline |

### logs_data_quality

Data quality check results.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| check_id | STRING | NO | Unique check identifier (PK) |
| check_timestamp | TIMESTAMP | NO | When the check was executed |
| table_name | STRING | NO | Table being checked |
| check_type | STRING | NO | "ROW_COUNT", "NULL_PERCENTAGE", "FRESHNESS", "REFERENTIAL_INTEGRITY" |
| check_description | STRING | YES | Human-readable description |
| expected_value | STRING | YES | Expected threshold or value |
| actual_value | STRING | YES | Actual measured value |
| passed | BOOLEAN | NO | Whether the check passed |
| batch_id | STRING | YES | Batch identifier |

---

## Semantic Model (Star Schema)

### fact_user_usage

Central fact table for Power BI reporting.

| Column | Type | Description |
|--------|------|-------------|
| UserKey | INT | Surrogate key → dim_user |
| DateKey | INT | Surrogate key → dim_date (YYYYMMDD) |
| DepartmentKey | INT | Surrogate key → dim_department |
| LicenseKey | INT | Surrogate key → dim_license |
| InteractionCount | INT | Number of Copilot interactions |
| AppsUsedCount | INT | Distinct Copilot apps used |
| FrequencyScore | DECIMAL | Frequency component (0-1) |
| BreadthScore | DECIMAL | Breadth component (0-1) |
| RecencyScore | DECIMAL | Recency component (0-1) |
| UsageScore | DECIMAL | Composite usage score (0-1) |
| UsageCategory | STRING | "Active", "Low Usage", "Inactive" |
| Recommendation | STRING | License recommendation |
| DaysSinceLastActivity | INT | Days since last Copilot activity |

### dim_user

| Column | Type | Description |
|--------|------|-------------|
| UserKey | INT | Surrogate key (PK) |
| UserID | STRING | Azure AD object ID (business key) |
| UserPrincipalName | STRING | UPN |
| DisplayName | STRING | Display name |
| Email | STRING | Email address |
| JobTitle | STRING | Job title |
| IsEnabled | BOOLEAN | Account status |
| HasCopilotLicense | BOOLEAN | Copilot license flag |
| LicenseAssignedDate | DATE | License assignment date |

### dim_date

| Column | Type | Description |
|--------|------|-------------|
| DateKey | INT | YYYYMMDD format (PK) |
| Date | DATE | Calendar date |
| Year | INT | Year |
| Quarter | INT | Quarter (1-4) |
| Month | INT | Month (1-12) |
| MonthName | STRING | Month name |
| Week | INT | ISO week number |
| DayOfWeek | INT | Day of week (1-7) |
| DayName | STRING | Day name |
| IsWeekend | BOOLEAN | Weekend flag |
| IsBusinessDay | BOOLEAN | Business day flag |

### dim_department

| Column | Type | Description |
|--------|------|-------------|
| DepartmentKey | INT | Surrogate key (PK) |
| DepartmentName | STRING | Department name (business key) |
| Location | STRING | Primary office location |
| TotalUsers | INT | Total users in department |
| CopilotLicensedUsers | INT | Licensed users count |

### dim_license

| Column | Type | Description |
|--------|------|-------------|
| LicenseKey | INT | Surrogate key (PK) |
| SkuID | STRING | License SKU GUID (business key) |
| SkuName | STRING | License name |
| IsCopilotLicense | BOOLEAN | Copilot license flag |
| CostPerMonth | DECIMAL | License cost per user per month |

### Relationships

```
dim_user (1) ──── (*) fact_user_usage
dim_date (1) ──── (*) fact_user_usage
dim_department (1) ──── (*) fact_user_usage
dim_license (1) ──── (*) fact_user_usage
```

All relationships are one-to-many, uni-directional (dimension → fact). No bi-directional cross-filtering.

---

## State Transitions

### User Account Status

```
Active ──[disabled]──▶ Disabled ──[deleted]──▶ Deleted (soft)
  ▲                       │
  └──[re-enabled]─────────┘
```

- **Active**: `is_enabled = true`, `is_deleted = false`
- **Disabled**: `is_enabled = false`, `is_deleted = false` — retained in analytics with flag
- **Deleted**: `is_enabled = false`, `is_deleted = true` — historical data preserved, excluded from active metrics

### License Assignment Status

```
Unassigned ──[assigned]──▶ Active ──[removed]──▶ Removed
                             ▲                      │
                             └──[reassigned]────────┘
```

- **Active**: `is_active = true`, `removed_date = null`
- **Removed**: `is_active = false`, `removed_date` populated — tracked for historical analysis

### Usage Classification

```
                  ┌──────────────────────────────────┐
                  ▼                                  │
Inactive ──[activity]──▶ Low Usage ──[more activity]──▶ Active
  ▲                         │                           │
  │                         │                           │
  └──[30+ days no use]──────┘                           │
  └──[30+ days no use]─────────────────────────────────┘
```

Recalculated daily based on rolling 30-day window.

---

## Data Lineage

```
Microsoft Graph API
  /v1.0/users ──────────────▶ bronze_users ────────▶ silver_users ───┐
  /v1.0/users?assignedLic ──▶ bronze_licenses ─────▶ silver_licenses │
  /beta/reports/Copilot ────▶ bronze_usage_reports ▶ silver_copilot_ │
                                                     usage ──────────│─▶ gold_user_license_usage
  /v1.0/reports/M365Active ▶ bronze_m365_activity ▶ silver_m365_    │   gold_daily_metrics
                                                     activity ───────│   gold_department_summary
  /security/auditLog ──────▶ bronze_audit_logs ───▶ silver_copilot_ │
                                                     interactions ───┘
                                                                               │
                                                                               ▼
                                                                     Power BI Semantic Model
                                                                     (fact_user_usage + dims)
```
