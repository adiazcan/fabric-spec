# Implementation Plan: Microsoft 365 Copilot License Usage Analytics

**Branch**: `001-copilot-license-optimization` | **Date**: 2026-02-23 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/001-copilot-license-optimization/spec.md`

## Summary

Build a Microsoft Fabric analytics solution that monitors and optimizes Microsoft 365 Copilot license usage across the organization. The solution ingests data from Microsoft Graph API (user profiles, license assignments, usage reports, audit logs) through a Bronze-Silver-Gold medallion architecture using PySpark notebooks, orchestrated by Fabric Data Pipelines on a daily schedule. A Power BI Semantic Model (star schema) and multi-page Power BI Report provide executive dashboards, user-level detail, department analysis, and license reassignment recommendations. Automated email alerts notify administrators when utilization drops below configurable thresholds.

## Technical Context

**Language/Version**: Python 3.x (PySpark), Spark SQL, DAX  
**Primary Dependencies**: msgraph-sdk (Microsoft Graph SDK), azure-identity (Azure AD auth), pyspark (Spark runtime), delta-lake (storage format)  
**Storage**: Microsoft Fabric Lakehouse with Delta Lake format on OneLake  
**Testing**: Inline data quality assertions in notebooks + dedicated data quality notebook (99_data_quality_checks.py)  
**Target Platform**: Microsoft Fabric (Lakehouse, Data Engineering, Data Factory, Power BI)  
**Project Type**: Data analytics platform (ETL pipelines + BI reporting)  
**Performance Goals**: Dashboard drill-down loads within 5 seconds (SC-007); daily pipeline completes before business hours; data no more than 24 hours stale (SC-002)  
**Constraints**: API rate limiting (Microsoft Graph throttling at ~10,000 requests/10 min per app); 90-day minimum data retention; incremental-only loads (no full refreshes per constitution)  
**Scale/Scope**: Organization-wide (all Microsoft 365 users with Copilot licenses); 8 ingestion/transformation notebooks; 4 Power BI report pages; 1 scheduled pipeline

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### I. Security & Compliance

| Gate | Status | Notes |
|------|--------|-------|
| Service Principal auth (no interactive credentials) | PASS | Azure AD Service Principal via `azure-identity` `ClientSecretCredential`; credentials from Key Vault |
| PII handling (GDPR compliance) | PASS ⚠️ | PII stored in clear text across all layers; access controlled via Fabric workspace RBAC and Power BI RLS (documented exception — see Complexity Tracking) |
| API call and data access logging | PASS | All API calls logged to `logs_execution_errors` Delta table with timestamp, caller, target, outcome |
| No hardcoded secrets | PASS | Credentials stored in Azure Key Vault, referenced via Fabric environment variables |

### II. Data Quality

| Gate | Status | Notes |
|------|--------|-------|
| Schema validation and null checks before Bronze write | PASS | Each ingestion notebook validates schema before write; invalid records quarantined |
| Delta Lake for all persistent storage | PASS | All layers use Delta Lake format exclusively |
| Incremental loading (no full refreshes) | PASS | Watermark-based incremental ingestion; FR-013 mandates incremental updates |
| Rate-limit handling with exponential backoff | PASS | Exponential backoff with jitter for 429 errors; capped retry budgets |

### III. Lakehouse Architecture

| Gate | Status | Notes |
|------|--------|-------|
| Bronze-Silver-Gold medallion layers | PASS | Three-layer architecture as specified |
| Modular notebooks (one layer transition per notebook) | PASS | Separate notebooks: 01-04 (source→Bronze), 05 (Bronze→Silver), 06 (Silver→Gold) |
| Orchestration via Fabric Pipelines only | PASS | Fabric Data Pipeline for daily scheduling; no external schedulers |
| Star schema Semantic Model | PASS | Fact table (fact_user_usage) + dimensions (dim_user, dim_date, dim_department, dim_license) |
| No bi-directional cross-filtering | PASS | Star schema with uni-directional relationships by default |

### IV. Testing & Monitoring

| Gate | Status | Notes |
|------|--------|-------|
| Inline data quality assertions in notebooks | PASS | Row counts, null thresholds, freshness, referential integrity in each notebook |
| Structured execution metrics logging | PASS | Duration, records read/written/rejected, errors logged per execution |
| Retry logic with circuit-breaker | PASS | Exponential backoff with max retries; pipeline continues on non-critical failures |
| Data lineage documentation | PASS | Source-to-target mapping documented across Bronze→Silver→Gold |

### V. Deployment & CI/CD

| Gate | Status | Notes |
|------|--------|-------|
| Git version control for all Fabric items | PASS | All notebooks, pipelines, semantic models in Git (GitHub) |
| Externalized config in parameter.yml | PASS | DEV/PROD separation via `parameter.yml` with `find_replace`, `key_value_replace`, and `semantic_model_binding` (see R-012) |
| Automated deployments via GitHub Actions | PASS | GitHub Actions using `fabric-cicd` (`publish_all_items`) for deployment + `fab` CLI for post-deploy pipeline execution (see R-011, R-012) |

**Pre-research gate evaluation**: ALL GATES PASS. No violations detected. Proceeding to Phase 0.

**Post-design re-evaluation (Phase 1)**: ALL GATES PASS. Design artifacts (data-model.md, contracts/, quickstart.md) reviewed against all 5 constitution principles. No new violations introduced. Key confirmations:
- Single app registration for Graph API with `AuditLogsQuery.Read.All` for audit logs (Security)
- `logs_execution_errors` and `logs_data_quality` Delta tables for audit logging (Security + Monitoring)
- Copilot audit events sourced via Graph Security Audit Log API (`/security/auditLog/queries`) — single app registration (Architecture)
- Usage reports use beta API — documented as known risk with migration path (Data Quality)
- Star schema with no bi-directional cross-filtering (Architecture)

## Project Structure

### Documentation (this feature)

```text
specs/001-copilot-license-optimization/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
│   └── api-contracts.md # Microsoft Graph API contract definitions
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)

```text
notebooks/
├── 01_ingest_graph_users.py          # Bronze: User profiles from Graph API
├── 02_ingest_graph_licenses.py       # Bronze: License assignments from Graph API
├── 03_ingest_usage_reports.py        # Bronze: Copilot usage metrics from M365 Reports API
├── 03b_ingest_m365_activity.py       # Bronze: General M365 productivity activity per user
├── 04_ingest_audit_logs.py           # Bronze: Copilot audit events from Graph Security Audit Log API
├── 05_transform_bronze_to_silver.py  # Silver: Clean and conform raw data
├── 06_transform_silver_to_gold.py    # Gold: Calculate usage scores and recommendations
└── 99_data_quality_checks.py         # Validation: Data completeness and freshness

config/
├── parameter.yml                     # Environment-specific configuration (DEV/PROD)
└── key_vault_config.yml              # Key Vault reference configuration

pipelines/
└── daily_refresh_pipeline.json       # Fabric Data Pipeline definition

semantic-model/
├── model.bim                         # Power BI Semantic Model definition
└── measures.dax                      # DAX measures for KPIs

reports/
└── copilot_license_analytics.pbir    # Power BI Report definition

.github/
└── workflows/
    └── deploy.yml                    # GitHub Actions CI/CD workflow
```

**Structure Decision**: Flat notebook structure under `notebooks/` with numbered prefixes indicating execution order. Configuration externalized under `config/`. Pipeline definitions, semantic model, and reports in separate top-level directories. This follows the Fabric-native project layout pattern.

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| PII in clear text across all layers (Principle I) | IT administrators must identify specific users by name/email for license reallocation — the core use case | Pseudonymization in Silver rejected: breaks join integrity, requires complex re-identification, adds latency with no security benefit given workspace RBAC already restricts access to IT admins |
| gold_user_license_usage full Overwrite (Principle II) | Rolling 30-day composite usage score requires full recalculation from Silver; all users' relative activity affects classification thresholds | Incremental Gold update rejected: usage_score categories shift when any user's activity changes; partial recalculation produces inconsistent classifications |
