# Implementation Plan: M365 Copilot License Usage Analytics

**Branch**: `001-copilot-license-usage` | **Date**: 2026-02-25 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/001-copilot-license-usage/spec.md`

## Summary

Build a Microsoft Fabric analytics data pipeline that ingests Microsoft 365 Copilot license assignments, per-user usage metrics, and audit logs from Microsoft Graph API into a Lakehouse using the Bronze–Silver–Gold medallion pattern. The pipeline calculates a recency-weighted usage score (0–100) per licensed user to identify underutilised Copilot licenses. All infrastructure is provisioned via Fabric CLI (`fab`), deployed via `fabric-cicd` through GitHub Actions, and orchestrated by Fabric Data Pipelines on a daily 6 AM schedule. No Fabric UI interaction is required.

## Technical Context

**Language/Version**: Python 3.11 (PySpark kernel, Fabric Runtime 1.3)
**Primary Dependencies**: `msgraph-sdk` 1.55.0 (Graph v1.0), `msgraph-beta-sdk` 1.56.0 (Graph beta), `azure-identity` (auth), `pyspark` (built-in), `delta-spark` (built-in), `fabric-cicd` 0.2.0 (deployment), `ms-fabric-cli` 1.4.0 (provisioning)
**Storage**: Microsoft Fabric Lakehouse — Delta Lake format exclusively (Constitution P-IV)
**Testing**: Data quality checks via `dq_results` Delta table; pipeline smoke-tests via `fab run`; linting via `ruff`
**Target Platform**: Microsoft Fabric (Spark notebooks + Data Pipelines)
**Project Type**: Data pipeline / analytics platform
**Performance Goals**: End-to-end pipeline completes within 60 minutes for 50K users (SC-006)
**Constraints**: All Graph API endpoints used are in `/beta` (usage reports, audit log queries); data latency 24-48h per Microsoft SLA; audit log query API uses async pattern (create → poll → retrieve records)
**Scale/Scope**: Up to 50,000 users; 8 Copilot-enabled apps tracked; indefinite data retention

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

**NON-NEGOTIABLE gates — ALL must be ✅ before implementation begins:**

- [x] **P-I  (Security)**      Service Principal authentication planned for all Graph API calls via `azure-identity` `ClientSecretCredential` + official Microsoft Graph SDKs (`msgraph-sdk`, `msgraph-beta-sdk`). No interactive user credentials in any notebook or pipeline. SP permissions scoped to `User.Read.All`, `Reports.Read.All`, `AuditLogsQuery.Read.All` (least-privilege).
- [x] **P-II (Secrets)**       Client ID, client secret, and tenant ID stored in Azure Key Vault. Retrieved at runtime via `notebookutils.credentials.getSecret()`. `parameter.yml` contains Key Vault URL and secret names only — zero secret values.
- [x] **P-III (Data Quality)** Schema validation + null checks on all required fields in every ingestion notebook (01–03). `_rejected` partition defined in data model for quarantined records. Freshness check gates Silver-layer processing (FR-013).
- [x] **P-VII (Observability)**Structured logging (run_id, timestamps, record counts) in every notebook. `gold_copilot_audit_log` Delta table (Gold layer) captures all API calls and data access. `gold_copilot_dq_results` Delta table (Gold layer) persists quality metrics after every run.

**Architecture gates — verify at Phase 1 design review:**

- [x] Delta Lake format confirmed for all durable storage (Principle IV). No CSV/JSON/Parquet persisted beyond transient staging.
- [x] Bronze–Silver–Gold layer assignments documented in [data-model.md](data-model.md).
- [x] Modular notebook decomposition shown in project structure: `01_ingest_users`, `02_ingest_usage`, `03_ingest_audit_logs`, `04_transform_silver`, `05_transform_gold`, `06_compute_scores`, `99_dq_checks` (Principle V). User profiles and license details are combined in `01_ingest_users` (single Graph API call via `$expand=licenseDetails`).
- [x] Exponential backoff (base 1s, max 60s, 5 retries, full jitter) specified for all Graph API notebooks. Rate-limit thresholds declared in `parameter.yml` (Principle VI).
- [x] `parameter.yml` per environment (`dev/`, `prod/`) defined with `find_replace` for Lakehouse GUIDs and `key_value_replace` for pipeline notebook references. No hardcoded environment strings (Principle VIII).
- [x] Fabric Data Pipeline orchestrates all scheduling (daily 6 AM). No Python schedulers (Principle VIII, Data Standards).
- [x] GitHub Actions CI/CD pipeline planned: `ruff` lint → `gitleaks` secret-scan → `fab auth login` → `fabric-cicd publish_all_items()` → `fab run` smoke-test. No manual production deployments (Principle VIII).

## Project Structure

### Documentation (this feature)

```text
specs/001-copilot-license-usage/
├── spec.md              # Feature specification (completed)
├── plan.md              # This file
├── research.md          # Phase 0: Technology research & decisions
├── data-model.md        # Phase 1: Delta table schemas (Bronze/Silver/Gold)
├── quickstart.md        # Phase 1: CLI-first setup guide
├── contracts/           # Phase 1: Graph API endpoint contracts
│   ├── graph-users.md
│   ├── graph-usage-reports.md
│   └── graph-audit-logs.md
└── tasks.md             # Phase 2 output (/speckit.tasks — NOT created by plan)
```

### Source Code (repository root)

```text
workspace/
├── 00_watermarks.Notebook/
│   └── notebook-content.py       # Watermark management (read/write delta tokens)
├── 01_ingest_users.Notebook/
│   └── notebook-content.py       # Ingest user profiles + license details from Graph
├── 02_ingest_usage.Notebook/
│   └── notebook-content.py       # Ingest Copilot usage reports from Graph beta
├── 03_ingest_audit_logs.Notebook/
│   └── notebook-content.py       # Ingest audit log events from Graph beta
├── 04_transform_silver.Notebook/
│   └── notebook-content.py       # Validate, cleanse, MERGE into Silver tables
├── 05_transform_gold.Notebook/
│   └── notebook-content.py       # Aggregate Gold dimension/fact tables
├── 06_compute_scores.Notebook/
│   └── notebook-content.py       # Calculate usage scores (recency/freq/breadth)
├── 99_dq_checks.Notebook/
│   └── notebook-content.py       # Data quality checks → dq_results table
├── helpers.Notebook/
│   └── notebook-content.py       # Shared utilities (auth, retry, logging, schema)
├── CopilotUsagePipeline.DataPipeline/
│   └── pipeline-content.json     # Daily orchestration pipeline definition
├── CopilotUsageLakehouse.Lakehouse/
│   └── .platform                 # Lakehouse metadata
├── CopilotUsageEnv.Environment/
│   ├── Setting.yml               # Python library dependencies
│   └── Sparkcompute.yml          # Spark pool configuration
├── parameter.yml                 # Environment parameterisation (dev/prod)
└── .github/
    └── workflows/
        └── deploy.yml            # CI/CD: lint → secret-scan → deploy → smoke-test
```

**Structure Decision**: Single-workspace layout following `fabric-cicd` conventions: `<ItemName>.<ItemType>/` per Fabric item. All notebooks follow Constitution Principle V (single responsibility). The `helpers.Notebook` contains shared utilities referenced via `%run` or `notebookutils.notebook.run()`. Pipeline JSON references notebooks by name — parameterised via `key_value_replace` in `parameter.yml` for cross-environment deployment.

## Complexity Tracking

No Constitution violations identified. All NON-NEGOTIABLE and architecture gates pass.

| Decision | Rationale |
|----------|-----------|
| Graph API beta endpoints | No GA alternative exists for Copilot usage reports. Schema changes monitored via response validation in `helpers.Notebook`. |
| Single Lakehouse (not per-layer) | Constitution allows single Lakehouse with table-level layer prefixes (`bronze_*`, `silver_*`, `gold_*`). Reduces workspace item count and simplifies `parameter.yml`. |
| Combined user+license ingestion | Graph `/users?$expand=licenseDetails` returns both in one call. Splitting into two notebooks would double API calls without benefit. FR-001 and FR-002 served by `01_ingest_users.Notebook`. |
