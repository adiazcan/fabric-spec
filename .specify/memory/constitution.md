<!--
================================================================================
SYNC IMPACT REPORT
================================================================================
Version Change  : [CONSTITUTION_VERSION] → 1.0.0
                  (Initial ratification — all placeholders resolved)

Modified Principles : None (initial population; no prior named principles existed)

Added Sections:
  - Core Principles I–VIII (all new):
      I.   Security & Zero-Trust Identity        (NON-NEGOTIABLE)
      II.  Secrets & Credential Hygiene          (NON-NEGOTIABLE)
      III. Data Quality at Ingestion             (NON-NEGOTIABLE)
      IV.  Lakehouse-First Architecture
      V.   Modular Notebook Design
      VI.  Resilient API Integration
      VII. Observability & Monitoring            (NON-NEGOTIABLE)
      VIII.Versioned & Automated Deployment
  - Data Standards & Platform Contracts          (new)
  - Development Workflow & Quality Gates         (new)
  - Governance                                   (expanded from template stub)

Removed Sections : None

Templates Updated:
  ✅ .specify/templates/plan-template.md   — Constitution Check gates populated
  ✅ .specify/templates/spec-template.md   — Security & Compliance FR hints added
  ✅ .specify/templates/tasks-template.md  — Foundational phase examples updated
  ⚠  .specify/templates/checklist-template.md — Manual review recommended;
       add data-quality and deployment categories for Fabric feature checklists
  ⚠  .specify/templates/agent-file-template.md — Update [ACTIVE TECHNOLOGIES]
       when first plan.md is generated (auto-populated by /speckit.plan)

Deferred TODOs:
  - RATIFICATION_DATE set to 2026-02-25 (today). Update if a formal governance
    board approval date differs from the initial commit date.
================================================================================
-->

# Fabric Analytics Platform Constitution

## Core Principles

### I. Security & Zero-Trust Identity (NON-NEGOTIABLE)

All data access MUST use Service Principal (SP) authentication with least-privilege
role assignments scoped to the minimum required Fabric workspace, Lakehouse, or
dataset. Interactive user credentials MUST NOT be used in automated pipelines or
notebooks. Service Principal permissions MUST be reviewed quarterly, and excess
permissions MUST be revoked within five business days of detection. Any deviation
requires a documented exception approved by the security lead.

**Rationale**: Service Principals provide auditable, revocable, non-human identities
that satisfy enterprise Zero-Trust requirements and prevent credential sprawl across
pipeline runs.

### II. Secrets & Credential Hygiene (NON-NEGOTIABLE)

Secrets, connection strings, client IDs, client secrets, and tenant IDs MUST NEVER
be hardcoded in notebooks, pipeline definitions, parameter files, or source code.
All secrets MUST be retrieved at runtime from Azure Key Vault via Fabric's linked
Key Vault secret store or secure environment variable injection. `parameter.yml`
files MUST contain only non-sensitive configuration references (e.g., Key Vault
secret names, workspace IDs, API endpoint URLs) — never secret values themselves.
Secret-scanning MUST be enforced as a blocking GitHub Actions step on every PR.

**Rationale**: Hardcoded credentials in version-controlled files are the leading
cause of enterprise data breaches. Key Vault centralises rotation and audit of all
credentials without requiring code changes.

### III. Data Quality at Ingestion (NON-NEGOTIABLE)

Every ingestion notebook MUST enforce schema validation (column names, data types)
and null checks on all required fields before writing to the Bronze layer. Records
failing validation MUST be quarantined to a `_rejected` partition with structured
error metadata (rule name, timestamp, record count, sample payload). Ingestion MUST
NOT silently discard or overwrite corrupt data. A data freshness check (configurable
maximum source-data age) MUST gate downstream Silver-layer processing; stale sources
MUST raise an alertable warning before proceeding.

**Rationale**: Defects caught at ingestion prevent cascading quality failures across
Bronze → Silver → Gold. Quarantine partitions enable root-cause analysis without
blocking the pipeline or corrupting downstream aggregates.

### IV. Lakehouse-First Architecture

All durable data storage MUST use Delta Lake format within a Fabric Lakehouse. The
Bronze–Silver–Gold medallion pattern is MANDATORY across all data domains:

- **Bronze**: Raw, append-only ingested data. No transformations applied. Schema
  is inferred at read time. One partition per source system / ingestion date.
- **Silver**: Validated, cleansed, and incrementally merged data. Schema enforced.
  Delta `MERGE INTO` or watermark-based append MUST be used. No full overwrites.
- **Gold**: Business-aggregated, star-schema-aligned tables for reporting and
  Semantic Model consumption. Optimised for analytical query patterns.

Full table refreshes are PROHIBITED in production unless explicitly justified in
writing, approved by the data engineering lead, and documented in the feature spec.
Incremental loading is the default strategy for every layer.

**Rationale**: Delta Lake ACID semantics prevent partial-write corruption; medallion
layers create clear audit points, independent reprocessing zones, and observable
data lineage from source to report.

### V. Modular Notebook Design

Notebooks MUST be decomposed by single logical concern — one notebook per stage
(e.g., `ingest_<source>.py`, `transform_<domain>.py`, `report_<subject>.py`).
A single notebook MUST NOT combine ingestion, transformation, and reporting logic.
Notebooks MUST accept all environment-specific values (workspace paths, table
names, API endpoints) via Fabric Pipeline parameters or `notebookutils` parameter
cells — no hardcoded paths or names in notebook body code. Shared utility functions
MUST reside in a dedicated utilities notebook and be referenced via `%run` or
`notebookutils.notebook.run`.

**Rationale**: Modular notebooks are independently testable, reusable across
pipelines, and limit the blast radius of failures to a single pipeline stage.

### VI. Resilient API Integration

All external API calls MUST implement exponential backoff with jitter for transient
failures (HTTP 429, 503, and 5xx responses). Retry attempts MUST be bounded:
maximum 5 retries, base delay 1 s, maximum delay 60 s, with full-jitter
randomisation. Per-source rate-limit thresholds MUST be declared in `parameter.yml`
and enforced before making requests. Unrecoverable failures (4xx except 429) MUST
fail fast with structured error logging and MUST NOT trigger retry cycles. All API
responses MUST be validated against expected schema prior to persistence.

**Rationale**: Uncontrolled retries amplify upstream outages; bounded exponential
backoff with jitter distributes retry load and respects upstream SLAs without
manual intervention.

### VII. Observability & Monitoring (NON-NEGOTIABLE)

Every notebook and Fabric Pipeline MUST emit structured logs containing: run ID,
execution start/end timestamps, records ingested, records processed, records
rejected, and error details (type, message, stack trace). Audit logs for all data
access operations and external API calls MUST be written to a dedicated
`audit_log` Delta table in the Gold layer for compliance tracking and retention.
Data quality check results (row counts, null percentages, freshness deltas) MUST
be persisted to a `dq_results` Delta table after every pipeline run. Data lineage
(source → Bronze → Silver → Gold table mappings, including transformation logic
references) MUST be documented and updated with each schema change.

**Rationale**: Structured, persisted observability enables SLA reporting, compliance
audits, and rapid incident triage — without relying on ephemeral notebook output
cells that disappear after a run.

### VIII. Versioned & Automated Deployment

All Fabric items (Notebooks, Pipelines, Semantic Models, Lakehouses, Dataflows)
MUST be version-controlled in Git and deployable via Fabric CLI (`fab`) or the
Fabric CI/CD REST API. Manual production deployments are PROHIBITED. Environment-
specific configuration (workspace IDs, Lakehouse paths, API endpoints, Key Vault
URIs) MUST be stored in `parameter.yml` files per environment (`dev/`, `prod/`).
Deployments to the PROD workspace MUST be gated by a successful DEV pipeline run
and an approved GitHub Pull Request. GitHub Actions workflows MUST automate the
full deploy cycle: lint → secret-scan → validate → deploy → smoke-test.

**Rationale**: Manual Fabric deployments are error-prone and unauditable. CI/CD
gates enforce quality and provide a complete, reproducible deployment audit trail
aligned with enterprise change-management requirements.

## Data Standards & Platform Contracts

- **Delta Lake ONLY**: Parquet, CSV, and JSON formats MUST only exist as transient
  staging files during ingestion. All durable, queryable storage MUST be Delta Lake.

- **Star Schema for Gold & Semantic Models**: Gold-layer tables exposed via a
  Semantic Model MUST follow star schema design: one fact table per business
  process, conformed dimension tables with surrogate keys, and explicit
  relationships defined and documented in the Semantic Model.

- **Naming Conventions**: Tables MUST follow the `<layer>_<domain>_<entity>`
  pattern (e.g., `bronze_sales_orders`, `silver_crm_contacts`,
  `gold_finance_revenue`). Notebooks MUST follow `<stage>_<source_or_domain>.py`.

- **Orchestration via Fabric Pipelines**: Python-based scheduling (e.g.,
  APScheduler, cron, Airflow external to Fabric) is PROHIBITED for production
  orchestration. All scheduling, dependency management, and retry orchestration
  MUST use Fabric Data Pipelines.

- **Parameter Management**: Every environment-specific value MUST resolve through
  `parameter.yml`. Hardcoded environment names (e.g., string literals `"dev"`,
  `"prod"`) embedded in notebook logic are PROHIBITED.

- **Incremental Loading Default**: Delta `MERGE INTO` with a watermark column, or
  high-watermark append, is the default ingestion pattern for all layers. Any
  full-refresh implementation MUST be labelled `FULL_REFRESH=true` and logged.

## Development Workflow & Quality Gates

**Feature Development Flow**:

1. Branch from `main`; name branches `feature/<ticket-id>-short-description`.
2. Develop and validate exclusively in the DEV workspace using `dev/parameter.yml`.
3. All notebooks MUST pass linting (`pylint` / `ruff`) before raising a PR.
4. PRs that introduce schema changes MUST include updated data lineage docs.
5. Peer review MUST verify compliance against Principles I–VIII before approval.
6. Merge triggers GitHub Actions CI pipeline; no manual deployments bypass CI.

**Quality Gates (MUST pass before PROD deployment)**:

- [ ] Schema validation tests pass for all affected Bronze, Silver, and Gold layers.
- [ ] Data quality checks (row counts, null %, freshness) emit green status in
      the DEV pipeline run and results are persisted to `dq_results`.
- [ ] Secret-scanning step in GitHub Actions returns zero findings.
- [ ] Audit log entries are generated for all data access and API call operations.
- [ ] Exponential backoff retry logic is verified for all API-connected notebooks.
- [ ] `parameter.yml` contains zero secret values (Key Vault secret names only).
- [ ] GitHub Actions CI pipeline passes end-to-end on the feature branch.
- [ ] Data lineage documentation is current with the deployed schema state.

**Code Review Checklist**:

- Does each notebook follow single-responsibility (one concern per notebook)?
- Are all secrets retrieved from Azure Key Vault — none hardcoded?
- Is Delta `MERGE INTO` / watermark strategy used instead of full table overwrites?
- Are quality check results written to `dq_results` after the run?
- Is the data lineage document updated to reflect any schema changes?
- Are retry bounds and rate-limit thresholds declared in `parameter.yml`?
- Is the deployment automated via GitHub Actions — no manual `fab` commands
  executed outside of CI?

## Governance

This constitution supersedes all other engineering practices and style guides for
this project. In the event of conflict, this document takes precedence.

**Amendment Procedure**:

1. Open a GitHub Issue labelled `constitution-amendment` describing the proposed
   change, its motivation, and the affected principles or sections.
2. Classify the version bump — PATCH, MINOR, or MAJOR — per the policy below and
   justify the classification in the issue.
3. Obtain written approval from at least two domain leads (data engineering and
   security/compliance).
4. Update `.specify/memory/constitution.md` and propagate changes to all dependent
   templates using the `/speckit.constitution` command.
5. Merge via standard PR with the commit message:
   `docs: amend constitution to vX.Y.Z (<one-line summary>)`

**Versioning Policy**:

- **MAJOR**: Removal or redefinition of a NON-NEGOTIABLE principle; breaking change
  to platform contracts, the medallion layer model, or the deployment gate model.
- **MINOR**: New principle added; new mandatory section introduced; material
  expansion of an existing principle's scope or enforcement requirements.
- **PATCH**: Clarifications, wording improvements, typo fixes, example updates,
  or non-semantic refinements that do not alter enforcement intent.

**Compliance Review**: All active feature branches MUST be reviewed against this
constitution at PR time via the Code Review Checklist above. A quarterly governance
review MUST audit Fabric workspace Service Principal permissions, Azure Key Vault
access policies, secret-scanning pipeline health, and deployment audit logs. Audit
findings MUST be resolved within 30 days or escalated to the project owner.

**Version**: 1.0.0 | **Ratified**: 2026-02-25 | **Last Amended**: 2026-02-25
