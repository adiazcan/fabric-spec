<!--
  Sync Impact Report
  ===================
  Version change: N/A → 1.0.0 (initial ratification)
  Modified principles: N/A (initial version)
  Added sections:
    - Core Principles (5): Security & Compliance, Data Quality,
      Lakehouse Architecture, Testing & Monitoring, Deployment & CI/CD
    - Technology Stack & Constraints
    - Development Workflow & Quality Gates
    - Governance
  Removed sections: N/A
  Templates requiring updates:
    - .specify/templates/plan-template.md ✅ compatible (no changes needed)
    - .specify/templates/spec-template.md ✅ compatible (no changes needed)
    - .specify/templates/tasks-template.md ✅ compatible (no changes needed)
    - .specify/templates/checklist-template.md ✅ compatible (no changes needed)
  Follow-up TODOs: None
-->

# Fabric Analytics Platform Constitution

## Core Principles

### I. Security & Compliance (NON-NEGOTIABLE)

- All data access MUST use Service Principal authentication with
  least-privilege permissions. Interactive user credentials MUST NOT
  be used in automated pipelines or notebooks.
- PII data (user emails, names, identifiers) MUST be handled in
  compliance with GDPR and organizational data retention policies.
  PII MUST be masked or pseudonymized in Bronze/Silver layers and
  only surfaced in Gold when explicitly authorized.
- Every API call and data access operation MUST be logged with
  timestamp, caller identity, target resource, and outcome for
  compliance auditing.
- Credentials and secrets MUST NOT be hardcoded in notebooks,
  pipelines, or configuration files. All secrets MUST be retrieved
  at runtime from Azure Key Vault via Fabric-native integration or
  linked services.
- Rationale: Enterprise data platforms handle sensitive business and
  personal data; a single credential leak or unauthorized access can
  result in regulatory penalties and data breaches.

### II. Data Quality

- All ingested data MUST pass schema validation and null-value checks
  before being written to the Bronze layer. Invalid records MUST be
  quarantined to a dedicated error table with rejection metadata.
- Delta Lake format MUST be used for all persistent data storage to
  guarantee ACID transactions, time-travel, and schema evolution.
- Data loading MUST use incremental patterns (watermark columns,
  change data capture, or delta detection). Full refreshes are
  prohibited unless explicitly justified and documented per dataset.
- API data sources MUST implement graceful rate-limit handling with
  exponential backoff and jitter. Retry budgets MUST be capped to
  prevent runaway execution costs.
- Rationale: Data is the foundation of all downstream analytics and
  AI; quality failures propagate silently and erode trust in
  reporting and decision-making.

### III. Lakehouse Architecture

- The platform MUST follow a Lakehouse-first architecture organized
  into three medallion layers:
  - **Bronze**: Raw data ingested as-is from sources, append-only.
  - **Silver**: Cleansed, conformed, and deduplicated data with
    enforced schemas.
  - **Gold**: Business-level aggregates and curated datasets
    optimized for consumption.
- Notebooks MUST be modular: separate notebooks for ingestion,
  transformation, and reporting concerns. A single notebook MUST NOT
  span more than one medallion layer transition.
- Orchestration MUST use Fabric Pipelines (Data Factory). Complex
  scheduling logic MUST NOT be implemented in Python code within
  notebooks.
- Semantic Models MUST follow star schema design with explicit
  dimension and fact tables and properly defined relationships.
  Bi-directional cross-filtering MUST be avoided unless justified.
- Rationale: The medallion architecture provides clear data lineage,
  separation of concerns, and enables independent evolution of each
  layer without downstream breakage.

### IV. Testing & Monitoring

- Every notebook MUST include inline data quality assertions: row
  count validation, null-percentage thresholds, data freshness
  checks, and referential integrity verification.
- Each pipeline and notebook execution MUST log structured metrics:
  execution duration, records read/written, records rejected, and
  error details.
- All external API integrations MUST implement retry logic with
  configurable max-retry counts and circuit-breaker patterns for
  sustained failures.
- Data lineage documentation MUST be generated and maintained,
  mapping source-to-target for every dataset across Bronze, Silver,
  and Gold layers.
- Rationale: Without proactive monitoring and quality gates, data
  issues are discovered only when business users encounter incorrect
  reports—by which time the damage is already done.

### V. Deployment & CI/CD

- All Fabric items (notebooks, pipelines, semantic models, lakehouses)
  MUST be version-controlled in Git and deployable via Fabric CLI or
  Fabric CI/CD deployment pipelines.
- Environment-specific configuration (workspace IDs, API endpoints,
  Key Vault URIs) MUST be externalized in `parameter.yml` files with
  clear DEV/PROD separation. Configuration values MUST NOT be
  inlined in code.
- Deployments to production MUST be automated via GitHub Actions
  workflows with mandatory approval gates. Manual deployments to
  production are prohibited.
- Rationale: Reproducible, auditable deployments eliminate
  configuration drift between environments and reduce the risk of
  human error in production releases.

## Technology Stack & Constraints

- **Platform**: Microsoft Fabric (Lakehouse, Data Engineering,
  Data Factory, Power BI)
- **Compute**: Fabric Spark notebooks (PySpark / Spark SQL)
- **Storage**: OneLake with Delta Lake format exclusively
- **Orchestration**: Fabric Pipelines (Data Factory)
- **Secrets Management**: Azure Key Vault
- **Authentication**: Azure AD Service Principals with RBAC
- **Semantic Layer**: Power BI Semantic Models (star schema)
- **Version Control**: Git (GitHub)
- **CI/CD**: GitHub Actions + Fabric CLI / Fabric CI/CD
- **Configuration**: `parameter.yml` per environment
- **Language**: Python 3.x (PySpark), Spark SQL, DAX
- No external schedulers (Airflow, cron) MUST be introduced;
  Fabric Pipelines are the single orchestration mechanism.

## Development Workflow & Quality Gates

1. **Branch Strategy**: All changes MUST be developed on feature
   branches and merged to main via pull requests with at least one
   reviewer approval.
2. **Pre-Merge Gates**:
   - Notebook linting and static analysis pass.
   - Data quality assertions execute successfully against DEV data.
   - No hardcoded credentials detected (automated secret scanning).
   - `parameter.yml` contains valid entries for all target
     environments.
3. **Deployment Pipeline**:
   - Merge to main triggers GitHub Actions workflow.
   - Workflow deploys to DEV workspace → runs smoke tests →
     promotes to PROD workspace with manual approval gate.
4. **Post-Deployment Validation**:
   - Row count and freshness checks run automatically after each
     pipeline execution in production.
   - Execution metrics are published to a monitoring dashboard.
5. **Documentation**: Every new dataset, pipeline, or notebook MUST
   include a README or inline markdown documentation describing
   purpose, inputs, outputs, and schedule.

## Governance

- This constitution supersedes all other development practices for
  the Fabric Analytics Platform. In case of conflict, constitution
  principles take precedence.
- **Amendment Procedure**: Any proposed change to this constitution
  MUST be submitted as a pull request with:
  1. A clear description of the change and its rationale.
  2. Impact analysis on existing pipelines and artifacts.
  3. Approval from at least two team leads.
- **Versioning Policy**: The constitution follows semantic versioning
  (MAJOR.MINOR.PATCH):
  - MAJOR: Principle removal or backward-incompatible redefinition.
  - MINOR: New principle or materially expanded guidance.
  - PATCH: Clarifications, wording fixes, non-semantic refinements.
- **Compliance Review**: All pull requests and code reviews MUST
  verify adherence to these principles. Reviewers MUST use the
  Constitution Check section in plan documents as a validation gate.
- **Guidance File**: Use `.specify/memory/constitution.md` as the
  authoritative reference for runtime development guidance.

**Version**: 1.0.0 | **Ratified**: 2026-02-23 | **Last Amended**: 2026-02-23
