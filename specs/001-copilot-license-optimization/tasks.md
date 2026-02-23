# Tasks: Microsoft 365 Copilot License Usage Analytics

**Input**: Design documents from `/specs/001-copilot-license-optimization/`
**Prerequisites**: plan.md (required), spec.md (required), research.md, data-model.md, contracts/api-contracts.md, quickstart.md

**Tests**: Not requested — no test tasks included. Data quality validation is handled by `99_data_quality_checks.py` as part of the pipeline (US7).

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US7)
- Include exact file paths in descriptions

## Path Conventions

Based on plan.md project structure:

```
notebooks/          # PySpark notebooks (01-06, 99)
config/             # Runtime configuration (parameter.yml, key_vault_config.yml)
pipelines/          # Fabric Data Pipeline definitions
semantic-model/     # Power BI Semantic Model (model.bim, measures.dax)
reports/            # Power BI Report definitions
.github/workflows/  # CI/CD workflows
.deploy/            # Deployment scripts
```

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization, directory structure, and environment configuration

- [X] T001 Create project directory structure with notebooks/, config/, pipelines/, semantic-model/, reports/, .deploy/, and .github/workflows/ directories
- [X] T002 [P] Create runtime configuration with Key Vault references, Copilot SKU IDs, usage score weights, alert thresholds, and license cost in config/parameter.yml
- [X] T003 [P] Create Key Vault secret name mappings for graph-tenant-id, graph-client-id, and graph-client-secret in config/key_vault_config.yml

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: CI/CD infrastructure and deployment configuration that MUST be complete before user story implementation

**⚠️ CRITICAL**: These enable automated deployments and environment-specific parameterization for all Fabric items

- [X] T004 Create fabric-cicd deployment parameterization with find_replace for lakehouse/workspace IDs, key_value_replace for pipeline schedules, and semantic_model_binding in the fabric-cicd parameter.yml at the workspace repository root (distinct from config/parameter.yml runtime config)
- [X] T005 [P] Create deployment script using fabric-cicd publish_all_items and unpublish_all_orphan_items with Notebook, DataPipeline, Environment, SemanticModel, Report, and Lakehouse item types in .deploy/fabric_workspace.py
- [X] T006 [P] Create GitHub Actions CI/CD workflow with checkout, Python setup, fabric-cicd install, Azure login, and deployment script execution in .github/workflows/deploy.yml

**Checkpoint**: Foundation ready — user story implementation can now begin

---

## Phase 3: User Story 7 — Daily Data Refresh with Incremental Updates (Priority: P1) 🎯 MVP

**Goal**: Build the complete data pipeline that ingests data from Microsoft Graph API through Bronze-Silver-Gold medallion architecture using PySpark notebooks, orchestrated by a Fabric Data Pipeline on a daily schedule

**Independent Test**: Run the pipeline on two consecutive days; verify only new/changed records are processed on the second run; confirm Bronze tables contain raw data with `_ingested_at` timestamps, Silver tables are deduplicated and conformed, and Gold tables contain usage scores and recommendations

### Implementation for User Story 7

- [X] T007 [P] [US7] Implement user profile ingestion from Graph API `/v1.0/users` with delta queries, `@odata.nextLink` pagination, watermark tracking in control_pipeline_state, schema validation, and append-only write to bronze_users Delta table in notebooks/01_ingest_graph_users.py
- [X] T008 [P] [US7] Implement license assignment ingestion from Graph API `/v1.0/users?$select=id,userPrincipalName,assignedLicenses` with full snapshot retrieval, Copilot SKU identification (639dec6b, a809996b, ad9c22b3), and append-only write to bronze_licenses Delta table in notebooks/02_ingest_graph_licenses.py
- [X] T009 [P] [US7] Implement Copilot usage report ingestion from Graph API `/beta/reports/getMicrosoft365CopilotUsageUserDetail(period='D30')` with JSON format pagination, report_refresh_date watermark, and append-only write to bronze_usage_reports Delta table in notebooks/03_ingest_usage_reports.py
- [X] T010 [P] [US7] Implement audit log ingestion via Graph Security Audit Log API with POST query creation for CopilotInteraction records, async polling for query completion, paginated record retrieval, time-window tracking, and append-only write to bronze_audit_logs Delta table in notebooks/04_ingest_audit_logs.py
- [X] T031 [P] [US7] Implement M365 productivity activity ingestion from Graph API `/v1.0/reports/getOffice365ActiveUserDetail(period='D30')` with JSON format pagination, report_refresh_date watermark, and append-only write to bronze_m365_activity Delta table in notebooks/03b_ingest_m365_activity.py
- [X] T011 [US7] Implement Bronze-to-Silver transformation with MERGE upsert for silver_users (deduplicated by user_id), silver_licenses (upsert on user_id+sku_id with is_copilot_license flag), silver_copilot_usage (normalized dates, apps_used_count calculation), silver_copilot_interactions (parsed auditData for app_host, event deduplication by event_id), and silver_m365_activity (normalized M365 service activity dates, active_services_count) in notebooks/05_transform_bronze_to_silver.py
- [X] T012 [US7] Implement Silver-to-Gold transformation with composite usage score calculation (frequency 0.4 + breadth 0.3 + recency 0.3), usage category classification (Active ≥0.5, Low Usage 0.1-0.49, Inactive <0.1), license recommendation logic, m365_activity_score computation from silver_m365_activity for unlicensed candidate ranking (FR-007), daily aggregate metrics for gold_daily_metrics, and department summary aggregation for gold_department_summary in notebooks/06_transform_silver_to_gold.py
- [X] T013 [US7] Implement data quality validation checks for row counts, null percentage thresholds, data freshness (≤24h), referential integrity (silver_licenses.user_id → silver_users), usage score range validation (0.0-1.0), and write results to logs_data_quality Delta table in notebooks/99_data_quality_checks.py
- [X] T014 [US7] Create daily refresh pipeline definition with sequential notebook activities (01→02→03→03b→04→05→06→99), semantic model refresh activity, error handling with retry (max 3 attempts), and daily 6 AM schedule in pipelines/daily_refresh_pipeline.json
- [X] T032 [US7] Add pipeline completion/failure email notification activity: send email to configured recipients on pipeline completion with status (SUCCESS/PARTIAL/FAILED), records processed, active users count, utilization rate, error count, and dashboard link per contract 2.2 in pipelines/daily_refresh_pipeline.json

**Checkpoint**: Data pipeline is fully functional — Bronze/Silver/Gold tables are populated with fresh data daily. All reporting stories can now be implemented.

---

## Phase 4: User Story 1 — License Utilization Overview (Priority: P1)

**Goal**: Deliver an executive summary dashboard showing total Copilot licenses, active users, utilization rate, estimated monthly savings, and adoption trend line

**Independent Test**: Open the Executive Dashboard page; verify KPI cards display total licenses, active users, utilization rate percentage, and estimated savings; verify trend line shows adoption rate changes over at least 30 days of data

### Implementation for User Story 1

- [X] T015 [US1] Create Power BI Semantic Model star schema with fact_user_usage (sourced from gold_user_license_usage), dim_user, dim_date (generated calendar table), dim_department, dim_license tables, surrogate keys, and uni-directional one-to-many relationships (dimension→fact) in semantic-model/model.bim
- [X] T016 [P] [US1] Define core DAX measures: Utilization Rate (DIVIDE active/licensed), Est Monthly Savings (inactive × cost), Active Users (COUNTROWS filtered), Total Licenses, Avg Usage Score (AVERAGE), and License Cost Per Month parameter in semantic-model/measures.dax
- [X] T017 [US1] Create Executive Dashboard report page with KPI cards (total licenses, active users, utilization rate, estimated savings), utilization gauge visual, and 30-day adoption trend line chart with date axis in reports/copilot_license_analytics.pbir

**Checkpoint**: Executive Dashboard is live — IT administrators can see organization-wide Copilot license utilization at a glance

---

## Phase 5: User Story 2 — Identify Underutilized Licenses (Priority: P1)

**Goal**: Provide a filterable list of users with Copilot licenses who have had no activity in the last 30 days, with adjustable inactivity threshold and CSV export capability

**Independent Test**: Navigate to User Details page; verify table shows users with columns (name, department, location, license date, last activity, days inactive); adjust days inactive filter from 30 to 14; export filtered list to CSV and verify file opens in spreadsheet applications

### Implementation for User Story 2

- [X] T018 [P] [US2] Add DAX measures for underutilized license analysis: Inactive Users count, Days Since Last Activity (selected user), configurable inactivity threshold parameter, and filtered user count by threshold in semantic-model/measures.dax
- [X] T019 [US2] Create User Details report page with filterable table (UserPrincipalName, DisplayName, Department, Location, LicenseAssignedDate, LastActivityDate, DaysSinceLastActivity, UsageScore, Recommendation), conditional formatting on usage score, days inactive slicer, and CSV export button in reports/copilot_license_analytics.pbir

**Checkpoint**: IT administrators can identify and export underutilized license holders for reallocation action

---

## Phase 6: User Story 3 — Departmental Adoption Analysis (Priority: P2)

**Goal**: Show Copilot adoption metrics broken down by department and location with drill-down to individual user detail

**Independent Test**: Open Department Analysis page; verify matrix shows utilization rate, average usage score, and active user count per department; click a department to drill through to individual users; apply location filter and verify metrics update

### Implementation for User Story 3

- [X] T020 [P] [US3] Add department-level DAX measures: Department Utilization Rate, Department Avg Usage Score, Department Active Users, Department Interaction Count in semantic-model/measures.dax
- [X] T021 [US3] Create Department Analysis report page with matrix visual (department × metrics), location slicer, drillthrough action from department row to User Details page filtered by selected department in reports/copilot_license_analytics.pbir

**Checkpoint**: Department managers can identify teams needing additional Copilot training or enablement support

---

## Phase 7: User Story 4 — License Assignment Candidates (Priority: P2)

**Goal**: Provide a ranked list of unlicensed users with high Microsoft 365 activity as candidates for Copilot license assignment, with CSV export

**Independent Test**: Open Recommendations page; verify list shows unlicensed users ranked by M365 activity score; verify columns include name, department, location, activity score, recommended priority; export list to CSV

### Implementation for User Story 4

- [X] T022 [P] [US4] Add license candidate DAX measures: M365 Activity Score ranking, Candidate Count, Recommended Priority label, and filter for users without Copilot license in semantic-model/measures.dax
- [X] T023 [US4] Create Recommendations report page with ranked list visual showing unlicensed user candidates ordered by M365 activity score, recommendation priority column, and CSV export button in reports/copilot_license_analytics.pbir

**Checkpoint**: IT administrators have data-driven reassignment recommendations to maximize Copilot ROI

---

## Phase 8: User Story 5 — Usage Trend Analysis (Priority: P2)

**Goal**: Display time-series charts showing Copilot adoption trends over configurable date ranges with department comparison capability

**Independent Test**: View trend analysis visuals; verify time-series chart shows daily active users, total interactions, and utilization rate over time; select a date range and verify chart updates; select multiple departments and verify separate trend lines appear

### Implementation for User Story 5

- [X] T024 [P] [US5] Add trend analysis DAX measures: Daily Active Users trend, Total Interactions trend, Utilization Rate trend, period-over-period comparison, and date range filtering support in semantic-model/measures.dax
- [X] T025 [US5] Add time-series trend chart visuals to the Executive Dashboard page with date range slicer, multi-department selection filter, and separate trend lines per department in reports/copilot_license_analytics.pbir

**Checkpoint**: IT leaders can measure the impact of training programs and organizational initiatives over time

---

## Phase 9: User Story 6 — Automated Usage Alerts (Priority: P3)

**Goal**: Send automated email alerts when overall utilization drops below a configurable threshold or individual users cross an inactivity mark

**Independent Test**: Configure utilization threshold to 60%; simulate a data refresh where utilization falls below 60%; verify email alert is sent to configured recipients with current utilization rate and dashboard link

### Implementation for User Story 6

- [X] T026 [US6] Add alert evaluation logic to pipeline: after Gold transformation, query gold_daily_metrics for latest utilization_rate, compare against alerts.utilization_threshold from config/parameter.yml, and collect individual users crossing inactivity threshold from gold_user_license_usage in pipelines/daily_refresh_pipeline.json
- [X] T027 [US6] Add email notification activity to pipeline: on threshold breach, send email to alerts.alert_recipients with subject "[Copilot Analytics] Usage Alert", body containing utilization rate, inactive user count, affected user digest, and dashboard link in pipelines/daily_refresh_pipeline.json

**Checkpoint**: IT administrators receive proactive notifications without manually checking the dashboard

---

## Phase 10: Polish & Cross-Cutting Concerns

**Purpose**: Deployment structure, verification, and documentation

- [X] T028 Create fabric-cicd workspace directory structure with .platform files for each Fabric item (Notebooks, DataPipeline, Lakehouse, SemanticModel, Report)
- [X] T029 [P] Run quickstart.md verification checklist to validate Key Vault access, Graph API connectivity, data pipeline execution, and report rendering
- [X] T030 [P] Update README.md with project overview, prerequisites, setup steps, architecture diagram reference, and links to spec documentation
- [X] T033 [P] Add "No data available" empty-state handling to all report pages with guidance text on when to expect first data refresh in reports/copilot_license_analytics.pbir

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup — BLOCKS deployment capability
- **US7 (Phase 3)**: Depends on Setup (config files) — BLOCKS all reporting stories
- **US1 (Phase 4)**: Depends on US7 completion (Gold tables must have data)
- **US2 (Phase 5)**: Depends on US1 (semantic model must exist); adds page + measures
- **US3 (Phase 6)**: Depends on US1 (semantic model must exist); can run in parallel with US2
- **US4 (Phase 7)**: Depends on US1 (semantic model must exist); can run in parallel with US2, US3
- **US5 (Phase 8)**: Depends on US1 (semantic model must exist); can run in parallel with US2, US3, US4
- **US6 (Phase 9)**: Depends on US7 (pipeline must exist); independent of reporting stories
- **Polish (Phase 10)**: Depends on all desired stories being complete

### User Story Dependencies

- **US7 (P1)**: Can start after Setup (Phase 1) — No dependencies on other stories. **Must complete before any reporting story.**
- **US1 (P1)**: Can start after US7 — Creates the semantic model and first report page
- **US2 (P1)**: Can start after US1 — Adds measures and page to existing semantic model/report
- **US3 (P2)**: Can start after US1 — Independent of US2; can run in parallel with US2
- **US4 (P2)**: Can start after US1 — Independent of US2/US3; can run in parallel
- **US5 (P2)**: Can start after US1 — Independent of US2/US3/US4; can run in parallel
- **US6 (P3)**: Can start after US7 — Independent of all reporting stories; modifies pipeline only

### Within Each User Story

- Models/schemas before services/transformations
- Transformations before pipeline definition
- Semantic model before DAX measures
- DAX measures before report pages
- Core implementation before integration

### Parallel Opportunities

- **Phase 1**: T002 and T003 can run in parallel (different config files)
- **Phase 2**: T005 and T006 can run in parallel (different files)
- **Phase 3**: T007, T008, T009, T010, T031 can all run in parallel (independent ingestion notebooks targeting different Bronze tables)
- **Phase 4**: T016 can run in parallel with T015 (measures vs model definition)
- **Phases 5-8**: US2, US3, US4, US5 can all run in parallel after US1 completes (each adds independent measures + report pages)
- **Phase 9**: US6 can run in parallel with US2-US5 (modifies pipeline, not semantic model/reports)
- **Phase 10**: T029 and T030 can run in parallel

---

## Parallel Example: User Story 7 (Data Pipeline)

```text
# Launch all ingestion notebooks in parallel (independent Bronze tables):
T007: "Implement user profile ingestion in notebooks/01_ingest_graph_users.py"
T008: "Implement license assignment ingestion in notebooks/02_ingest_graph_licenses.py"
T009: "Implement Copilot usage report ingestion in notebooks/03_ingest_usage_reports.py"
T010: "Implement audit log ingestion in notebooks/04_ingest_audit_logs.py"
T031: "Implement M365 activity ingestion in notebooks/03b_ingest_m365_activity.py"

# Then sequentially:
T011: "Implement Bronze-to-Silver transformation in notebooks/05_transform_bronze_to_silver.py"
T012: "Implement Silver-to-Gold transformation in notebooks/06_transform_silver_to_gold.py"
T013: "Implement data quality checks in notebooks/99_data_quality_checks.py"
T014: "Create pipeline definition in pipelines/daily_refresh_pipeline.json"
T032: "Add pipeline notification in pipelines/daily_refresh_pipeline.json"
```

## Parallel Example: Reporting Stories (After US1)

```text
# After US1 (semantic model + executive dashboard) is complete,
# launch US2, US3, US4, US5 in parallel:

Developer A — US2: T018 → T019 (underutilized licenses measures + page)
Developer B — US3: T020 → T021 (department analysis measures + page)
Developer C — US4: T022 → T023 (license candidates measures + page)
Developer D — US5: T024 → T025 (trend analysis measures + visuals)

# US6 can also run in parallel (modifies pipeline only):
Developer E — US6: T026 → T027 (alert logic + email notification)
```

---

## Implementation Strategy

### MVP First (US7 + US1 Only)

1. Complete Phase 1: Setup (T001-T003)
2. Complete Phase 2: Foundational (T004-T006)
3. Complete Phase 3: US7 — Data Pipeline (T007-T014)
4. **STOP and VALIDATE**: Run pipeline, verify Bronze/Silver/Gold tables populated
5. Complete Phase 4: US1 — Executive Dashboard (T015-T017)
6. **STOP and VALIDATE**: Open dashboard, verify KPIs display correctly
7. Deploy/demo if ready — this is the **Minimum Viable Product**

### Incremental Delivery

1. Setup + Foundational → Infrastructure ready
2. US7 (Data Pipeline) → Data flowing through Bronze-Silver-Gold → Validate pipeline
3. US1 (Executive Dashboard) → First visual output → **MVP Deploy/Demo!**
4. US2 (Underutilized Licenses) → Actionable user list → Deploy/Demo
5. US3 (Department Analysis) → Department insights → Deploy/Demo
6. US4 (License Candidates) → Reassignment recommendations → Deploy/Demo
7. US5 (Usage Trends) → Historical analysis → Deploy/Demo
8. US6 (Automated Alerts) → Proactive monitoring → Deploy/Demo
9. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. US7 (Data Pipeline): Full team collaborates (4 parallel ingestion notebooks + sequential transforms)
3. Once US7 and US1 are done:
   - Developer A: US2 (Underutilized Licenses)
   - Developer B: US3 (Department Analysis)
   - Developer C: US4 (License Candidates)
   - Developer D: US5 (Usage Trends)
   - Developer E: US6 (Automated Alerts)
4. Stories complete and integrate independently

---

## Notes

- [P] tasks = different files, no dependencies on incomplete tasks
- [Story] label maps task to specific user story for traceability
- Each user story is independently completable and testable after its prerequisites
- All notebooks include inline data quality assertions (row counts, null checks, schema validation)
- All notebooks create their target Delta tables with CREATE TABLE IF NOT EXISTS on first run
- Control table (control_pipeline_state) is initialized by the first ingestion notebook that runs
- Error/quality logging tables are created on first write by each notebook
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- Beta APIs (Copilot usage reports, Security Audit Log) are documented as known risks with migration paths in research.md
