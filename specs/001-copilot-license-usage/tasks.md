# Tasks: M365 Copilot License Usage Analytics

**Input**: Design documents from `/specs/001-copilot-license-usage/`
**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/

**Tests**: No unit test tasks generated — spec defines data quality checks via `dq_results` Delta table and pipeline smoke-tests via `fab run`. Linting via `ruff`.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **Workspace items**: `workspace/<ItemName>.<ItemType>/` per `fabric-cicd` convention
- **Notebooks**: `workspace/<name>.Notebook/notebook-content.py`
- **Pipelines**: `workspace/<name>.DataPipeline/pipeline-content.json`
- **Lakehouse**: `workspace/<name>.Lakehouse/.platform`
- **Environment**: `workspace/<name>.Environment/Setting.yml`, `Sparkcompute.yml`
- **CI/CD**: `.github/workflows/deploy.yml`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Create the repository directory structure and Fabric item metadata files

- [ ] T001 Create workspace directory structure with all item folders: `workspace/00_watermarks.Notebook/`, `workspace/01_ingest_users.Notebook/`, `workspace/02_ingest_usage.Notebook/`, `workspace/03_ingest_audit_logs.Notebook/`, `workspace/04_transform_silver.Notebook/`, `workspace/05_transform_gold.Notebook/`, `workspace/06_compute_scores.Notebook/`, `workspace/99_dq_checks.Notebook/`, `workspace/helpers.Notebook/`, `workspace/CopilotUsagePipeline.DataPipeline/`, `workspace/CopilotUsageLakehouse.Lakehouse/`, `workspace/CopilotUsageEnv.Environment/`
- [ ] T002 [P] Create Lakehouse platform metadata in `workspace/CopilotUsageLakehouse.Lakehouse/.platform`
- [ ] T003 [P] Create Environment configuration with Python dependencies (`msgraph-sdk`, `msgraph-beta-sdk`, `azure-identity`) in `workspace/CopilotUsageEnv.Environment/Setting.yml`
- [ ] T004 [P] Create Spark pool configuration in `workspace/CopilotUsageEnv.Environment/Sparkcompute.yml`
- [ ] T005 [P] Create `workspace/parameter.yml` with `find_replace` entries for Lakehouse GUIDs, workspace IDs, Key Vault URLs, and `key_value_replace` for pipeline notebook references and schedule enable/disable per environment (DEV/PROD) — Principle II & VIII

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core shared utilities, system tables, and watermark management that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [ ] T006 Implement Service Principal authentication helper (`get_graph_client`, `get_beta_graph_client`) using `ClientSecretCredential` with Key Vault secret retrieval via `notebookutils.credentials.getSecret()` in `workspace/helpers.Notebook/notebook-content.py` — Principle I & II
- [ ] T007 Implement exponential backoff retry utility (`retry_with_backoff`) with full jitter (base 1s, max 60s, max 5 retries) and 4xx fast-fail (except 429) in `workspace/helpers.Notebook/notebook-content.py` — Principle VI
- [ ] T008 Implement structured audit logging utility (`write_audit_entry`) writing to `gold_copilot_audit_log` Delta table with run_id, timestamp, notebook_name, operation, target, status, records_affected, duration_ms, error_message, http_status_code fields in `workspace/helpers.Notebook/notebook-content.py` — Principle VII
- [ ] T009 Implement data quality check utility (`write_dq_result`) writing to `gold_copilot_dq_results` Delta table with check_id, run_id, check_date, table_name, check_name, check_result, metric_value, threshold, details fields in `workspace/helpers.Notebook/notebook-content.py` — Principle VII
- [ ] T010 Implement schema validation utility (`validate_schema`, `quarantine_rejected`) for type checking, null checks on required fields, and rejection to `_rejected` partition tables with structured error metadata in `workspace/helpers.Notebook/notebook-content.py` — Principle III (FR-012)
- [ ] T011 Implement Graph API pagination helper (`paginate_graph_response`) to follow `odata_next_link` until exhausted, collecting all pages into a single list in `workspace/helpers.Notebook/notebook-content.py`
- [ ] T012 Implement shared run_id generation and parameter loading (`load_parameters`, `generate_run_id`) reading from `parameter.yml` in `workspace/helpers.Notebook/notebook-content.py`
- [ ] T013 Implement watermark management notebook with `read_watermark(source_name)` and `write_watermark(source_name, value, run_id, records_processed)` functions operating on `sys_watermarks` Delta table in `workspace/00_watermarks.Notebook/notebook-content.py`

**Checkpoint**: Foundation ready — Service Principal auth, Key Vault integration, audit/DQ tables, retry utility, schema validation, pagination helper, watermark management, and parameter loading in place. User story work can begin.

---

## Phase 3: User Story 1 — License Assignment Data Ingestion (Priority: P1) 🎯 MVP

**Goal**: Ingest all user profiles and Copilot license assignments from Microsoft Graph into Bronze, validate and merge into Silver, and produce a Gold-layer license summary table

**Independent Test**: Run the ingestion pipeline and query `gold_copilot_license_summary` to confirm every user is present with their current Copilot license status, filterable by department

### Implementation for User Story 1

- [ ] T014 [US1] Implement Bronze ingestion in `workspace/01_ingest_users.Notebook/notebook-content.py`: authenticate via helpers, call `GET /v1.0/users?$select=...&$expand=licenseDetails&$top=999` using `msgraph-sdk`, handle pagination, support delta queries (`deltaLink` watermark from `sys_watermarks`), write raw JSON per user to `bronze_graph_users` partitioned by `ingestion_date`, update watermark, write audit log entries per contract `contracts/graph-users.md`
- [ ] T015 [US1] Implement Silver user transform in `workspace/04_transform_silver.Notebook/notebook-content.py`: read latest `bronze_graph_users` partition, parse `raw_json`, validate schema (required: `user_id`, `user_principal_name`, `display_name`, `account_enabled`), quarantine invalid records to `silver_users_rejected`, Delta MERGE into `silver_users` on `user_id`, derive `is_departed` from `account_enabled` (FR-014)
- [ ] T016 [US1] Implement Silver license transform in `workspace/04_transform_silver.Notebook/notebook-content.py`: extract `licenseDetails` from parsed Bronze data, identify Copilot SKU (`MICROSOFT_365_COPILOT` case-insensitive), Delta MERGE into `silver_user_licenses` on `user_id`, track `license_assignment_date` (retain earliest), set `license_removal_date` when Copilot SKU disappears
- [ ] T017 [US1] Implement Gold license summary dimension in `workspace/05_transform_gold.Notebook/notebook-content.py`: join `silver_users` + `silver_user_licenses`, compute `account_status` (active/departed), `license_days_held`, `is_new_assignment` (< 14 days, FR-010), Delta MERGE into `gold_copilot_license_summary` on `user_id`
- [ ] T018 [US1] Implement user/license data quality checks in `workspace/99_dq_checks.Notebook/notebook-content.py`: row count check on `silver_users`, null percentage check on required fields (`user_id`, `display_name`), freshness check comparing `max(_ingestion_date)` against current date (FR-013), write all results to `sys_dq_results`

**Checkpoint**: User Story 1 complete — all org users with Copilot license status queryable in `gold_copilot_license_summary`, filterable by department, with DQ validation and audit logging

---

## Phase 4: User Story 2 — Copilot Usage Data Ingestion (Priority: P1)

**Goal**: Ingest per-user Copilot usage metrics (interaction dates, apps used) from Microsoft Graph beta into the Lakehouse and enrich the Gold-layer license summary with activity data

**Independent Test**: Run the usage ingestion pipeline and query `gold_copilot_license_summary` to confirm per-user activity summaries including last-active date and apps_used_count

### Implementation for User Story 2

- [ ] T019 [US2] Implement Bronze usage ingestion in `workspace/02_ingest_usage.Notebook/notebook-content.py`: authenticate via helpers with beta client, call `GET /beta/reports/getMicrosoft365CopilotUsageUserDetail(period='D7')` with `Accept: application/json`, handle pagination, write raw JSON per user-report to `bronze_graph_usage_reports` partitioned by `ingestion_date`, compare `reportRefreshDate` against watermark to skip already-ingested data, update watermark, write audit log entries per contract `contracts/graph-usage-reports.md`
- [ ] T020 [US2] Implement Silver usage transform in `workspace/04_transform_silver.Notebook/notebook-content.py`: read latest `bronze_graph_usage_reports` partition, parse `raw_json`, map API fields to Silver columns per contract field mapping (cast dates, prefix report_period with `D`), validate schema, quarantine invalid records to `silver_usage_rejected`, Delta MERGE into `silver_copilot_usage` on composite key (`user_principal_name` + `report_refresh_date`)
- [ ] T021 [US2] Update Gold license summary in `workspace/05_transform_gold.Notebook/notebook-content.py`: join `gold_copilot_license_summary` with latest `silver_copilot_usage` on UPN, populate `last_activity_date`, compute `days_since_last_activity` and `apps_used_count` (count non-null per-app columns), Delta MERGE update
- [ ] T022 [US2] Implement usage data quality checks in `workspace/99_dq_checks.Notebook/notebook-content.py`: row count check on `silver_copilot_usage`, freshness check on `report_refresh_date` (expect within 48 hours), null percentage on `user_principal_name`, write results to `sys_dq_results`

**Checkpoint**: User Story 2 complete — per-user Copilot activity data (last-active date, apps used) visible in `gold_copilot_license_summary`, historical usage in `silver_copilot_usage`

---

## Phase 5: User Story 3 — Underutilized License Scoring (Priority: P2)

**Goal**: Calculate a recency-weighted usage score (0–100) per licensed user and flag underutilized licenses in the Gold layer

**Independent Test**: Run the scoring pipeline and query `gold_copilot_usage_scores` to verify users with zero activity get score 0 and are flagged underutilized; verify score components sum correctly with weights 50/30/20

**Depends on**: US1 (license data) + US2 (usage data)

### Implementation for User Story 3

- [ ] T023 [US3] Implement score computation in `workspace/06_compute_scores.Notebook/notebook-content.py`: read `silver_copilot_usage` + `silver_user_licenses`, compute recency_score (50% weight: based on `days_since_last_activity`), frequency_score (30% weight: based on `active_days_in_period`), breadth_score (20% weight: based on `apps_used_count` out of 8), calculate final `usage_score` (0–100), set `is_underutilized` (score ≤ configurable threshold from `parameter.yml`, default 20, AND license held ≥ 14 days), set `is_new_assignment` (license < 14 days, FR-010), insert into `gold_copilot_usage_scores` (insert-only, one row per user per score_date)
- [ ] T024 [US3] Update Gold license summary with latest scores in `workspace/05_transform_gold.Notebook/notebook-content.py`: join `gold_copilot_license_summary` with latest `gold_copilot_usage_scores` on `user_id`, populate `latest_usage_score` and `is_underutilized`, Delta MERGE update
- [ ] T025 [US3] Implement score data quality checks in `workspace/99_dq_checks.Notebook/notebook-content.py`: verify all licensed users have a score row for today's `score_date`, verify score range 0–100, verify `is_underutilized` flag consistency (score ≤ threshold AND not new assignment), write results to `sys_dq_results`

**Checkpoint**: User Story 3 complete — every licensed user has a daily usage score in `gold_copilot_usage_scores` with correct underutilization flagging; historical score trends available

---

## Phase 6: User Story 4 — Copilot Audit Log Ingestion (Priority: P2)

**Goal**: Ingest detailed Copilot audit logs from the Purview Audit Search API using the 3-step async query pattern and produce a Gold-layer audit summary

**Independent Test**: Run the audit ingestion pipeline and query `gold_copilot_audit_summary` by user, date range, and application to confirm event counts

### Implementation for User Story 4

- [ ] T026 [US4] Implement Bronze audit log ingestion in `workspace/03_ingest_audit_logs.Notebook/notebook-content.py`: authenticate via helpers with beta client, implement 3-step async pattern per contract `contracts/graph-audit-logs.md` — (1) `POST /beta/security/auditLog/queries` with `filterStartDateTime` from watermark and `serviceFilter="MicrosoftCopilot"`, (2) poll `GET /beta/security/auditLog/queries/{id}` with exponential backoff (start 5s, max 60s, timeout 10 min) until `status=="succeeded"`, (3) `GET /beta/security/auditLog/queries/{id}/records` with pagination, write raw JSON per record to `bronze_graph_audit_logs` partitioned by `ingestion_date`, update watermark with `filterEndDateTime`, write audit log entries
- [ ] T027 [US4] Implement Silver audit transform in `workspace/04_transform_silver.Notebook/notebook-content.py`: read latest `bronze_graph_audit_logs` partition, parse `raw_json`, map fields to Silver columns per contract field mapping (`event_id`, `created_date_time`, `operation`, `service`, `record_type`, `user_principal_name`, `user_id`, `user_type`, `client_ip`, `object_id`, `organization_id`, serialize `auditData` to `audit_data_json`), validate schema, quarantine to `silver_audit_rejected`, Delta MERGE into `silver_audit_events` on `event_id` (insert-only — skip existing)
- [ ] T028 [US4] Implement Gold audit summary aggregation in `workspace/05_transform_gold.Notebook/notebook-content.py`: aggregate `silver_audit_events` by `user_id` + `activity_date` + `application_name`, compute `event_count`, `success_count`, `failure_count`, Delta MERGE into `gold_copilot_audit_summary` on composite key (upsert for late-arriving events)
- [ ] T029 [US4] Implement audit data quality checks in `workspace/99_dq_checks.Notebook/notebook-content.py`: row count check on `silver_audit_events`, null percentage on `event_id` and `created_date_time`, freshness check on `max(created_date_time)`, write results to `sys_dq_results`

**Checkpoint**: User Story 4 complete — Copilot audit events queryable by user, date range, and application in `gold_copilot_audit_summary`; full event detail in `silver_audit_events`

---

## Phase 7: User Story 5 — Daily Automated Data Refresh (Priority: P1)

**Goal**: Wire all notebooks into an orchestrated Fabric Data Pipeline with daily 6 AM schedule, incremental loading, and failure handling

**Independent Test**: Trigger a pipeline run via `fab run` and verify only new/changed records are ingested, existing records are updated (not duplicated), and pipeline completes within 60 minutes

### Implementation for User Story 5

- [ ] T030 [US5] Create pipeline definition in `workspace/CopilotUsagePipeline.DataPipeline/pipeline-content.json`: define sequential notebook activities in order — `00_watermarks` → `01_ingest_users` → `02_ingest_usage` → `03_ingest_audit_logs` → `04_transform_silver` → `05_transform_gold` → `06_compute_scores` → `99_dq_checks`, configure notebook references using parameterised IDs (`$items.Notebook.<name>.$id`), set Lakehouse binding per activity
- [ ] T031 [US5] Configure daily 6 AM UTC schedule trigger in `workspace/CopilotUsagePipeline.DataPipeline/pipeline-content.json`: add `schedules` array with `jobType: "Execute"`, set `enabled: false` for DEV (parameterised to `true` for PROD via `parameter.yml`)
- [ ] T032 [US5] Implement data freshness gate in `workspace/04_transform_silver.Notebook/notebook-content.py`: before Silver processing, check `max(ingestion_date)` of each Bronze table against current date; if stale (> 48 hours), log warning to `gold_copilot_dq_results` and `gold_copilot_audit_log` (FR-013); proceed with available data
- [ ] T033 [US5] Add pipeline-level error handling in `workspace/CopilotUsagePipeline.DataPipeline/pipeline-content.json`: configure retry policy on each notebook activity (max 3 retries with backoff), set failure dependencies so `99_dq_checks` runs even if upstream notebooks fail, log pipeline status to `gold_copilot_audit_log`
- [ ] T034 [US5] Verify incremental loading end-to-end: confirm `01_ingest_users` uses `deltaLink` watermark, `02_ingest_usage` uses `reportRefreshDate` watermark, `03_ingest_audit_logs` uses `filterStartDateTime` watermark, all Silver transforms use Delta MERGE (no full refresh) — test by running pipeline twice and verifying no duplicate records

**Checkpoint**: User Story 5 complete — fully automated daily pipeline with incremental loading, freshness gates, and retry handling; completes within 60 minutes for 50K users

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: CI/CD automation, code quality, and deployment validation

- [ ] T035 [P] Create GitHub Actions CI/CD workflow in `.github/workflows/deploy.yml`: trigger on push to `main`, steps — checkout → setup Python 3.11 → `pip install ruff fabric-cicd ms-fabric-cli` → `ruff check workspace/` → `gitleaks detect` → `fab auth login` → `fabric-cicd publish_all_items()` → `fab run CopilotUsagePipeline.DataPipeline` (smoke-test) — Principle VIII
- [ ] T036 [P] Create `ruff` configuration (in `pyproject.toml` or `ruff.toml` at repo root) with Python 3.11 target, enable import sorting, line length 120, and include `workspace/**/*.py`
- [ ] T037 [P] Add `# META` header blocks to all notebook files (`workspace/**/notebook-content.py`) with `default_lakehouse` and `default_lakehouse_workspace_id` placeholders for `fabric-cicd` regex parameterisation
- [ ] T038 Run `quickstart.md` validation: verify all `fab` CLI commands from `specs/001-copilot-license-usage/quickstart.md` execute successfully against a DEV workspace (provision, deploy, wire bindings, trigger pipeline, verify data)
- [ ] T039 Performance validation: run full pipeline against realistic data volume (≤ 50K users), verify end-to-end completion within 60 minutes (SC-006), verify pagination handles all pages, verify memory usage is bounded during audit log processing

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **US1 (Phase 3)**: Depends on Foundational — no dependencies on other stories
- **US2 (Phase 4)**: Depends on Foundational — no dependencies on other stories; can run in parallel with US1
- **US3 (Phase 5)**: Depends on US1 + US2 (needs both license and usage data for scoring)
- **US4 (Phase 6)**: Depends on Foundational — no dependencies on other stories; can run in parallel with US1/US2
- **US5 (Phase 7)**: Depends on US1 + US2 + US3 + US4 (wires all notebooks into pipeline)
- **Polish (Phase 8)**: Depends on all user stories being complete

### User Story Dependencies

- **US1 (P1)**: Independent after Foundational → `01_ingest_users`, `04_transform_silver` (users), `05_transform_gold` (license summary), `99_dq_checks` (users)
- **US2 (P1)**: Independent after Foundational → `02_ingest_usage`, `04_transform_silver` (usage), `05_transform_gold` (usage enrichment), `99_dq_checks` (usage)
- **US3 (P2)**: Depends on US1 + US2 → `06_compute_scores`, `05_transform_gold` (score enrichment), `99_dq_checks` (scores)
- **US4 (P2)**: Independent after Foundational → `03_ingest_audit_logs`, `04_transform_silver` (audit), `05_transform_gold` (audit summary), `99_dq_checks` (audit)
- **US5 (P1)**: Depends on US1 + US2 + US3 + US4 → `CopilotUsagePipeline.DataPipeline`, `04_transform_silver` (freshness gate)

### Within Each User Story

- Bronze ingestion before Silver transform
- Silver transform before Gold aggregation
- Gold aggregation before DQ checks
- Core implementation before enrichment

### Parallel Opportunities

- All Setup tasks marked [P] (T002–T005) can run in parallel
- All Foundational tasks editing `helpers.Notebook` (T006–T012) are in the same file — execute sequentially; T013 (watermarks) is a separate file and can run in parallel
- US1 and US2 can run in parallel after Foundational (different Bronze notebooks, different Silver/Gold sections)
- US4 can run in parallel with US1/US2 (independent audit log pipeline)
- All Polish tasks marked [P] (T035–T037) can run in parallel

---

## Parallel Example: After Foundational

```
# US1 and US2 can start simultaneously:
Stream A (US1): T014 → T015 → T016 → T017 → T018
Stream B (US2): T019 → T020 → T021 → T022
Stream C (US4): T026 → T027 → T028 → T029

# After US1 + US2 complete:
Stream D (US3): T023 → T024 → T025

# After all stories complete:
Stream E (US5): T030 → T031 → T032 → T033 → T034

# Polish (parallel with or after US5):
Stream F: T035 | T036 | T037 → T038 → T039
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL — blocks all stories)
3. Complete Phase 3: User Story 1 — License Assignment Data Ingestion
4. **STOP and VALIDATE**: Query `gold_copilot_license_summary` to confirm all users with license status
5. Deploy to DEV if ready

### Incremental Delivery

1. Setup + Foundational → Foundation ready
2. Add US1 → License data in Gold → Validate independently (MVP!)
3. Add US2 → Usage data in Gold → Validate independently
4. Add US3 → Scoring in Gold → Validate independently (depends on US1 + US2)
5. Add US4 → Audit data in Gold → Validate independently
6. Add US5 → Full automated pipeline → End-to-end validation
7. Polish → CI/CD, linting, performance validation → Production-ready

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: US1 (License ingestion)
   - Developer B: US2 (Usage ingestion)
   - Developer C: US4 (Audit ingestion)
3. After US1 + US2: Developer A or B takes US3 (Scoring)
4. After all stories: Any developer takes US5 (Pipeline orchestration)
5. Team collaborates on Polish

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable (except US3 which depends on US1 + US2)
- Shared notebooks (`04_transform_silver`, `05_transform_gold`, `99_dq_checks`) are extended incrementally per story — each story adds its section
- All notebooks reference `helpers.Notebook` via `%run` or `notebookutils.notebook.run()`
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
