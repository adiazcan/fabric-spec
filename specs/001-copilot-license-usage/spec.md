# Feature Specification: M365 Copilot License Usage Analytics

**Feature Branch**: `001-copilot-license-usage`  
**Created**: 2026-02-25  
**Status**: Draft  
**Input**: User description: "Microsoft Fabric analytics solution to monitor and optimize Microsoft 365 Copilot license usage — identify licensed/unlicensed users, track usage metrics, collect audit logs, and calculate usage scores to surface underutilized licenses."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - License Assignment Data Ingestion (Priority: P1)

As an IT administrator, I need all user profiles and their Copilot license assignment status ingested into the Lakehouse daily, so that downstream analytics and future reports can accurately reflect license distribution across the organization.

**Why this priority**: License data is the foundational data layer — every other feature (usage tracking, scoring, audit, reporting) depends on knowing who holds a license. Without this, no downstream analysis is possible.

**Independent Test**: Can be fully tested by running the ingestion pipeline and querying Gold-layer tables to confirm every user in the organization is present with their current Copilot license status.

**Acceptance Scenarios**:

1. **Given** the daily ingestion pipeline has completed, **When** a consumer queries the Gold-layer license table, **Then** every user in the organization is listed with their current Copilot license assignment status (assigned / not assigned).
2. **Given** a user's Copilot license was added or removed since the last refresh, **When** the next daily refresh completes, **Then** the updated license status is reflected in the Gold-layer table within one business day.
3. **Given** the organization has users across multiple departments, **When** a consumer queries by department, **Then** only users in the selected scope are returned with accurate license counts.

---

### User Story 2 - Copilot Usage Data Ingestion (Priority: P1)

As an IT manager, I need per-user Copilot activity data — including which Microsoft 365 applications were used with Copilot and when each was last active — ingested into the Lakehouse daily, so that this data is available for future reporting and analysis of adoption patterns.

**Why this priority**: Usage data directly enables the core business goal of license optimization. Without per-user metrics in the Lakehouse, there is no foundation for scoring or future dashboards.

**Independent Test**: Can be fully tested by running the ingestion pipeline and querying Gold-layer tables to confirm per-user activity summaries including last-active date per application and apps used count.

**Acceptance Scenarios**:

1. **Given** usage data has been refreshed, **When** a consumer queries the Gold-layer usage table, **Then** each licensed user has a record showing the list of Copilot-enabled apps used (e.g., Word, Excel, Teams, Outlook, PowerPoint), last-activity date per app, and date of most recent overall activity.
2. **Given** a user has not used Copilot in the last 30 days, **When** a consumer queries for inactive users, **Then** that user appears with their last-active date.
3. **Given** usage data is retained indefinitely, **When** a consumer queries historical usage data, **Then** daily usage snapshots (D7 report rows) are available per user for the full historical period, enabling ad-hoc weekly and monthly aggregation via SQL queries.

---

### User Story 3 - Underutilized License Scoring (Priority: P2)

As a license optimization analyst, I need a computed usage score for each licensed user stored in the Gold layer, so that future reports or direct queries can surface a prioritized list of underutilized licenses for reallocation or training.

**Why this priority**: Scoring transforms raw usage data into actionable intelligence. This is the primary computed deliverable for cost optimization decisions but depends on the usage data from P1 stories.

**Independent Test**: Can be fully tested by running the scoring pipeline and querying the Gold-layer score table to verify that users with low activity have low scores and the underutilization flag is correctly set.

**Acceptance Scenarios**:

1. **Given** usage metrics are available for all licensed users, **When** the daily pipeline completes, **Then** each licensed user has a usage score between 0 and 100 calculated using the recency-weighted model (50% recency, 30% frequency, 20% breadth) stored in the Gold-layer score table.
2. **Given** a licensed user has no Copilot activity in the last 30 days (null or stale `last_activity_date`), **When** the scoring pipeline runs, **Then** that user receives a score of 0 and is flagged as "underutilized."
3. **Given** a configurable threshold (default: score ≤ 20), **When** a consumer queries the score table for underutilized users, **Then** all users below the threshold are returned with their score, last-active date, and department.
4. **Given** historical scores are retained indefinitely, **When** a consumer queries a user's score history, **Then** the full score trend over time is available to distinguish chronically inactive users from temporarily inactive ones.

---

### User Story 4 - Copilot Audit Log Ingestion (Priority: P2)

As a compliance officer, I need detailed audit logs of Copilot interactions ingested into the Lakehouse — including timestamps, application context, and event types — so that the data is available for compliance queries and future reporting.

**Why this priority**: Audit logs provide the granular event-level data needed for compliance and deeper behavioral analysis. While not required for basic usage data, they are essential for regulatory readiness and security posture.

**Independent Test**: Can be fully tested by running the audit log ingestion pipeline and querying Gold-layer audit tables by user, date range, and application.

**Acceptance Scenarios**:

1. **Given** audit log ingestion has completed, **When** a consumer queries audit data for a specific user and date range, **Then** all Copilot interaction events for that user within the range are returned with timestamp, application name, and event type.
2. **Given** new Copilot events occur in the tenant, **When** the next daily refresh completes, **Then** those events are available in the Gold-layer audit table within one business day of occurrence.
3. **Given** audit data is retained indefinitely, **When** a consumer queries historical audit data for any past date, **Then** all events since system inception are accessible for investigation.

---

### User Story 5 - Daily Automated Data Refresh (Priority: P1)

As a data platform operator, I need the entire data pipeline to execute automatically on a daily schedule with incremental loading, so that reports reflect near-current data without manual intervention or full data reloads.

**Why this priority**: Automated refresh is infrastructure-critical — without it, all reports become stale and the system provides no ongoing value. Incremental loading aligns with the constitution's mandatory Delta MERGE strategy.

**Independent Test**: Can be fully tested by triggering a pipeline run and verifying that only new or changed records are ingested, existing records are updated (not duplicated), and the pipeline completes within the expected time window.

**Acceptance Scenarios**:

1. **Given** the pipeline is scheduled for daily execution, **When** the scheduled time arrives, **Then** the pipeline starts automatically without manual intervention.
2. **Given** the previous run ingested data up to date D, **When** the incremental pipeline runs on date D+1, **Then** only records changed or created after date D are fetched and merged into the Lakehouse.
3. **Given** an upstream API is temporarily unavailable during the pipeline run, **When** the pipeline encounters a transient failure, **Then** it retries with exponential backoff (up to 5 retries) before marking the step as failed and logging the error.

---

### Edge Cases

- What happens when a user's Copilot license is removed mid-reporting period? The system retains historical data for the user and updates their license status to "not assigned" from the next refresh onward. Historical usage and scores remain available for trend analysis.
- How does the system handle users with licenses assigned but no usage data available (e.g., brand-new assignments)? These users appear with zero usage metrics and a score of 0, flagged as "new assignment" rather than "underutilized" for the first 14 days.
- What happens when the Microsoft Graph API or Usage Reports API returns partial data due to service degradation? The pipeline logs a warning, quarantines incomplete records, and proceeds with available data. A data freshness check prevents stale data from propagating to Gold-layer reports.
- How does the system handle organizational restructuring (department changes, user departures)? Departed users (disabled accounts) are retained in historical data with a "departed" flag. Department changes are reflected on the next refresh based on the latest organizational data from Microsoft Graph.
- What happens when the audit log volume exceeds expected thresholds? The pipeline uses pagination and incremental watermarking to process large audit log volumes without memory issues or timeouts. Rate-limiting thresholds are configured in `parameter.yml`.

## Clarifications

### Session 2026-02-25

- Q: What are the relative weights of the three usage score components (frequency, breadth, recency)? → A: Recency-weighted — 50% recency, 30% frequency, 20% breadth.
- Q: How long should data be retained (audit logs vs. other data)? → A: All data retained indefinitely — no deletion policy.
- Q: How should PII (emails, display names) be handled in the Gold layer? → A: No masking or separation — full user details visible to all report consumers.
- Q: How should operators be alerted on pipeline failures or data quality warnings? → A: Log-only — operators monitor `dq_results` and `audit_log` tables manually; no automated push notifications.
- Q: What is the reporting delivery mechanism (Power BI, direct SQL, etc.)? → A: Dashboards and reports are deferred to a separate future feature. This spec covers the data pipeline and Gold-layer tables only.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST ingest user profiles from Microsoft Graph API including display name, email, department, job title, account status, company name, and office location.
- **FR-002**: System MUST ingest Copilot license assignment data from Microsoft Graph API and maintain a current record of each user's license status (assigned / not assigned).
- **FR-003**: System MUST ingest Microsoft 365 Copilot usage reports via the Usage Reports API, capturing per-user last-activity dates per Copilot-enabled application and overall last-activity date.
- **FR-004**: System MUST ingest Copilot audit log events from the Microsoft 365 Unified Audit Log, capturing event timestamps, user identifiers, application context, and event types.
- **FR-005**: System MUST execute all data ingestion and transformation on a daily automated schedule using Fabric Data Pipelines.
- **FR-006**: System MUST load data incrementally using watermark-based strategies (Delta MERGE or high-watermark append) for all layers — no full table refreshes in production.
- **FR-007**: System MUST retain all historical data indefinitely (no automated deletion) across all sources — user profiles, license assignments, usage metrics, audit events, and usage scores — to support long-term trend analysis and compliance investigations.
- **FR-008**: System MUST calculate a usage score (0–100) for each licensed user using a recency-weighted model: 50% recency of last activity, 30% activity frequency (active-day count in last 30 days), 20% application breadth. Component formulas: `recency_score = max(0, 100 − (days_since_last_activity × 100 / 30))` clamped to [0, 100]; `frequency_score = min(100, active_days_in_period × 100 / 30)`; `breadth_score = apps_used_count × 100 / 8`. Final score = `0.50 × recency_score + 0.30 × frequency_score + 0.20 × breadth_score`, rounded to nearest integer.
- **FR-009**: System MUST flag licensed users as "underutilized" when their usage score falls at or below a configurable threshold (default: 20).
- **FR-010**: System MUST distinguish newly licensed users (within 14 days of assignment) from underutilized users to avoid false positives in reallocation recommendations.
- **FR-011**: Gold-layer tables MUST support querying by department, license status, usage score range, and activity date range to enable future reporting and ad-hoc analysis.
- **FR-012**: System MUST quarantine records that fail schema validation or null checks into a `_rejected` partition with structured error metadata.
- **FR-013**: System MUST enforce a data freshness check before Silver-layer processing; stale source data MUST raise an alertable warning.
- **FR-014**: System MUST track departed users (disabled accounts) with a "departed" flag while retaining their historical data.
- **FR-015**: Pipeline failures and data quality warnings MUST be recorded in the `gold_copilot_audit_log` and `gold_copilot_dq_results` Delta tables respectively. No automated push notifications (email, Teams) are required; operators will monitor these tables manually.

### Security & Compliance Requirements *(mandatory for all Fabric features)*

- **SEC-001**: Feature MUST use Service Principal authentication for all Microsoft Graph API, Usage Reports API, and Unified Audit Log access (Principle I). The Service Principal MUST be granted only the minimum required Microsoft Graph application permissions (e.g., `User.Read.All`, `Reports.Read.All`, `AuditLogsQuery.Read.All`).
- **SEC-002**: All secrets required by this feature (client secret, tenant ID, client ID) MUST be stored in Azure Key Vault and referenced by name only in `parameter.yml` (Principle II).
- **SEC-003**: All Microsoft Graph API calls, Usage Reports API calls, and Audit Log API calls MUST produce audit log entries written to the `gold_copilot_audit_log` Delta table (Principle VII).
- **SEC-004**: Data quality check results (row counts, null %, freshness) for user, license, usage, and audit data MUST be persisted to `gold_copilot_dq_results` after every pipeline run (Principle VII).
- **SEC-005**: Gold-layer tables and reports MUST display full user details (display names, email addresses) to all report consumers. No PII masking or column-level security is required for this feature. Access control is managed at the Fabric workspace level — only users with workspace access can view reports.

### Key Entities

- **User**: An individual in the organization's directory. Key attributes: unique identifier, display name, email, department, job title, account status (active / disabled), company name, office location.
- **License Assignment**: The relationship between a User and a Microsoft 365 Copilot license. Key attributes: user identifier, license SKU, assignment date, status (assigned / removed).
- **Usage Metric**: A periodic summary of a User's Copilot activity. Key attributes: user identifier, reporting period, per-application last-activity dates, overall last-activity date.
- **Audit Event**: A single Copilot interaction event from the Unified Audit Log. Key attributes: event identifier, user identifier, timestamp, application name, event type, operation details.
- **Usage Score**: A calculated measure of a licensed User's Copilot engagement. Key attributes: user identifier, score date, score value (0–100), score components (frequency, breadth, recency), underutilization flag.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: All organization users (licensed and unlicensed) are present in the Gold-layer license table within one business day of the daily refresh.
- **SC-002**: Per-user Copilot usage metrics are updated daily in the Gold-layer usage table and reflect activity from the prior business day.
- **SC-003**: Underutilized licenses (users with score ≤ 20 and license held for 14+ days) are identified and flagged in the Gold-layer score table with 100% recall — no underutilized license goes unrecorded.
- **SC-004**: Historical usage data and scores are queryable indefinitely for any user — no data is aged out or deleted.
- **SC-005**: Audit log events are queryable by user, date range, and application in Gold-layer tables within one business day of the event occurring.
- **SC-006**: The daily pipeline completes end-to-end within 60 minutes for an organization of up to 50,000 users.
- **SC-007**: Data quality issues (schema violations, null values in required fields, stale sources) are detected, quarantined, and reported — zero corrupt records reach the Gold layer.

## Assumptions

- The organization has Microsoft 365 E3/E5 or equivalent licensing that includes access to Microsoft Graph API, Usage Reports API, and Unified Audit Log.
- Audit logging is enabled in the Microsoft 365 tenant (Purview compliance portal) before pipeline deployment.
- The Service Principal used for API access will be granted the required Microsoft Graph application permissions by the tenant administrator prior to pipeline deployment.
- Usage report data from Microsoft 365 may have a 24–48 hour delay from the time of the actual activity per Microsoft's standard reporting latency.
- The organization size is expected to be up to 50,000 users; scaling beyond this may require pipeline performance tuning.
- The usage score formula (activity frequency, application breadth, recency) and underutilization threshold (default: 20) can be refined after initial deployment based on stakeholder feedback.
- Dashboards and Power BI reports are out of scope for this feature and will be delivered in a subsequent feature spec. This feature delivers queryable Gold-layer Delta tables only.
