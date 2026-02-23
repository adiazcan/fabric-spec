# Feature Specification: Microsoft 365 Copilot License Usage Analytics

**Feature Branch**: `001-copilot-license-optimization`  
**Created**: 2026-02-23  
**Status**: Draft  
**Input**: User description: "I am building a Microsoft Fabric analytics solution to monitor and optimize Microsoft 365 Copilot license usage"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - License Utilization Overview (Priority: P1)

As an IT administrator, I want to see an executive summary of Copilot license utilization across my organization so I can quickly understand overall adoption health and identify cost-saving opportunities.

**Why this priority**: License cost optimization is the primary business driver. Without visibility into utilization rates, the organization cannot make informed decisions about license allocation. This delivers immediate, measurable financial value.

**Independent Test**: Can be fully tested by loading license assignment data and usage metrics, then verifying that the dashboard displays accurate KPIs (total licenses, active users, utilization rate, estimated savings). Delivers value as a standalone executive view.

**Acceptance Scenarios**:

1. **Given** the system has ingested license assignment and usage data, **When** the IT admin opens the executive dashboard, **Then** they see KPIs showing total Copilot licenses, number of active users, overall utilization rate (percentage), and estimated monthly savings from unused licenses.
2. **Given** the organization has 500 Copilot licenses with 350 active users, **When** the dashboard loads, **Then** it displays a 70% utilization rate and highlights 150 underutilized licenses with their associated cost.
3. **Given** usage data has been collected for at least 30 days, **When** the admin views the executive summary, **Then** they see a trend line showing adoption rate changes over time.

---

### User Story 2 - Identify Underutilized Licenses (Priority: P1)

As an IT administrator, I want to see a list of users who have Copilot licenses but have not used Copilot in the last 30 days, so I can reclaim those licenses and assign them to users who would benefit more.

**Why this priority**: Directly drives cost savings by identifying licenses that can be reallocated. This is the most actionable output of the system and the core use case.

**Independent Test**: Can be fully tested by loading user license data alongside usage activity data, identifying users with zero or minimal activity in the past 30 days, and producing a filterable list with export capability.

**Acceptance Scenarios**:

1. **Given** the system has license and usage data, **When** the IT admin navigates to the underutilized licenses view, **Then** they see a table of users with Copilot licenses who have had no Copilot activity in the past 30 days, including user name, department, location, license assignment date, and last activity date.
2. **Given** the admin is viewing the underutilized licenses list, **When** they click "Export to CSV", **Then** a CSV file is downloaded containing all listed users and their details for action by the IT team.
3. **Given** the admin wants to refine the inactivity threshold, **When** they adjust the "days inactive" filter (e.g., from 30 to 14 days), **Then** the list updates to reflect the new threshold.

---

### User Story 3 - Departmental Adoption Analysis (Priority: P2)

As a department manager or IT leader, I want to see Copilot adoption metrics broken down by department and location so I can identify teams that need additional training or enablement support.

**Why this priority**: Enables targeted adoption strategies rather than blanket company-wide efforts. Helps justify training investments and supports organizational change management.

**Independent Test**: Can be tested by verifying that usage data is aggregated correctly by department and location, and that drill-down navigation from department-level to user-level works correctly.

**Acceptance Scenarios**:

1. **Given** usage and organizational data have been ingested, **When** the admin selects the department analysis view, **Then** they see a breakdown of utilization rate, average usage score, and active user count per department.
2. **Given** the admin is viewing department-level data, **When** they click on a specific department, **Then** they drill down to see individual user usage details within that department.
3. **Given** the organization has users across multiple locations, **When** the admin filters by location, **Then** the department metrics update to show only users in the selected location.

---

### User Story 4 - License Assignment Candidates (Priority: P2)

As an IT administrator, I want to identify users without Copilot licenses who show high activity in Microsoft 365 productivity tools (Word, Excel, PowerPoint, Teams, Outlook), so I can prioritize them for license assignment.

**Why this priority**: Maximizes ROI of Copilot licenses by assigning them to users most likely to benefit. Complements the underutilized license identification (User Story 2) by providing a data-driven reassignment strategy.

**Independent Test**: Can be tested by loading Microsoft 365 activity data for unlicensed users, scoring them by productivity tool engagement, and verifying the ranked recommendation list.

**Acceptance Scenarios**:

1. **Given** the system has Microsoft 365 usage data and license assignments, **When** the admin views the license candidates report, **Then** they see a ranked list of users without Copilot licenses, ordered by their Microsoft 365 activity score.
2. **Given** the candidate list is displayed, **When** the admin exports the list, **Then** they receive a CSV with user details, activity scores, and recommended priority for license assignment.

---

### User Story 5 - Usage Trend Analysis (Priority: P2)

As an IT leader, I want to see time-series charts showing Copilot adoption trends over time so I can measure the impact of training programs and organizational initiatives.

**Why this priority**: Provides historical context and measures the effectiveness of adoption strategies. Essential for long-term planning and executive reporting.

**Independent Test**: Can be tested by verifying that historical usage data is correctly aggregated into daily/weekly/monthly time series and displayed as trend charts with proper date axes.

**Acceptance Scenarios**:

1. **Given** the system has at least 30 days of historical data, **When** the admin views the trend analysis, **Then** they see a time-series chart showing daily active Copilot users, total interactions, and utilization rate over time.
2. **Given** the admin is viewing trend data, **When** they select a specific date range, **Then** the chart updates to display only data within that range.
3. **Given** the admin wants to compare departments, **When** they select multiple departments in the filter, **Then** the chart displays separate trend lines for each selected department.

---

### User Story 6 - Automated Usage Alerts (Priority: P3)

As an IT administrator, I want to receive automated email alerts when overall or individual usage drops below a configurable threshold so I can take proactive action without manually checking the dashboard.

**Why this priority**: Reduces the need for manual monitoring and enables proactive license management. Lower priority because the dashboards (P1/P2) deliver value first; alerts add automation on top.

**Independent Test**: Can be tested by configuring a usage threshold, simulating a usage drop below that threshold during a data refresh, and verifying that an email alert is sent to the configured recipients.

**Acceptance Scenarios**:

1. **Given** the admin has configured a utilization threshold of 60%, **When** the daily data refresh calculates an overall utilization rate below 60%, **Then** an email alert is sent to the configured recipients with a summary of the current utilization rate and a link to the dashboard.
2. **Given** the admin has configured individual user inactivity alerts for 30 days, **When** a licensed user crosses the 30-day inactivity mark, **Then** the system includes that user in the next alert digest.

---

### User Story 7 - Daily Data Refresh with Incremental Updates (Priority: P1)

As a system operator, I want the data pipeline to refresh automatically every day using incremental updates so the dashboard reflects recent usage patterns without requiring full data reloads.

**Why this priority**: Without reliable daily data refresh, all other user stories deliver stale data. This is foundational infrastructure that enables the entire solution.

**Independent Test**: Can be tested by running the pipeline on two consecutive days, verifying that only new/changed records are processed on the second run, and confirming that the dashboard reflects the latest data.

**Acceptance Scenarios**:

1. **Given** the pipeline is configured, **When** the scheduled daily refresh triggers, **Then** the system pulls only new and changed data from the source APIs since the last successful refresh.
2. **Given** the pipeline has been running for 90 days, **When** an admin queries historical data, **Then** all 90 days of data are available for trend analysis.
3. **Given** an API source is temporarily unavailable during a refresh, **When** the pipeline encounters the error, **Then** it logs the failure, retries up to 3 times, and sends a notification if all retries fail, without corrupting previously ingested data.

---

### Edge Cases

- What happens when a user's license is assigned or removed mid-reporting period? The system should reflect the license status as of the most recent data refresh and track the change in history.
- How does the system handle users who exist in multiple departments (e.g., joint appointments)? The system should use the user's primary department from Azure AD as the canonical department.
- What happens when Microsoft Graph API rate limits are exceeded during data ingestion? The pipeline should implement exponential backoff and retry, and log rate-limit events for monitoring.
- How does the system handle deleted or disabled user accounts? Disabled accounts should be flagged but retained in historical data; deleted accounts should be marked as "removed" while preserving their historical usage records.
- What happens when no usage data exists (e.g., first-time setup)? The dashboard should display a "No data available" state with guidance on when to expect the first data refresh.
- What if audit log data is delayed or arrives out of order? The pipeline should process records by event timestamp rather than ingestion timestamp and handle late-arriving data gracefully during the next refresh cycle.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST ingest user profile data including display name, email, department, location, and job title from the organizational directory.
- **FR-002**: System MUST identify all users with Microsoft 365 Copilot licenses assigned and track license assignment/removal dates.
- **FR-003**: System MUST collect Copilot usage metrics per user, including number of interactions, features used (e.g., Copilot in Word, Excel, Teams, Outlook, PowerPoint), and frequency of use.
- **FR-004**: System MUST collect audit log data for Copilot activities, including the application used, timestamps, and query types.
- **FR-005**: System MUST calculate a usage score per user based on interaction frequency, feature breadth, and recency of last activity, to classify users as "active", "low usage", or "inactive".
- **FR-006**: System MUST generate a report of users with Copilot licenses who have had no activity in a configurable inactivity period (default: 30 days).
- **FR-007**: System MUST generate a ranked list of users without Copilot licenses, ordered by their Microsoft 365 productivity tool activity, as candidates for license assignment.
- **FR-008**: System MUST calculate and display overall license utilization rate, segmented by department and location.
- **FR-009**: System MUST provide time-series visualizations of Copilot adoption trends (daily active users, total interactions, utilization rate) over a configurable date range.
- **FR-010**: System MUST support drill-down navigation from organization-level metrics to department-level and then to individual user-level detail.
- **FR-011**: System MUST allow export of underutilized license and license candidate reports as CSV files.
- **FR-012**: System MUST send automated email alerts when overall utilization rate drops below a configurable threshold.
- **FR-013**: System MUST refresh data daily via an automated pipeline using incremental updates (processing only new or changed records since the last refresh).
- **FR-014**: System MUST retain historical data for a minimum of 90 days to support trend analysis.
- **FR-015**: System MUST handle API errors (rate limiting, timeouts, temporary unavailability) with retry logic and failure notifications, without corrupting existing data.
- **FR-016**: System MUST display an executive summary dashboard with KPIs: total licenses, active users, utilization rate, and estimated cost savings from unused licenses.
- **FR-017**: System MUST preserve historical usage records for disabled or deleted user accounts, marking them appropriately.

### Key Entities

- **User**: Represents an individual in the organization. Key attributes: display name, email, department, location, job title, account status (active/disabled/deleted).
- **License Assignment**: Represents the assignment of a Copilot license to a user. Key attributes: user reference, license type, assignment date, removal date (if applicable), current status.
- **Usage Metric**: Aggregated usage data per user per time period. Key attributes: user reference, period (daily), interaction count, features used, last activity date, calculated usage score.
- **Audit Event**: Individual Copilot interaction event. Key attributes: user reference, timestamp, application (Word, Excel, Teams, etc.), query type.
- **Department**: Organizational unit. Key attributes: name, location, total user count, licensed user count.
- **Alert Configuration**: Defines thresholds and recipients for automated notifications. Key attributes: metric type, threshold value, recipient list, frequency.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: IT administrators can identify all underutilized Copilot licenses (users inactive for 30+ days) within 2 minutes of opening the dashboard.
- **SC-002**: Dashboard data is no more than 24 hours old at any point during business hours, reflecting the daily refresh cycle.
- **SC-003**: The system retains and displays at least 90 days of historical usage data for trend analysis.
- **SC-004**: License utilization rate is accurately calculated to within 1% of the actual value (validated against source systems).
- **SC-005**: Exportable CSV reports contain all required fields (user name, email, department, location, usage score, last activity date, recommendation) and can be opened without errors in standard spreadsheet applications.
- **SC-006**: Automated alerts are delivered within 1 hour of a daily refresh that detects a threshold breach.
- **SC-007**: Department and location drill-down navigation loads results within 5 seconds.
- **SC-008**: The system correctly handles incremental data updates, processing only changed records, reducing daily pipeline runtime compared to a full reload.
- **SC-009**: Usage score classification (active, low usage, inactive) is consistent and reproducible given the same input data.
- **SC-010**: 90% of IT administrators can complete their primary tasks (identify underutilized licenses, export reports, view trends) on first use without additional training.

## Assumptions

- The organization has an active Microsoft 365 tenant with Microsoft 365 Copilot licenses deployed.
- Microsoft Graph API and Microsoft 365 Usage Reports API access is available with appropriate permissions (e.g., Reports.Read.All, AuditLog.Read.All, User.Read.All).
- The Microsoft Fabric workspace is provisioned with sufficient capacity for data storage and daily pipeline execution.
- Audit logging is enabled in the Microsoft 365 tenant for Copilot activities.
- User department and location data is maintained in Azure Active Directory (Entra ID) and is reasonably up to date.
- The organization uses standard Microsoft 365 Copilot features (Word, Excel, PowerPoint, Teams, Outlook); custom or third-party Copilot extensions are out of scope.
- License cost information will be provided as a configuration parameter (e.g., cost per license per month) rather than pulled from a billing API.
- Email alert delivery uses the organization's existing email infrastructure (e.g., Microsoft 365 email, Power Automate, or equivalent notification mechanism).
- The solution will be accessed by IT administrators and IT leaders; end-user self-service is out of scope.
