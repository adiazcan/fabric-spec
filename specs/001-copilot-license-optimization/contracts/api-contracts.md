# API Contracts: Microsoft 365 Copilot License Usage Analytics

**Feature Branch**: `001-copilot-license-optimization`
**Date**: 2026-02-23

This document defines the external API contracts consumed by the data pipeline and the semantic model contract exposed to Power BI.

---

## 1. Consumed APIs

### 1.1 Microsoft Graph API — Users

**Endpoint**: `GET https://graph.microsoft.com/v1.0/users`

**Authentication**: `msgraph-sdk` `GraphServiceClient` with `azure-identity` `ClientSecretCredential`

**Request**:
```http
GET /v1.0/users?$select=id,displayName,mail,userPrincipalName,department,officeLocation,jobTitle,accountEnabled&$top=999
Authorization: Bearer {access_token}
ConsistencyLevel: eventual
```

**Response** (200 OK):
```json
{
  "@odata.context": "https://graph.microsoft.com/v1.0/$metadata#users",
  "@odata.nextLink": "https://graph.microsoft.com/v1.0/users?$skiptoken=...",
  "value": [
    {
      "id": "string (GUID)",
      "displayName": "string",
      "mail": "string | null",
      "userPrincipalName": "string",
      "department": "string | null",
      "officeLocation": "string | null",
      "jobTitle": "string | null",
      "accountEnabled": "boolean"
    }
  ]
}
```

**Pagination**: Follow `@odata.nextLink` until absent. Do NOT use `$skip`.

**Required Permission**: `User.Read.All` (application)

**Rate Limit**: 8,000 resource units / 10 seconds (large tenant)

---

### 1.2 Microsoft Graph API — User License Assignments

**Endpoint**: `GET https://graph.microsoft.com/v1.0/users`

**Request**:
```http
GET /v1.0/users?$select=id,userPrincipalName,assignedLicenses&$top=999
Authorization: Bearer {access_token}
```

**Response** (200 OK):
```json
{
  "value": [
    {
      "id": "string (GUID)",
      "userPrincipalName": "string",
      "assignedLicenses": [
        {
          "skuId": "string (GUID)",
          "disabledPlans": ["string (GUID)"]
        }
      ]
    }
  ]
}
```

**Copilot SKU Filter** (optional):
```http
GET /v1.0/users?$filter=assignedLicenses/any(l:l/skuId eq '639dec6b-bb19-468b-871c-c5c441c4b0cb')&$select=id,userPrincipalName,assignedLicenses
```

**Required Permission**: `User.Read.All` (application)

**Known Copilot SKU GUIDs**:
- `639dec6b-bb19-468b-871c-c5c441c4b0cb` — Copilot for Microsoft 365
- `a809996b-059e-42e2-9866-db24b99a9782` — Microsoft Copilot for Microsoft 365
- `ad9c22b3-52d7-4e7e-973c-88121ea96436` — Copilot for Microsoft 365 (EDU)

---

### 1.3 Microsoft Graph API — Subscribed SKUs *(Reference Only — not consumed by pipeline)*

**Endpoint**: `GET https://graph.microsoft.com/v1.0/subscribedSkus`

**Request**:
```http
GET /v1.0/subscribedSkus
Authorization: Bearer {access_token}
```

**Response** (200 OK):
```json
{
  "value": [
    {
      "id": "string",
      "skuId": "string (GUID)",
      "skuPartNumber": "string",
      "consumedUnits": "integer",
      "prepaidUnits": {
        "enabled": "integer",
        "suspended": "integer",
        "warning": "integer"
      },
      "servicePlans": [
        {
          "servicePlanId": "string (GUID)",
          "servicePlanName": "string",
          "provisioningStatus": "string"
        }
      ]
    }
  ]
}
```

**Required Permission**: `Organization.Read.All` (application)

---

### 1.4 Microsoft Graph API — Copilot Usage Reports (Beta)

**Endpoint**: `GET https://graph.microsoft.com/beta/reports/getMicrosoft365CopilotUsageUserDetail(period='{period}')`

**Request**:
```http
GET /beta/reports/getMicrosoft365CopilotUsageUserDetail(period='D30')?$format=application/json
Authorization: Bearer {access_token}
```

**Response** (200 OK, JSON):
```json
{
  "value": [
    {
      "reportRefreshDate": "2026-02-22",
      "reportPeriod": 30,
      "userPrincipalName": "string",
      "displayName": "string",
      "lastActivityDate": "2026-02-20 | null",
      "microsoftTeamsCopilotLastActivityDate": "2026-02-15 | null",
      "wordCopilotLastActivityDate": "2026-02-18 | null",
      "excelCopilotLastActivityDate": "null",
      "powerPointCopilotLastActivityDate": "null",
      "outlookCopilotLastActivityDate": "2026-02-20 | null",
      "oneNoteCopilotLastActivityDate": "null",
      "loopCopilotLastActivityDate": "null",
      "copilotChatLastActivityDate": "2026-02-19 | null",
      "copilotActivityUserDetailsByPeriod": [
        {
          "reportPeriod": 30
        }
      ]
    }
  ],
  "@odata.nextLink": "https://graph.microsoft.com/beta/reports/getMicrosoft365CopilotUsageUserDetail(period='D30')?$format=application/json&$skiptoken=MDoyOg"
}
```

**Available Periods**: `D7`, `D30`, `D90`, `D180`, `ALL`

**Rate Limit**: 100 requests / 10 minutes (JSON, beta)

**Required Permission**: `Reports.Read.All` (application)

**Important**: Beta API — subject to breaking changes. User identities may be anonymized (tenant setting).

---

### 1.5 Microsoft Graph API — Copilot Usage Trend *(Reference Only — not consumed by pipeline)*

**Endpoint**: `GET https://graph.microsoft.com/beta/reports/getMicrosoft365CopilotUserCountTrend(period='{period}')`

**Request**:
```http
GET /beta/reports/getMicrosoft365CopilotUserCountTrend(period='D30')?$format=application/json
Authorization: Bearer {access_token}
```

**Response** (200 OK, JSON):
```json
{
  "value": [
    {
      "reportRefreshDate": "2026-02-22",
      "reportDate": "2026-02-22",
      "anyAppEnabledUserCount": 500,
      "anyAppActiveUserCount": 350,
      "teamsEnabledUserCount": 480,
      "teamsActiveUserCount": 300,
      "wordEnabledUserCount": 490,
      "wordActiveUserCount": 200,
      "excelEnabledUserCount": 490,
      "excelActiveUserCount": 150
    }
  ]
}
```

**Required Permission**: `Reports.Read.All` (application)

---

### 1.6 Graph Security Audit Log API — Copilot Audit Logs

**Base URL**: `https://graph.microsoft.com/beta/security/auditLog`

**Authentication**: Same `GraphServiceClient` instance with `AuditLogsQuery.Read.All` permission.

**Step 1 — Create Query**:
```http
POST https://graph.microsoft.com/beta/security/auditLog/queries
Content-Type: application/json
Authorization: Bearer {access_token}

{
  "displayName": "CopilotInteraction-Daily",
  "filterStartDateTime": "2026-02-22T00:00:00Z",
  "filterEndDateTime": "2026-02-23T00:00:00Z",
  "recordTypeFilters": ["CopilotInteraction"],
  "operationFilters": ["CopilotInteraction"],
  "serviceFilter": "Copilot"
}
```

**Response** (201 Created):
```json
{
  "@odata.type": "#microsoft.graph.security.auditLogQuery",
  "id": "168ec429-084b-a489-90d8-504a87846305",
  "displayName": "CopilotInteraction-Daily",
  "filterStartDateTime": "2026-02-22T00:00:00Z",
  "filterEndDateTime": "2026-02-23T00:00:00Z",
  "recordTypeFilters": ["CopilotInteraction"],
  "status": "notStarted"
}
```

**Step 2 — Poll for Completion**:
```http
GET https://graph.microsoft.com/beta/security/auditLog/queries/{queryId}
Authorization: Bearer {access_token}
```
Poll until `status` changes to `"succeeded"`.

**Step 3 — Retrieve Records**:
```http
GET https://graph.microsoft.com/beta/security/auditLog/queries/{queryId}/records
Authorization: Bearer {access_token}
```

**Audit Log Record Schema** (`auditLogRecord`):
```json
{
  "@odata.type": "#microsoft.graph.security.auditLogRecord",
  "id": "40706737-7eca-f9a1-97a5-dedd3260e24a",
  "createdDateTime": "2026-02-22T10:30:00Z",
  "auditLogRecordType": "CopilotInteraction",
  "operation": "CopilotInteraction",
  "userId": "user@contoso.com",
  "userPrincipalName": "user@contoso.com",
  "service": "Copilot",
  "clientIp": "10.0.0.1",
  "auditData": {
    "AppHost": "Teams",
    "AccessedResources": [
      {
        "ResourceType": "File",
        "FileName": "string",
        "SiteUrl": "string"
      }
    ]
  }
}
```

**Pagination**: Use `@odata.nextLink` for paginated results.

**Filtering**: Use `recordTypeFilters: ["CopilotInteraction"]` and `serviceFilter: "Copilot"` in the POST query body.

**Retention**: 180 days (Audit Standard); up to 1 year (Audit Premium).

---

### 1.7 Microsoft Graph API — M365 Active User Detail

**Endpoint**: `GET https://graph.microsoft.com/v1.0/reports/getOffice365ActiveUserDetail(period='{period}')`

**Request**:
```http
GET /v1.0/reports/getOffice365ActiveUserDetail(period='D30')?$format=application/json
Authorization: Bearer {access_token}
```

**Response** (200 OK, JSON):
```json
{
  "value": [
    {
      "reportRefreshDate": "2026-02-22",
      "userPrincipalName": "string",
      "displayName": "string",
      "exchangeLastActivityDate": "2026-02-20 | null",
      "oneDriveLastActivityDate": "2026-02-18 | null",
      "sharePointLastActivityDate": "2026-02-15 | null",
      "teamsLastActivityDate": "2026-02-20 | null",
      "wordLastActivityDate": "2026-02-19 | null",
      "excelLastActivityDate": "null",
      "powerPointLastActivityDate": "null",
      "reportPeriod": "30"
    }
  ],
  "@odata.nextLink": "..."
}
```

**Available Periods**: `D7`, `D30`, `D90`, `D180`

**Required Permission**: `Reports.Read.All` (application) — already granted

**Purpose**: Provides per-user M365 productivity activity data used to compute `m365_activity_score` for license assignment candidate ranking (FR-007, US4).

---

## 2. Exposed Interfaces

### 2.1 Power BI Semantic Model

The solution exposes a Power BI Semantic Model (star schema) for report consumption.

**Model Name**: `Copilot License Analytics`

**Tables**:

| Table | Type | Grain | Source |
|-------|------|-------|--------|
| fact_user_usage | Fact | One row per user per date | gold_user_license_usage + gold_daily_metrics |
| dim_user | Dimension | One row per user | gold_user_license_usage |
| dim_date | Dimension | One row per calendar date | Generated date table |
| dim_department | Dimension | One row per department | gold_department_summary |
| dim_license | Dimension | One row per license SKU | silver_licenses (distinct SKUs) |

**Relationships** (all uni-directional, single):

| From | To | Cardinality | Key |
|------|----|-------------|-----|
| dim_user.UserKey | fact_user_usage.UserKey | 1:* | Surrogate |
| dim_date.DateKey | fact_user_usage.DateKey | 1:* | YYYYMMDD |
| dim_department.DepartmentKey | fact_user_usage.DepartmentKey | 1:* | Surrogate |
| dim_license.LicenseKey | fact_user_usage.LicenseKey | 1:* | Surrogate |

**Key DAX Measures**:

```dax
// Utilization Rate
Utilization Rate =
DIVIDE(
    CALCULATE(COUNTROWS(fact_user_usage), fact_user_usage[UsageCategory] = "Active"),
    CALCULATE(COUNTROWS(fact_user_usage), dim_user[HasCopilotLicense] = TRUE()),
    0
)

// Estimated Monthly Savings
Est Monthly Savings =
CALCULATE(
    COUNTROWS(fact_user_usage),
    fact_user_usage[UsageCategory] = "Inactive",
    dim_user[HasCopilotLicense] = TRUE()
) * [License Cost Per Month]

// Active Users
Active Users = CALCULATE(COUNTROWS(fact_user_usage), fact_user_usage[UsageCategory] = "Active")

// Average Usage Score
Avg Usage Score = AVERAGE(fact_user_usage[UsageScore])
```

**Report Pages**:

| Page | Primary Visuals | Filters |
|------|-----------------|---------|
| Executive Dashboard | KPI cards, utilization gauge, trend line chart | Date range |
| User Details | Table (conditional formatting on usage score) | Department, Location, Usage Category, Days Inactive |
| Department Analysis | Matrix with drillthrough | Department, Location |
| Recommendations | List visual with export button | Recommendation type |

### 2.2 Pipeline Notification Contract

The daily pipeline sends an email notification on completion.

**Trigger**: Pipeline completion (success or failure)

**Notification payload**:
```
Subject: [Copilot Analytics] Daily Refresh - {status} - {date}

Body:
- Pipeline Status: {SUCCESS|PARTIAL|FAILED}
- Records Processed: {total_records}
- Active Users: {active_count}
- Utilization Rate: {rate}%
- Errors: {error_count}
- Dashboard Link: {powerbi_url}
```

### 2.3 CSV Export Contract

Exported CSV files follow this schema for interoperability.

**Underutilized Licenses Export**:
```csv
UserPrincipalName,DisplayName,Email,Department,Location,LicenseAssignedDate,LastActivityDate,DaysSinceLastActivity,UsageScore,Recommendation
```

**License Candidates Export**:
```csv
UserPrincipalName,DisplayName,Email,Department,Location,M365ActivityScore,RecommendedPriority
```
