# API Contract: Microsoft Graph — Copilot Usage Reports (Beta)

**Used by**: `02_ingest_usage.Notebook`
**Authentication**: Service Principal (application permissions)
**Permission**: `Reports.Read.All`
**API Status**: Beta — subject to schema changes without notice

## SDK Usage

```python
from azure.identity.aio import ClientSecretCredential
from msgraph_beta import GraphServiceClient

credential = ClientSecretCredential(tenant_id, client_id, client_secret)
client = GraphServiceClient(credential, scopes=['https://graph.microsoft.com/.default'])

response = await client.reports.get_microsoft365_copilot_usage_user_detail_with_period('D7').get()
```

**Package**: `msgraph-beta-sdk` v1.56.0 (beta endpoint — not available in v1.0 SDK)

## Endpoint

```
GET /beta/reports/getMicrosoft365CopilotUsageUserDetail(period='{period}')
```

## Request Parameters

| Parameter | Value | Purpose |
|-----------|-------|---------|
| `period` | `D7` (daily runs), `D30` (backfill) | Reporting period |
| `$top` | `999` | Page size |

## Request

```http
GET https://graph.microsoft.com/beta/reports/getMicrosoft365CopilotUsageUserDetail(period='D7')
Authorization: Bearer {access_token}
Accept: application/json
```

**Note**: Default response is CSV. Use `Accept: application/json` header for JSON.

## Response Schema (per user-report row)

```json
{
  "reportRefreshDate": "2026-02-24",
  "reportPeriod": 7,
  "userPrincipalName": "user@contoso.com",
  "displayName": "John Doe",
  "lastActivityDate": "2026-02-23",
  "microsoftTeamsCopilotLastActivityDate": "2026-02-23",
  "wordCopilotLastActivityDate": "2026-02-20",
  "excelCopilotLastActivityDate": null,
  "powerPointCopilotLastActivityDate": "2026-02-18",
  "outlookCopilotLastActivityDate": "2026-02-22",
  "oneNoteCopilotLastActivityDate": null,
  "loopCopilotLastActivityDate": null,
  "copilotChatLastActivityDate": "2026-02-23"
}
```

## Field Mapping to Silver

| API Field | Silver Column | Transform |
|-----------|--------------|-----------|
| `reportRefreshDate` | `report_refresh_date` | Cast to DATE |
| `reportPeriod` | `report_period` | Prefix with `D` → `D7` |
| `userPrincipalName` | `user_principal_name` | Direct |
| `lastActivityDate` | `last_activity_date` | Cast to DATE, null if empty |
| `microsoftTeamsCopilotLastActivityDate` | `teams_last_activity_date` | Cast to DATE, null if empty |
| `wordCopilotLastActivityDate` | `word_last_activity_date` | Cast to DATE, null if empty |
| `excelCopilotLastActivityDate` | `excel_last_activity_date` | Cast to DATE, null if empty |
| `powerPointCopilotLastActivityDate` | `powerpoint_last_activity_date` | Cast to DATE, null if empty |
| `outlookCopilotLastActivityDate` | `outlook_last_activity_date` | Cast to DATE, null if empty |
| `oneNoteCopilotLastActivityDate` | `onenote_last_activity_date` | Cast to DATE, null if empty |
| `loopCopilotLastActivityDate` | `loop_last_activity_date` | Cast to DATE, null if empty |
| `copilotChatLastActivityDate` | `copilot_chat_last_activity_date` | Cast to DATE, null if empty |

## Incremental Loading Strategy

- **Watermark column**: `reportRefreshDate`
- **Logic**: Store `max(reportRefreshDate)` in `sys_watermarks`. Next run skips records where `reportRefreshDate <= watermark`.
- **Note**: The API itself does not support date filtering. The full report is returned; filtering is applied in the notebook after retrieval.

## Pagination

Response includes `@odata.nextLink` for large organizations:

```json
{
  "@odata.nextLink": "https://graph.microsoft.com/beta/reports/...?$skiptoken=...",
  "value": [ ... ]
}
```

## Data Latency

Microsoft reports a **24-48 hour delay** before usage data appears. The `reportRefreshDate` indicates the actual data currency, not the request date.

## Derived Metrics for Scoring

From the per-app last-activity columns, the scoring notebook computes:

- **Recency**: `days_since_last_activity = DATEDIFF(current_date, last_activity_date)`
- **Breadth**: `apps_used_count = COUNT(non-null per-app columns)`
- **Active days**: Approximated from cross-period analysis (D7 vs previous D7 reports)

## Rate Limits

- **Per-app limit**: 130 requests per 10 seconds (shared with other Graph endpoints)
- **Report-specific**: Reports endpoints may have additional daily limits for large tenants
- **Throttling**: HTTP 429 with `Retry-After` header

## Error Handling

| HTTP Status | Action |
|-------------|--------|
| 200 | Process response |
| 401 | Re-authenticate |
| 403 | Verify `Reports.Read.All` permission |
| 429 | Exponential backoff using `Retry-After` header |
| 5xx | Retry with exponential backoff (max 5 retries) |
