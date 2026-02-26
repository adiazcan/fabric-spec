# API Contract: Microsoft Graph — Purview Audit Search (Beta)

**Used by**: `03_ingest_audit_logs.Notebook`
**Authentication**: Service Principal (application permissions)
**Permission**: `AuditLogsQuery.Read.All`
**API Status**: Beta — subject to schema changes without notice

## SDK Usage

```python
from azure.identity.aio import ClientSecretCredential
from msgraph_beta import GraphServiceClient
from msgraph_beta.generated.models.security import AuditLogQuery

credential = ClientSecretCredential(tenant_id, client_id, client_secret)
client = GraphServiceClient(credential, scopes=['https://graph.microsoft.com/.default'])

# Step 1: Create query
query_body = AuditLogQuery(
    display_name="CopilotAuditIngestion",
    filter_start_date_time=watermark_dt,
    filter_end_date_time=now_dt,
    service_filter="MicrosoftCopilot",
)
query = await client.security.audit_log.queries.post(query_body)

# Step 2: Poll until succeeded
import asyncio
while True:
    q = await client.security.audit_log.queries.by_audit_log_query_id(query.id).get()
    if q.status.value == "succeeded":
        break
    await asyncio.sleep(poll_interval)  # exponential backoff recommended

# Step 3: Retrieve records
records_response = await client.security.audit_log.queries.by_audit_log_query_id(query.id).records.get()
```

**Package**: `msgraph-beta-sdk` v1.56.0 (beta endpoint — not available in v1.0 SDK)

## Workflow: 3-Step Async Query Pattern

### Step 1: Create Audit Log Query

```
POST /beta/security/auditLog/queries
```

#### Request Body

```json
{
  "@odata.type": "#microsoft.graph.security.auditLogQuery",
  "displayName": "CopilotAuditIngestion",
  "filterStartDateTime": "2026-02-24T00:00:00Z",
  "filterEndDateTime": "2026-02-25T06:00:00Z",
  "serviceFilter": "MicrosoftCopilot",
  "recordTypeFilters": [],
  "operationFilters": [],
  "userPrincipalNameFilters": [],
  "ipAddressFilters": [],
  "objectIdFilters": [],
  "administrativeUnitIdFilters": []
}
```

#### Query Filter Parameters

| Parameter | Type | Purpose |
|-----------|------|---------|
| `displayName` | `String` | Human-readable query name |
| `filterStartDateTime` | `DateTimeOffset` | Start of date range (watermark) |
| `filterEndDateTime` | `DateTimeOffset` | End of date range (pipeline run time) |
| `serviceFilter` | `String` | Microsoft service (e.g., `MicrosoftCopilot`) |
| `recordTypeFilters` | `String[]` | Audit record types (e.g., `copilotInteraction`) |
| `operationFilters` | `String[]` | Operation names to filter |
| `userPrincipalNameFilters` | `String[]` | UPNs to filter |
| `keywordFilter` | `String` | Free-text search on non-indexed properties |
| `ipAddressFilters` | `String[]` | IP addresses to filter |
| `objectIdFilters` | `String[]` | Target object IDs to filter |
| `administrativeUnitIdFilters` | `String[]` | Admin unit IDs to filter |

#### Response: `201 Created`

```json
{
  "@odata.type": "#microsoft.graph.security.auditLogQuery",
  "id": "168ec429-084b-a489-90d8-504a87846305",
  "displayName": "CopilotAuditIngestion",
  "status": "notStarted",
  "filterStartDateTime": "2026-02-24T00:00:00Z",
  "filterEndDateTime": "2026-02-25T06:00:00Z",
  "serviceFilter": "MicrosoftCopilot"
}
```

### Step 2: Poll Query Status

```
GET /beta/security/auditLog/queries/{id}
```

Poll until `status` transitions to `succeeded`. Possible values: `notStarted`, `running`, `succeeded`, `failed`, `cancelled`.

**Polling strategy**: Exponential backoff starting at 5 seconds, max 60 seconds. Timeout after 10 minutes.

### Step 3: Retrieve Records

```
GET /beta/security/auditLog/queries/{id}/records
```

#### Response Schema (`auditLogRecord`)

```json
{
  "@odata.type": "#microsoft.graph.security.auditLogRecord",
  "id": "string (GUID)",
  "createdDateTime": "2026-02-24T14:30:00Z",
  "auditLogRecordType": "string",
  "operation": "string",
  "organizationId": "string (GUID)",
  "service": "string",
  "userId": "string",
  "userPrincipalName": "user@contoso.com",
  "userType": "string",
  "clientIp": "string",
  "objectId": "string | null",
  "administrativeUnits": ["string"],
  "auditData": {
    "@odata.type": "microsoft.graph.security.auditData"
  }
}
```

## Field Mapping to Silver

| API Field | Silver Column | Transform |
|-----------|--------------|-----------|
| `id` | `event_id` | Direct |
| `createdDateTime` | `created_date_time` | Cast to TIMESTAMP |
| `operation` | `operation` | Direct |
| `service` | `service` | Direct |
| `auditLogRecordType` | `record_type` | Direct |
| `userPrincipalName` | `user_principal_name` | Direct |
| `userId` | `user_id` | Direct |
| `userType` | `user_type` | Direct |
| `clientIp` | `client_ip` | Direct |
| `objectId` | `object_id` | Null-safe |
| `organizationId` | `organization_id` | Direct |
| `auditData` | `audit_data_json` | Serialize to JSON string |

## Incremental Loading Strategy

- **Watermark column**: `filterStartDateTime` (query creation parameter)
- **Logic**: Store `max(filterEndDateTime)` of last successful query in `sys_watermarks`. Next run sets `filterStartDateTime = watermark`.
- **Server-side filtering**: Date range is applied at query creation time — only matching records are returned.
- **Ordered by**: `createdDateTime` (server-side ordering within query results).

## Pagination

Records endpoint includes `@odata.nextLink` for large result sets:

```json
{
  "@odata.nextLink": "https://graph.microsoft.com/beta/security/auditLog/queries/{id}/records?$skiptoken=...",
  "value": [ ... ]
}
```

## Volume Considerations

- Large tenants may generate millions of audit events per day.
- Narrow date ranges and `serviceFilter` are essential to keep query processing time reasonable.
- The async pattern means query execution may take minutes for large result sets.
- Microsoft retains audit logs for 90 days (E3/G3) or 180 days (E5/G5). Our Lakehouse provides indefinite retention.

## Permissions

| Permission | Type | Description |
|-----------|------|-------------|
| `AuditLogsQuery.Read.All` | Application | All audit logs across all workloads |
| `AuditLogsQuery-Entra.Read.All` | Application | Entra ID audit logs only |
| `AuditLogsQuery-Exchange.Read.All` | Application | Exchange audit logs only |
| `AuditLogsQuery-SharePoint.Read.All` | Application | SharePoint audit logs only |

**Recommended**: `AuditLogsQuery.Read.All` for comprehensive Copilot audit coverage across all M365 services.

## Rate Limits

- **Per-app limit**: 130 requests per 10 seconds (shared Graph limit)
- **Query creation**: May have additional daily limits for large tenants
- **Throttling**: HTTP 429 with `Retry-After` header

## Error Handling

| HTTP Status | Action |
|-------------|--------|
| 201 | Query created successfully |
| 200 | Query status / records retrieved |
| 401 | Re-authenticate |
| 403 | Verify `AuditLogsQuery.Read.All` permission |
| 429 | Exponential backoff using `Retry-After` header |
| 5xx | Retry with exponential backoff (max 5 retries) |
| Query `status: "failed"` | Log error, retry query creation |
