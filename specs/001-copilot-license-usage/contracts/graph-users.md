# API Contract: Microsoft Graph — User Profiles & License Details

**Used by**: `01_ingest_users.Notebook`
**Authentication**: Service Principal (application permissions)
**Permission**: `User.Read.All`

## SDK Usage

```python
from azure.identity.aio import ClientSecretCredential
from msgraph import GraphServiceClient
from msgraph.generated.users.users_request_builder import UsersRequestBuilder

credential = ClientSecretCredential(tenant_id, client_id, client_secret)
client = GraphServiceClient(credential, scopes=['https://graph.microsoft.com/.default'])

# Initial sync
query = UsersRequestBuilder.UsersRequestBuilderGetQueryParameters(
    select=['id','displayName','mail','userPrincipalName','department','jobTitle','accountEnabled','companyName','officeLocation'],
    expand=['licenseDetails'],
    top=999
)
config = UsersRequestBuilder.UsersRequestBuilderGetRequestConfiguration(query_parameters=query)
response = await client.users.get(request_configuration=config)

# Delta sync
delta_response = await client.users.delta.get()
```

**Package**: `msgraph-sdk` v1.55.0 (v1.0 endpoint)

## Endpoint

```
GET /v1.0/users
```

## Request Parameters

| Parameter | Value | Purpose |
|-----------|-------|---------|
| `$select` | `id,displayName,mail,userPrincipalName,department,jobTitle,accountEnabled,companyName,officeLocation` | Limit returned fields |
| `$expand` | `licenseDetails` | Include license assignments inline |
| `$top` | `999` | Maximum page size |
| `$deltaLink` | `{stored_delta_token}` | Incremental sync (after first full sync) |

## Initial Sync Request

```http
GET https://graph.microsoft.com/v1.0/users
  ?$select=id,displayName,mail,userPrincipalName,department,jobTitle,accountEnabled,companyName,officeLocation
  &$expand=licenseDetails
  &$top=999
Authorization: Bearer {access_token}
Accept: application/json
```

## Incremental Sync Request

```http
GET {deltaLink}
Authorization: Bearer {access_token}
Accept: application/json
```

## Response Schema (per user object)

```json
{
  "id": "string (GUID)",
  "displayName": "string",
  "mail": "string | null",
  "userPrincipalName": "string",
  "department": "string | null",
  "jobTitle": "string | null",
  "accountEnabled": "boolean",
  "companyName": "string | null",
  "officeLocation": "string | null",
  "licenseDetails": [
    {
      "id": "string (GUID)",
      "skuId": "string (GUID)",
      "skuPartNumber": "string",
      "servicePlans": [
        {
          "servicePlanId": "string (GUID)",
          "servicePlanName": "string",
          "provisioningStatus": "string",
          "appliesTo": "string"
        }
      ]
    }
  ]
}
```

## Pagination

Response includes `@odata.nextLink` when more pages exist. Follow the link until absent.

```json
{
  "@odata.nextLink": "https://graph.microsoft.com/v1.0/users?$skiptoken=...",
  "value": [ ... ]
}
```

## Delta Response

After consuming all pages, the final response includes `@odata.deltaLink` for the next incremental sync. Store this token in `sys_watermarks`.

```json
{
  "@odata.deltaLink": "https://graph.microsoft.com/v1.0/users/delta?$deltatoken=...",
  "value": [ ... ]
}
```

## Copilot SKU Identification

Filter `licenseDetails` for Copilot-enabled licenses:

| SKU Part Number | Description |
|----------------|-------------|
| `MICROSOFT_365_COPILOT` | Microsoft 365 Copilot add-on |
| `Microsoft_365_Copilot` | Alternate casing (check case-insensitive) |

## Rate Limits

- **Per-app limit**: 130 requests per 10 seconds
- **Throttling**: HTTP 429 with `Retry-After` header
- **Expected volume**: ~50 pages for 50K users at `$top=999`

## Error Handling

| HTTP Status | Action |
|-------------|--------|
| 200 | Process response |
| 401 | Re-authenticate; token may be expired |
| 403 | Permission denied; verify SP permissions |
| 429 | Exponential backoff using `Retry-After` header |
| 5xx | Retry with exponential backoff (max 5 retries) |
