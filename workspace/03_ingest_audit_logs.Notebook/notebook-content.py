# Fabric notebook source

# METADATA ********************
# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "language_info": {
# META     "name": "python",
# META     "version": "3.11.0"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "00000000-0000-0000-0000-000000000000",
# META       "default_lakehouse_name": "CopilotUsageLakehouse",
# META       "default_lakehouse_workspace_id": "11111111-1111-1111-1111-111111111111"
# META     }
# META   }
# META }

# CELL ********************
# 03_ingest_audit_logs — Bronze layer ingestion: Copilot audit logs
#
# FR-004: Ingest Copilot audit events from Purview Audit Search API (beta)
# Principle I:   Service Principal auth only (no interactive credentials)
# Principle II:  Secrets from Azure Key Vault only
# Principle VI:  Retry with exponential backoff on all API calls
# Principle VII: Audit log entries written to gold_copilot_audit_log
#
# API Contract: specs/001-copilot-license-usage/contracts/graph-audit-logs.md
# 3-Step Async Pattern:
#   1. POST /beta/security/auditLog/queries (create query)
#   2. GET  /beta/security/auditLog/queries/{id} (poll until succeeded)
#   3. GET  /beta/security/auditLog/queries/{id}/records (retrieve + paginate)
#
# Bronze table:  bronze_graph_audit_logs (partitioned by ingestion_date)
# Watermark:     sys_watermarks WHERE source_name = 'graph_audit'
#                (filterStartDateTime ISO datetime string)

# noqa: E402
%run helpers
%run 00_watermarks

# CELL ********************

import asyncio
import json
import random
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import List, Optional, Tuple

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, StringType, StructField, StructType, TimestampType

NOTEBOOK_NAME = "03_ingest_audit_logs"
BRONZE_TABLE = "bronze_graph_audit_logs"
WATERMARK_SOURCE = "graph_audit"

# Polling config for async query (Step 2)
POLL_INITIAL_DELAY_S = 5.0
POLL_MAX_DELAY_S = 60.0
POLL_TIMEOUT_S = 600  # 10 minutes

_BRONZE_SCHEMA = StructType(
    [
        StructField("ingestion_date", DateType(), False),
        StructField("ingestion_run_id", StringType(), False),
        StructField("event_id", StringType(), False),
        StructField("created_date_time", TimestampType(), False),
        StructField("raw_json", StringType(), False),
    ]
)

# CELL ********************


def _serialize_audit_record(record) -> dict:
    """Serialize a msgraph_beta AuditLogRecord SDK object to a plain dict.

    Uses getattr with defaults so missing optional fields become None
    rather than raising AttributeError.
    """

    def _to_str(val) -> Optional[str]:
        return str(val) if val is not None else None

    audit_data = getattr(record, "audit_data", None)
    if audit_data is not None:
        # auditData may be a complex SDK object; serialize via additional_data or str
        if hasattr(audit_data, "additional_data"):
            audit_data_serialized = json.dumps(audit_data.additional_data, default=str)
        else:
            audit_data_serialized = json.dumps(str(audit_data))
    else:
        audit_data_serialized = None

    return {
        "id": getattr(record, "id", None),
        "createdDateTime": _to_str(getattr(record, "created_date_time", None)),
        "auditLogRecordType": _to_str(getattr(record, "audit_log_record_type", None)),
        "operation": getattr(record, "operation", None),
        "organizationId": getattr(record, "organization_id", None),
        "service": getattr(record, "service", None),
        "userId": getattr(record, "user_id", None),
        "userPrincipalName": getattr(record, "user_principal_name", None),
        "userType": _to_str(getattr(record, "user_type", None)),
        "clientIp": getattr(record, "client_ip", None),
        "objectId": getattr(record, "object_id", None),
        "administrativeUnits": [
            str(u) for u in (getattr(record, "administrative_units", None) or [])
        ],
        "auditData": audit_data_serialized,
    }


# CELL ********************


async def _create_audit_query(
    client,
    filter_start: datetime,
    filter_end: datetime,
) -> str:
    """Step 1: Create an audit log query and return its query ID.

    POST /beta/security/auditLog/queries
    """
    from msgraph_beta.generated.models.security.audit_log_query import AuditLogQuery

    query_body = AuditLogQuery(
        display_name="CopilotAuditIngestion",
        filter_start_date_time=filter_start,
        filter_end_date_time=filter_end,
        service_filter="MicrosoftCopilot",
    )

    query = await retry_with_backoff(
        client.security.audit_log.queries.post,
        query_body,
    )

    if query is None or query.id is None:
        raise RuntimeError("Audit log query creation returned no query ID")

    return query.id


async def _poll_until_succeeded(client, query_id: str) -> None:
    """Step 2: Poll query status until 'succeeded', 'failed', or timeout.

    Exponential backoff: start at 5s, max 60s, timeout after 10 minutes.
    """
    start_time = datetime.now(timezone.utc)
    delay = POLL_INITIAL_DELAY_S

    while True:
        elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
        if elapsed > POLL_TIMEOUT_S:
            raise TimeoutError(
                f"Audit query {query_id} did not complete within "
                f"{POLL_TIMEOUT_S}s (elapsed: {elapsed:.0f}s)"
            )

        q = await retry_with_backoff(
            client.security.audit_log.queries.by_audit_log_query_id(query_id).get
        )

        status_value = q.status.value if hasattr(q.status, "value") else str(q.status)

        if status_value == "succeeded":
            return
        if status_value in ("failed", "cancelled"):
            raise RuntimeError(
                f"Audit query {query_id} ended with status: {status_value}"
            )

        # Exponential backoff with full jitter
        await asyncio.sleep(random.uniform(0.0, delay))
        delay = min(delay * 2, POLL_MAX_DELAY_S)


async def _retrieve_all_records(client, query_id: str) -> List:
    """Step 3: Retrieve all audit log records, following pagination.

    GET /beta/security/auditLog/queries/{id}/records
    """
    from msgraph_beta.generated.security.audit_log.queries.item.records.records_request_builder import (  # noqa: E501
        RecordsRequestBuilder,
    )

    response = await retry_with_backoff(
        client.security.audit_log.queries.by_audit_log_query_id(query_id).records.get
    )

    all_records: List = []
    while response is not None:
        if getattr(response, "value", None):
            all_records.extend(response.value)

        next_link: Optional[str] = getattr(response, "odata_next_link", None)
        if not next_link:
            break

        next_builder = RecordsRequestBuilder(
            request_adapter=client.request_adapter,
            path_parameters={"request-raw-url": next_link},
        )
        response = await retry_with_backoff(next_builder.get)

    return all_records


# CELL ********************


async def _ingest(spark: SparkSession, run_id: str) -> Tuple[int, Optional[str]]:
    """Authenticate, run the 3-step async audit query, and write to Bronze."""
    client = get_beta_graph_client()
    existing_watermark: Optional[str] = read_watermark(spark, WATERMARK_SOURCE)

    # Determine query time range
    filter_end = datetime.now(timezone.utc)
    if existing_watermark:
        filter_start = datetime.fromisoformat(existing_watermark.replace("Z", "+00:00"))
    else:
        # First run: default to 7 days back to avoid overwhelming large tenants
        filter_start = filter_end - timedelta(days=7)

    filter_end_iso = filter_end.strftime("%Y-%m-%dT%H:%M:%SZ")

    # ── Step 1: Create audit log query ──
    api_start = datetime.now(timezone.utc)
    try:
        query_id = await _create_audit_query(client, filter_start, filter_end)
    except Exception as exc:
        write_audit_entry(
            spark,
            run_id=run_id,
            notebook_name=NOTEBOOK_NAME,
            operation="API_CALL",
            target="POST /beta/security/auditLog/queries",
            status="FAILURE",
            error_message=str(exc),
        )
        raise

    write_audit_entry(
        spark,
        run_id=run_id,
        notebook_name=NOTEBOOK_NAME,
        operation="API_CALL",
        target="POST /beta/security/auditLog/queries",
        status="SUCCESS",
    )

    # ── Step 2: Poll until succeeded ──
    try:
        await _poll_until_succeeded(client, query_id)
    except Exception as exc:
        write_audit_entry(
            spark,
            run_id=run_id,
            notebook_name=NOTEBOOK_NAME,
            operation="API_CALL",
            target=f"GET /beta/security/auditLog/queries/{query_id} (poll)",
            status="FAILURE",
            error_message=str(exc),
        )
        raise

    write_audit_entry(
        spark,
        run_id=run_id,
        notebook_name=NOTEBOOK_NAME,
        operation="API_CALL",
        target=f"GET /beta/security/auditLog/queries/{query_id} (poll)",
        status="SUCCESS",
    )

    # ── Step 3: Retrieve all records ──
    try:
        all_records = await _retrieve_all_records(client, query_id)
    except Exception as exc:
        write_audit_entry(
            spark,
            run_id=run_id,
            notebook_name=NOTEBOOK_NAME,
            operation="API_CALL",
            target=f"GET /beta/security/auditLog/queries/{query_id}/records",
            status="FAILURE",
            error_message=str(exc),
        )
        raise

    api_ms = int((datetime.now(timezone.utc) - api_start).total_seconds() * 1000)
    write_audit_entry(
        spark,
        run_id=run_id,
        notebook_name=NOTEBOOK_NAME,
        operation="API_CALL",
        target=f"GET /beta/security/auditLog/queries/{query_id}/records",
        status="SUCCESS",
        records_affected=len(all_records),
        duration_ms=api_ms,
    )

    if not all_records:
        print(
            f"[{NOTEBOOK_NAME}] No audit records in range "
            f"{filter_start.isoformat()} → {filter_end.isoformat()}. "
            "Skipping Bronze write."
        )
        return 0, filter_end_iso

    # ── Serialize and write to Bronze ──
    today = date.today()
    rows = []
    for r in all_records:
        serialized = _serialize_audit_record(r)
        created_dt = getattr(r, "created_date_time", None)
        if created_dt is None:
            # Parse from serialized fallback
            created_str = serialized.get("createdDateTime")
            if created_str:
                created_dt = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
            else:
                created_dt = datetime.now(timezone.utc)

        rows.append(
            (
                today,
                run_id,
                serialized.get("id") or str(uuid.uuid4()),
                created_dt,
                json.dumps(serialized),
            )
        )

    write_start = datetime.now(timezone.utc)
    (
        spark.createDataFrame(rows, schema=_BRONZE_SCHEMA)
        .write.format("delta")
        .mode("append")
        .partitionBy("ingestion_date")
        .saveAsTable(BRONZE_TABLE)
    )
    write_ms = int((datetime.now(timezone.utc) - write_start).total_seconds() * 1000)

    write_audit_entry(
        spark,
        run_id=run_id,
        notebook_name=NOTEBOOK_NAME,
        operation="TABLE_WRITE",
        target=BRONZE_TABLE,
        status="SUCCESS",
        records_affected=len(rows),
        duration_ms=write_ms,
    )

    print(
        f"[{NOTEBOOK_NAME}] Bronze write complete: {len(rows):,} audit records "
        f"(range: {filter_start.isoformat()} → {filter_end.isoformat()})"
    )

    return len(rows), filter_end_iso


# CELL ********************
# Main execution

spark = SparkSession.builder.getOrCreate()
run_id = generate_run_id()

try:
    record_count, new_watermark = asyncio.run(_ingest(spark, run_id))

    if new_watermark:
        write_watermark(spark, WATERMARK_SOURCE, new_watermark, run_id, record_count)
        write_audit_entry(
            spark,
            run_id=run_id,
            notebook_name=NOTEBOOK_NAME,
            operation="TABLE_WRITE",
            target="sys_watermarks",
            status="SUCCESS",
            records_affected=1,
        )

    print(
        f"[{NOTEBOOK_NAME}] Ingested {record_count:,} audit records | Run ID: {run_id}"
    )

except Exception as e:
    write_audit_entry(
        spark,
        run_id=run_id,
        notebook_name=NOTEBOOK_NAME,
        operation="ERROR",
        target=BRONZE_TABLE,
        status="FAILURE",
        error_message=str(e),
    )
    raise
