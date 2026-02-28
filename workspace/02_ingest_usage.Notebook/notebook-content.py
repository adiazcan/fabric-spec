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
# 02_ingest_usage — Bronze layer ingestion: per-user Copilot usage reports
#
# FR-003: Ingest per-user Copilot usage metrics from Microsoft Graph beta API
# Principle I:   Service Principal auth only (no interactive credentials)
# Principle II:  Secrets from Azure Key Vault only
# Principle VI:  Retry with exponential backoff on all API calls
# Principle VII: Audit log entries written to gold_copilot_audit_log
#
# API Contract: specs/001-copilot-license-usage/contracts/graph-usage-reports.md
# Bronze table:  bronze_graph_usage_reports (partitioned by ingestion_date)
# Watermark:    sys_watermarks WHERE source_name = 'graph_usage' (max reportRefreshDate)

# noqa: E402
%run helpers
%run 00_watermarks

# CELL ********************

import asyncio
import json
from datetime import date, datetime, timezone
from typing import List, Optional, Tuple

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, StringType, StructField, StructType

NOTEBOOK_NAME = "02_ingest_usage"
BRONZE_TABLE = "bronze_graph_usage_reports"
WATERMARK_SOURCE = "graph_usage"

_BRONZE_SCHEMA = StructType(
    [
        StructField("ingestion_date", DateType(), False),
        StructField("ingestion_run_id", StringType(), False),
        StructField("report_refresh_date", StringType(), False),
        StructField("user_principal_name", StringType(), True),
        StructField("raw_json", StringType(), False),
    ]
)

# CELL ********************


def _serialize_usage_record(record) -> dict:
    """Serialize a msgraph_beta Copilot usage report SDK object to a plain dict.

    Uses getattr with defaults so missing optional date fields become None
    rather than raising AttributeError.
    """

    def _date_str(val) -> Optional[str]:
        """Convert a date/datetime SDK value to ISO-8601 string, or None."""
        return str(val) if val is not None else None

    return {
        "reportRefreshDate": _date_str(getattr(record, "report_refresh_date", None)),
        "reportPeriod": getattr(record, "report_period", None),
        "userPrincipalName": getattr(record, "user_principal_name", None),
        "displayName": getattr(record, "display_name", None),
        "lastActivityDate": _date_str(getattr(record, "last_activity_date", None)),
        "microsoftTeamsCopilotLastActivityDate": _date_str(
            getattr(record, "microsoft_teams_copilot_last_activity_date", None)
        ),
        "wordCopilotLastActivityDate": _date_str(
            getattr(record, "word_copilot_last_activity_date", None)
        ),
        "excelCopilotLastActivityDate": _date_str(
            getattr(record, "excel_copilot_last_activity_date", None)
        ),
        "powerPointCopilotLastActivityDate": _date_str(
            getattr(record, "power_point_copilot_last_activity_date", None)
        ),
        "outlookCopilotLastActivityDate": _date_str(
            getattr(record, "outlook_copilot_last_activity_date", None)
        ),
        "oneNoteCopilotLastActivityDate": _date_str(
            getattr(record, "one_note_copilot_last_activity_date", None)
        ),
        "loopCopilotLastActivityDate": _date_str(
            getattr(record, "loop_copilot_last_activity_date", None)
        ),
        "copilotChatLastActivityDate": _date_str(
            getattr(record, "copilot_chat_last_activity_date", None)
        ),
    }


async def _fetch_all_usage_records(client, period: str) -> List:
    """Fetch all per-user Copilot usage records from Graph beta API.

    The usage reports endpoint returns the full report for all users —
    pagination is handled by following @odata.nextLink until exhausted.
    Incremental filtering (by reportRefreshDate watermark) is applied
    in the caller after all pages are collected.

    Args:
        client: Authenticated BetaGraphServiceClient.
        period: Reporting period string — 'D7', 'D30', 'D90', or 'D180'.
    """
    from msgraph_beta.generated.reports.get_microsoft365_copilot_usage_user_detail_with_period.get_microsoft365_copilot_usage_user_detail_with_period_request_builder import (  # noqa: E501
        GetMicrosoft365CopilotUsageUserDetailWithPeriodRequestBuilder as UsageBuilder,
    )

    # Request configuration: force JSON response (default may return CSV)
    req_config = UsageBuilder.GetMicrosoft365CopilotUsageUserDetailWithPeriodRequestBuilderGetRequestConfiguration()
    req_config.headers.add("Accept", "application/json")

    # Initial request via SDK convenience method
    response = await retry_with_backoff(
        client.reports.get_microsoft365_copilot_usage_user_detail_with_period(
            period
        ).get,
        request_configuration=req_config,
    )

    all_records: List = []
    while response is not None:
        if getattr(response, "value", None):
            all_records.extend(response.value)

        next_link: Optional[str] = getattr(response, "odata_next_link", None)
        if not next_link:
            break

        # Follow next page via raw-URL builder (Kiota "request-raw-url" pattern)
        next_builder = UsageBuilder(
            request_adapter=client.request_adapter,
            path_parameters={"request-raw-url": next_link},
        )
        response = await retry_with_backoff(next_builder.get, request_configuration=req_config)

    return all_records


async def _ingest(spark: SparkSession, run_id: str) -> Tuple[int, Optional[str]]:
    """Authenticate, fetch usage records from Graph beta API, and write to Bronze."""
    params = load_parameters()
    period: str = params["usage_report_period"]  # "D7" by default

    client = get_beta_graph_client()
    existing_watermark: Optional[str] = read_watermark(spark, WATERMARK_SOURCE)

    api_start = datetime.now(timezone.utc)
    try:
        all_records = await _fetch_all_usage_records(client, period)
    except Exception as exc:
        write_audit_entry(
            spark,
            run_id=run_id,
            notebook_name=NOTEBOOK_NAME,
            operation="API_CALL",
            target=f"/beta/reports/getMicrosoft365CopilotUsageUserDetail(period='{period}')",
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
        target=f"/beta/reports/getMicrosoft365CopilotUsageUserDetail(period='{period}')",
        status="SUCCESS",
        records_affected=len(all_records),
        duration_ms=api_ms,
    )

    # Apply watermark filter: skip records whose reportRefreshDate was already ingested.
    # The API always returns the full report; incremental logic is applied client-side.
    if existing_watermark:
        all_records = [
            r
            for r in all_records
            if str(getattr(r, "report_refresh_date", "") or "") > existing_watermark
        ]

    if not all_records:
        print(
            f"[{NOTEBOOK_NAME}] No new usage records since watermark={existing_watermark!r}. "
            "Skipping Bronze write."
        )
        return 0, existing_watermark

    # Determine max reportRefreshDate for watermark update
    max_refresh_date: Optional[str] = max(
        (str(getattr(r, "report_refresh_date", "") or "") for r in all_records),
        default=None,
    )

    # Serialize and write to Bronze Delta table (partitioned by ingestion_date)
    today = date.today()
    rows = [
        (
            today,
            run_id,
            str(getattr(r, "report_refresh_date", "") or ""),
            getattr(r, "user_principal_name", None),
            json.dumps(_serialize_usage_record(r)),
        )
        for r in all_records
    ]

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
        f"[{NOTEBOOK_NAME}] Bronze write complete: {len(rows):,} records "
        f"(reportRefreshDate max: {max_refresh_date})"
    )

    return len(rows), max_refresh_date


# CELL ********************
# Main execution

spark = SparkSession.builder.getOrCreate()
run_id = generate_run_id()

try:
    record_count, max_refresh_date = asyncio.run(_ingest(spark, run_id))

    if max_refresh_date:
        write_watermark(spark, WATERMARK_SOURCE, max_refresh_date, run_id, record_count)
        write_audit_entry(
            spark,
            run_id=run_id,
            notebook_name=NOTEBOOK_NAME,
            operation="TABLE_WRITE",
            target="sys_watermarks",
            status="SUCCESS",
            records_affected=1,
        )

    print(f"[{NOTEBOOK_NAME}] Ingested {record_count:,} usage records | Run ID: {run_id}")

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
