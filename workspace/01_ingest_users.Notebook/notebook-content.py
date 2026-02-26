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
# 01_ingest_users — Bronze layer ingestion: user profiles + Copilot license details
#
# FR-001: Ingest all user accounts with their Copilot license assignment status
# FR-002: Incremental loading via Graph API delta query with deltaLink watermark
# Principle I:   Service Principal auth only (no interactive credentials)
# Principle II:  Secrets from Azure Key Vault only
# Principle VI:  Retry with exponential backoff on all API calls
# Principle VII: Audit log entries written to gold_copilot_audit_log
#
# API Contract: specs/001-copilot-license-usage/contracts/graph-users.md
# Bronze table:  bronze_graph_users (partitioned by ingestion_date)
# Watermark:    sys_watermarks WHERE source_name = 'graph_users' (deltaLink URL)

# noqa: E402
%run helpers
%run 00_watermarks

# CELL ********************

import asyncio
import json
from datetime import date, datetime, timezone
from typing import List, Optional, Tuple

import pyspark.sql.functions as F
from msgraph.generated.users.delta.delta_request_builder import DeltaRequestBuilder
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, StringType, StructField, StructType

NOTEBOOK_NAME = "01_ingest_users"
BRONZE_TABLE = "bronze_graph_users"
WATERMARK_SOURCE = "graph_users"

_GRAPH_SELECT_FIELDS = [
    "id",
    "displayName",
    "mail",
    "userPrincipalName",
    "department",
    "jobTitle",
    "accountEnabled",
    "companyName",
    "officeLocation",
]

_BRONZE_SCHEMA = StructType(
    [
        StructField("ingestion_date", DateType(), False),
        StructField("ingestion_run_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("raw_json", StringType(), False),
    ]
)

# CELL ********************


def _serialize_user(user) -> dict:
    """Serialize a msgraph User SDK object to a plain dict for raw_json storage.

    Uses getattr with defaults to gracefully handle both fully-populated user
    objects and sparse delta-removed objects (which only carry `id` and
    `@odata.removed` metadata).
    """
    return {
        "id": user.id,
        "displayName": getattr(user, "display_name", None),
        "mail": getattr(user, "mail", None),
        "userPrincipalName": getattr(user, "user_principal_name", None),
        "department": getattr(user, "department", None),
        "jobTitle": getattr(user, "job_title", None),
        "accountEnabled": getattr(user, "account_enabled", None),
        "companyName": getattr(user, "company_name", None),
        "officeLocation": getattr(user, "office_location", None),
        "licenseDetails": [
            {
                "id": ld.id,
                "skuId": str(ld.sku_id) if getattr(ld, "sku_id", None) else None,
                "skuPartNumber": getattr(ld, "sku_part_number", None),
            }
            for ld in (getattr(user, "license_details", None) or [])
        ],
    }


async def _fetch_all_users(
    client,
    watermark: Optional[str],
) -> Tuple[List, Optional[str]]:
    """Fetch all users from Graph API via the delta endpoint.

    On first run (watermark is None): executes a full delta query using
    /v1.0/users/delta with $select, $expand=licenseDetails, $top=999.

    On subsequent runs: resumes from the stored deltaLink URL, which returns
    only records changed since the prior run.

    Pagination: follows @odata.nextLink until exhausted. The final page
    contains @odata.deltaLink — stored as the watermark for the next run.

    Returns:
        (all_user_objects, new_delta_link_url)
    """
    if watermark:
        # Incremental sync — resume from stored deltaLink URL using raw-url Kiota pattern
        builder = DeltaRequestBuilder(
            request_adapter=client.request_adapter,
            path_parameters={"request-raw-url": watermark},
        )
        response = await retry_with_backoff(builder.get)
    else:
        # Full sync — start fresh delta query with all required fields + licenseDetails
        params = DeltaRequestBuilder.DeltaRequestBuilderGetQueryParameters(
            select=_GRAPH_SELECT_FIELDS,
            expand=["licenseDetails"],
            top=999,
        )
        config = DeltaRequestBuilder.DeltaRequestBuilderGetRequestConfiguration(
            query_parameters=params
        )
        response = await retry_with_backoff(
            client.users.delta.get, request_configuration=config
        )

    all_users: List = []
    new_delta_link: Optional[str] = None

    while response is not None:
        if getattr(response, "value", None):
            all_users.extend(response.value)

        next_link: Optional[str] = getattr(response, "odata_next_link", None)
        if next_link:
            # Follow pagination using the raw-url Kiota pattern
            next_builder = DeltaRequestBuilder(
                request_adapter=client.request_adapter,
                path_parameters={"request-raw-url": next_link},
            )
            response = await retry_with_backoff(next_builder.get)
        else:
            # Last page — capture deltaLink for next incremental run
            new_delta_link = getattr(response, "odata_delta_link", None)
            break

    return all_users, new_delta_link


async def _ingest(spark: SparkSession, run_id: str) -> Tuple[int, Optional[str]]:
    """Authenticate, fetch users from Graph API, and write to bronze_graph_users."""
    client = get_graph_client()
    existing_watermark = read_watermark(spark, WATERMARK_SOURCE)

    api_start = datetime.now(timezone.utc)
    try:
        all_users, new_delta_link = await _fetch_all_users(client, existing_watermark)
    except Exception as exc:
        write_audit_entry(
            spark,
            run_id=run_id,
            notebook_name=NOTEBOOK_NAME,
            operation="API_CALL",
            target="/v1.0/users/delta",
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
        target="/v1.0/users/delta",
        status="SUCCESS",
        records_affected=len(all_users),
        duration_ms=api_ms,
    )

    if not all_users:
        print(f"[{NOTEBOOK_NAME}] No user records returned (incremental run, no changes).")
        return 0, new_delta_link

    ingestion_date = date.today()
    rows = [
        (ingestion_date, run_id, user.id, json.dumps(_serialize_user(user)))
        for user in all_users
        if user.id is not None  # skip any malformed objects without an id
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

    return len(rows), new_delta_link


# CELL ********************
# Main execution

spark = SparkSession.builder.getOrCreate()
run_id = generate_run_id()

try:
    record_count, new_delta_link = asyncio.run(_ingest(spark, run_id))

    if new_delta_link:
        write_watermark(spark, WATERMARK_SOURCE, new_delta_link, run_id, record_count)
        write_audit_entry(
            spark,
            run_id=run_id,
            notebook_name=NOTEBOOK_NAME,
            operation="TABLE_WRITE",
            target="sys_watermarks",
            status="SUCCESS",
            records_affected=1,
        )

    print(f"[{NOTEBOOK_NAME}] Ingested {record_count:,} user records | Run ID: {run_id}")

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
