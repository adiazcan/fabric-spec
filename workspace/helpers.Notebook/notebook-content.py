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
# helpers — Shared utilities for all CopilotUsage notebooks
#
# Principle I:   Service Principal authentication only (no interactive credentials)
# Principle II:  All secrets retrieved from Azure Key Vault via notebookutils
# Principle VI:  Exponential backoff with full jitter (base 1s, max 60s, 5 retries)
# Principle VII: Structured audit log + DQ results written to Gold layer Delta tables

import asyncio
import random
import uuid
from datetime import date, datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from azure.identity import ClientSecretCredential
from kiota_abstractions.api_error import APIError
from msgraph import GraphServiceClient
from msgraph_beta import GraphServiceClient as BetaGraphServiceClient
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Key Vault URL — substituted per environment by fabric-cicd find_replace (Principle II)
KV_URL = "https://kv-copilot-dev.vault.azure.net"

# CELL ********************
# ─────────────────────────────────────────────────────────────────────────────
# T006 — Service Principal Authentication Helpers (Principle I & II)
# ─────────────────────────────────────────────────────────────────────────────


def _get_sp_credential() -> ClientSecretCredential:
    """Retrieve SP credentials from Key Vault and return a ClientSecretCredential.

    Secret names stored in Key Vault (values never in source or parameter.yml):
      sp-tenant-id, sp-client-id, sp-client-secret
    """
    tenant_id = notebookutils.credentials.getSecret(KV_URL, "sp-tenant-id")  # noqa: F821
    client_id = notebookutils.credentials.getSecret(KV_URL, "sp-client-id")  # noqa: F821
    client_secret = notebookutils.credentials.getSecret(KV_URL, "sp-client-secret")  # noqa: F821
    return ClientSecretCredential(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
    )


def get_graph_client() -> GraphServiceClient:
    """Return an authenticated Microsoft Graph v1.0 client (Service Principal).

    Permissions required: User.Read.All
    """
    return GraphServiceClient(
        credentials=_get_sp_credential(),
        scopes=["https://graph.microsoft.com/.default"],
    )


def get_beta_graph_client() -> BetaGraphServiceClient:
    """Return an authenticated Microsoft Graph beta client (Service Principal).

    Permissions required: Reports.Read.All, AuditLogsQuery.Read.All
    """
    return BetaGraphServiceClient(
        credentials=_get_sp_credential(),
        scopes=["https://graph.microsoft.com/.default"],
    )


# CELL ********************
# ─────────────────────────────────────────────────────────────────────────────
# T007 — Exponential Backoff Retry Utility (Principle VI)
# ─────────────────────────────────────────────────────────────────────────────


async def retry_with_backoff(
    func: Callable,
    *args: Any,
    max_retries: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    **kwargs: Any,
) -> Any:
    """Execute an async callable with exponential backoff and full jitter.

    Retry policy (Principle VI):
      - Max retries:  5
      - Base delay:   1 s
      - Max delay:   60 s
      - Jitter:       full — sleep = uniform(0, min(max_delay, base * 2^attempt))
      - 4xx errors:   fail fast except HTTP 429 (Too Many Requests → retry)
    """
    last_exc: Optional[Exception] = None
    for attempt in range(max_retries + 1):
        try:
            return await func(*args, **kwargs)
        except APIError as exc:
            status: Optional[int] = getattr(exc, "response_status_code", None)
            # Fast-fail on 4xx client errors (not retryable), except 429
            if status is not None and 400 <= status < 500 and status != 429:
                raise
            if attempt == max_retries:
                raise
            last_exc = exc

        cap = min(max_delay, base_delay * (2**attempt))
        await asyncio.sleep(random.uniform(0.0, cap))

    raise last_exc  # unreachable; satisfies type checkers


# CELL ********************
# ─────────────────────────────────────────────────────────────────────────────
# T008 — Structured Audit Logging (Principle VII)
# ─────────────────────────────────────────────────────────────────────────────

_AUDIT_LOG_SCHEMA = StructType(
    [
        StructField("log_id", StringType(), False),
        StructField("run_id", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("notebook_name", StringType(), False),
        StructField("operation", StringType(), False),
        StructField("target", StringType(), False),
        StructField("status", StringType(), False),
        StructField("records_affected", LongType(), True),
        StructField("duration_ms", LongType(), True),
        StructField("error_message", StringType(), True),
        StructField("http_status_code", IntegerType(), True),
    ]
)


def write_audit_entry(
    spark: SparkSession,
    run_id: str,
    notebook_name: str,
    operation: str,
    target: str,
    status: str,
    records_affected: Optional[int] = None,
    duration_ms: Optional[int] = None,
    error_message: Optional[str] = None,
    http_status_code: Optional[int] = None,
) -> None:
    """Append one structured entry to the gold_copilot_audit_log Delta table.

    operation: API_CALL | TABLE_READ | TABLE_WRITE | ERROR
    status:    SUCCESS  | FAILURE    | RETRY
    """
    row = [
        (
            str(uuid.uuid4()),
            run_id,
            datetime.now(timezone.utc),
            notebook_name,
            operation,
            target,
            status,
            int(records_affected) if records_affected is not None else None,
            int(duration_ms) if duration_ms is not None else None,
            error_message,
            int(http_status_code) if http_status_code is not None else None,
        )
    ]
    (
        spark.createDataFrame(row, schema=_AUDIT_LOG_SCHEMA)
        .write.format("delta")
        .mode("append")
        .saveAsTable("gold_copilot_audit_log")
    )


# CELL ********************
# ─────────────────────────────────────────────────────────────────────────────
# T009 — Data Quality Result Writer (Principle VII)
# ─────────────────────────────────────────────────────────────────────────────

_DQ_RESULTS_SCHEMA = StructType(
    [
        StructField("check_id", StringType(), False),
        StructField("run_id", StringType(), False),
        StructField("check_date", TimestampType(), False),
        StructField("table_name", StringType(), False),
        StructField("check_name", StringType(), False),
        StructField("check_result", StringType(), False),
        StructField("metric_value", DoubleType(), True),
        StructField("threshold", DoubleType(), True),
        StructField("details", StringType(), True),
    ]
)


def write_dq_result(
    spark: SparkSession,
    run_id: str,
    table_name: str,
    check_name: str,
    check_result: str,
    metric_value: Optional[float] = None,
    threshold: Optional[float] = None,
    details: Optional[str] = None,
) -> None:
    """Append one DQ check result to the gold_copilot_dq_results Delta table.

    check_result: PASS | WARN | FAIL
    """
    row = [
        (
            str(uuid.uuid4()),
            run_id,
            datetime.now(timezone.utc),
            table_name,
            check_name,
            check_result,
            float(metric_value) if metric_value is not None else None,
            float(threshold) if threshold is not None else None,
            details,
        )
    ]
    (
        spark.createDataFrame(row, schema=_DQ_RESULTS_SCHEMA)
        .write.format("delta")
        .mode("append")
        .saveAsTable("gold_copilot_dq_results")
    )


# CELL ********************
# ─────────────────────────────────────────────────────────────────────────────
# T010 — Schema Validation & Rejected Record Quarantine (Principle III, FR-012)
# ─────────────────────────────────────────────────────────────────────────────

_REJECTED_SCHEMA = StructType(
    [
        StructField("rejection_date", DateType(), False),
        StructField("run_id", StringType(), False),
        StructField("source_table", StringType(), False),
        StructField("rule_name", StringType(), False),
        StructField("error_message", StringType(), False),
        StructField("record_count", IntegerType(), False),
        StructField("sample_payload", StringType(), True),
    ]
)


def validate_schema(
    df: DataFrame,
    required_fields: List[str],
    type_checks: Optional[Dict[str, str]] = None,
) -> Tuple[DataFrame, List[Dict]]:
    """Validate a DataFrame against required fields and optional type constraints.

    Performs:
      1. Null checks on every field listed in *required_fields*
      2. Optional type checks via *type_checks* (field → expected simpleString type)

    Returns:
        valid_df:           Rows passing all checks.
        rejection_metadata: List of dicts with keys: rule_name, error_message,
                            record_count, sample_payload (1 row JSON or None).

    type_checks example: {"user_id": "string", "account_enabled": "boolean"}
    """
    import pyspark.sql.functions as F

    valid_df: DataFrame = df
    rejection_metadata: List[Dict] = []
    schema_map = {f.name: f.dataType.simpleString() for f in df.schema.fields}

    for field in required_fields:
        if field not in schema_map:
            rejection_metadata.append(
                {
                    "rule_name": f"missing_column:{field}",
                    "error_message": f"Required column '{field}' absent from DataFrame schema",
                    "record_count": df.count(),
                    "sample_payload": None,
                }
            )
            # All rows are invalid if a structurally required column does not exist
            valid_df = valid_df.where(F.lit(False))
            continue

        null_rows = valid_df.where(F.col(field).isNull())
        null_count = null_rows.count()
        if null_count > 0:
            sample = null_rows.limit(1).toJSON().first()
            valid_df = valid_df.where(F.col(field).isNotNull())
            rejection_metadata.append(
                {
                    "rule_name": f"null_required:{field}",
                    "error_message": f"Null value in required field '{field}'",
                    "record_count": null_count,
                    "sample_payload": sample,
                }
            )

    if type_checks:
        for field, expected_type in type_checks.items():
            if field not in schema_map:
                continue
            actual_type = schema_map[field]
            if not actual_type.startswith(expected_type):
                rejection_metadata.append(
                    {
                        "rule_name": f"type_mismatch:{field}",
                        "error_message": (
                            f"Field '{field}' expected type '{expected_type}' "
                            f"but found '{actual_type}'"
                        ),
                        "record_count": 0,
                        "sample_payload": None,
                    }
                )

    return valid_df, rejection_metadata


def quarantine_rejected(
    spark: SparkSession,
    run_id: str,
    source_table: str,
    rejection_metadata: List[Dict],
    quarantine_table: str,
) -> None:
    """Write rejection metadata to the designated _rejected quarantine Delta table.

    Table schema per data-model.md (silver_*_rejected):
      rejection_date, run_id, source_table, rule_name, error_message,
      record_count, sample_payload — partitioned by rejection_date.

    No-op when rejection_metadata is empty.
    """
    if not rejection_metadata:
        return

    rows = [
        (
            date.today(),
            run_id,
            source_table,
            r["rule_name"],
            r["error_message"],
            r["record_count"],
            r.get("sample_payload"),
        )
        for r in rejection_metadata
    ]
    (
        spark.createDataFrame(rows, schema=_REJECTED_SCHEMA)
        .write.format("delta")
        .mode("append")
        .partitionBy("rejection_date")
        .saveAsTable(quarantine_table)
    )


# CELL ********************
# ─────────────────────────────────────────────────────────────────────────────
# T011 — Graph API Pagination Helper
# ─────────────────────────────────────────────────────────────────────────────


async def paginate_graph_response(
    initial_response: Any,
    follow_next_link_func: Callable[[str], Any],
) -> List[Any]:
    """Collect all items from a paginated Microsoft Graph response.

    Follows odata_next_link until exhausted, applying retry_with_backoff on
    each subsequent page request.

    Args:
        initial_response:      First response object with a .value attribute.
        follow_next_link_func: Async callable(next_link: str) → next response.
                               Typically: lambda url: client.with_url(url).get()

    Returns:
        Flat list of all items across all pages.
    """
    all_items: List[Any] = []
    response: Any = initial_response

    while response is not None:
        if getattr(response, "value", None):
            all_items.extend(response.value)

        next_link: Optional[str] = getattr(response, "odata_next_link", None)
        if not next_link:
            break

        response = await retry_with_backoff(follow_next_link_func, next_link)

    return all_items


# CELL ********************
# ─────────────────────────────────────────────────────────────────────────────
# T012 — Run ID Generation & Parameter Loading
# ─────────────────────────────────────────────────────────────────────────────


def generate_run_id() -> str:
    """Generate a unique, timestamped run identifier.

    Format: run_<YYYYMMDDTHHMMSS>_<8-char uuid hex fragment>
    Example: run_20260226T143000_a1b2c3d4
    """
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    uid = uuid.uuid4().hex[:8]
    return f"run_{ts}_{uid}"


def load_parameters() -> Dict[str, Any]:
    """Return runtime configuration as a plain dict.

    All values are either:
      - Embedded at deploy-time by fabric-cicd find_replace (KV_URL),
      - Default constants that the Fabric Data Pipeline can override via
        notebook parameters, or
      - Identifiers pointing to secrets in Key Vault (never inline values).

    Callers should prefer accessing via: params = load_parameters()
    """
    return {
        # Infrastructure (Principle II)
        "kv_url": KV_URL,
        "lakehouse_name": "CopilotUsageLakehouse",
        # Graph API — usage report period (D7 | D30 | D90 | D180)
        "usage_report_period": "D7",
        # Freshness gate: warn if Bronze tables are stale beyond this many hours (FR-013)
        "freshness_threshold_hours": 48,
        # Underutilization scoring thresholds (FR-008, FR-010)
        "underutilization_threshold": 20,
        "new_assignment_grace_days": 14,
        # Retry policy (Principle VI)
        "api_max_retries": 5,
        "api_base_delay_s": 1.0,
        "api_max_delay_s": 60.0,
    }
