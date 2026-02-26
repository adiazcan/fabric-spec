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
# 00_watermarks — Incremental loading watermark management
#
# Provides read_watermark() and write_watermark() operating on the sys_watermarks
# Delta table. Called at the start (read) and end (write) of each ingestion notebook
# to enable incremental loading without full-table refreshes (Principle IV).
#
# Source names (convention):
#   graph_users   — deltaLink token (string URL)
#   graph_usage   — reportRefreshDate ISO date string ("2026-01-15")
#   graph_audit   — filterStartDateTime ISO datetime string ("2026-01-15T00:00:00Z")

from datetime import datetime, timezone
from typing import Optional

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# CELL ********************
# ─────────────────────────────────────────────────────────────────────────────
# T013 — sys_watermarks Table Schema
# ─────────────────────────────────────────────────────────────────────────────

_WATERMARK_SCHEMA = StructType(
    [
        StructField("source_name", StringType(), False),
        StructField("last_watermark_value", StringType(), False),
        StructField("last_run_timestamp", TimestampType(), False),
        StructField("last_run_id", StringType(), False),
        StructField("records_processed", LongType(), True),
    ]
)

_WATERMARKS_TABLE = "sys_watermarks"


def _ensure_watermarks_table(spark: SparkSession) -> None:
    """Create sys_watermarks as an empty Delta table if it does not yet exist."""
    if not spark.catalog.tableExists(_WATERMARKS_TABLE):
        (
            spark.createDataFrame([], schema=_WATERMARK_SCHEMA)
            .write.format("delta")
            .saveAsTable(_WATERMARKS_TABLE)
        )


# CELL ********************
# ─────────────────────────────────────────────────────────────────────────────
# T013 — Watermark Read/Write Functions
# ─────────────────────────────────────────────────────────────────────────────


def read_watermark(spark: SparkSession, source_name: str) -> Optional[str]:
    """Return the last_watermark_value for *source_name*, or None on first run.

    Used by ingestion notebooks to determine the incremental load starting point:
      - graph_users:  deltaLink token (full URL returned by Graph API delta query)
      - graph_usage:  reportRefreshDate ISO date string, e.g. "2026-01-15"
      - graph_audit:  filterStartDateTime ISO datetime string, e.g. "2026-01-15T00:00:00Z"

    Returns None when no prior watermark exists (triggers full initial load).
    """
    _ensure_watermarks_table(spark)

    rows = (
        spark.table(_WATERMARKS_TABLE)
        .where(F.col("source_name") == source_name)
        .select("last_watermark_value")
        .collect()
    )
    return rows[0]["last_watermark_value"] if rows else None


def write_watermark(
    spark: SparkSession,
    source_name: str,
    value: str,
    run_id: str,
    records_processed: Optional[int] = None,
) -> None:
    """Upsert the watermark for *source_name* into sys_watermarks.

    Creates the table on first call. Uses Delta MERGE to upsert on source_name
    so that re-running a notebook is idempotent — the watermark advances only
    when the ingestion notebook commits a new run_id.

    Args:
        source_name:       Logical source identifier (e.g. "graph_users").
        value:             New watermark value to persist.
        run_id:            Current pipeline run ID (from helpers.generate_run_id).
        records_processed: Number of records processed in this run (optional,
                           used for observability / audit log enrichment).
    """
    _ensure_watermarks_table(spark)

    new_row = spark.createDataFrame(
        [
            (
                source_name,
                value,
                datetime.now(timezone.utc),
                run_id,
                int(records_processed) if records_processed is not None else None,
            )
        ],
        schema=_WATERMARK_SCHEMA,
    )

    (
        DeltaTable.forName(spark, _WATERMARKS_TABLE)
        .alias("tgt")
        .merge(
            new_row.alias("src"),
            "tgt.source_name = src.source_name",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
