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
# 99_dq_checks — Data quality validation for all pipeline tables
#
# [US1/T018] User + license DQ checks (this phase)
# [US2/T022] (Phase 4) Usage DQ checks
# [US3/T025] (Phase 5) Score DQ checks
# [US4/T029] (Phase 6) Audit DQ checks
#
# All results written to gold_copilot_dq_results Delta table (Principle VII).
# check_result values: PASS | WARN | FAIL
#   PASS: metric fully within acceptable bounds
#   WARN: metric approaching threshold — worth investigating
#   FAIL: metric exceeds threshold — pipeline output is suspect

# noqa: E402
%run helpers

# CELL ********************

from datetime import date, datetime, timezone

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

NOTEBOOK_NAME = "99_dq_checks"
FRESHNESS_THRESHOLD_HOURS = 48  # FR-013: Bronze tables must not exceed 48 h staleness

spark = SparkSession.builder.getOrCreate()
run_id = generate_run_id()

# CELL ********************
# ─────────────────────────────────────────────────────────────────────────────
# [US1/T018] DQ checks: silver_users
# ─────────────────────────────────────────────────────────────────────────────

if not spark.catalog.tableExists("silver_users"):
    write_dq_result(
        spark,
        run_id=run_id,
        table_name="silver_users",
        check_name="table_exists",
        check_result="FAIL",
        details="silver_users table does not exist — run 04_transform_silver first",
    )
else:
    silver_users_df = spark.table("silver_users")
    total_users = silver_users_df.count()

    # DQ-U01: Row count — silver_users must have at least 1 row
    write_dq_result(
        spark,
        run_id=run_id,
        table_name="silver_users",
        check_name="row_count",
        check_result="PASS" if total_users > 0 else "FAIL",
        metric_value=float(total_users),
        threshold=1.0,
        details=f"silver_users row count: {total_users:,}",
    )

    # DQ-U02: Null percentage on user_id (primary key — must be 0%)
    null_user_id_count = silver_users_df.filter(F.col("user_id").isNull()).count()
    null_user_id_pct = (
        (null_user_id_count / total_users * 100.0) if total_users > 0 else 0.0
    )
    write_dq_result(
        spark,
        run_id=run_id,
        table_name="silver_users",
        check_name="null_pct_user_id",
        check_result="PASS" if null_user_id_pct == 0.0 else "FAIL",
        metric_value=null_user_id_pct,
        threshold=0.0,
        details=(
            f"Null user_id count: {null_user_id_count:,} "
            f"({null_user_id_pct:.2f}%)"
        ),
    )

    # DQ-U03: Null percentage on display_name (required field — warn if any nulls)
    null_display_name_count = (
        silver_users_df.filter(F.col("display_name").isNull()).count()
    )
    null_display_name_pct = (
        (null_display_name_count / total_users * 100.0) if total_users > 0 else 0.0
    )
    write_dq_result(
        spark,
        run_id=run_id,
        table_name="silver_users",
        check_name="null_pct_display_name",
        check_result="PASS" if null_display_name_pct == 0.0 else "WARN",
        metric_value=null_display_name_pct,
        threshold=0.0,
        details=(
            f"Null display_name count: {null_display_name_count:,} "
            f"({null_display_name_pct:.2f}%)"
        ),
    )

    # DQ-U04: Data freshness — max(_ingestion_date) must not exceed FRESHNESS_THRESHOLD_HOURS (FR-013)
    max_ingestion_date = (
        silver_users_df.agg(F.max("_ingestion_date")).collect()[0][0]
    )
    if max_ingestion_date is None:
        freshness_result = "FAIL"
        hours_stale = None
    else:
        hours_stale = (date.today() - max_ingestion_date).days * 24
        if hours_stale > FRESHNESS_THRESHOLD_HOURS:
            freshness_result = "FAIL"
        elif hours_stale > 24:
            freshness_result = "WARN"
        else:
            freshness_result = "PASS"

    write_dq_result(
        spark,
        run_id=run_id,
        table_name="silver_users",
        check_name="data_freshness_hours",
        check_result=freshness_result,
        metric_value=float(hours_stale) if hours_stale is not None else None,
        threshold=float(FRESHNESS_THRESHOLD_HOURS),
        details=(
            f"Max _ingestion_date: {max_ingestion_date}, "
            f"hours stale: {hours_stale}"
        ),
    )

    print(
        f"[{NOTEBOOK_NAME}] silver_users DQ: rows={total_users:,}, "
        f"freshness={hours_stale}h"
    )

# CELL ********************
# ─────────────────────────────────────────────────────────────────────────────
# [US1/T018] DQ checks: silver_user_licenses
# ─────────────────────────────────────────────────────────────────────────────

if not spark.catalog.tableExists("silver_user_licenses"):
    write_dq_result(
        spark,
        run_id=run_id,
        table_name="silver_user_licenses",
        check_name="table_exists",
        check_result="FAIL",
        details=(
            "silver_user_licenses table does not exist — run 04_transform_silver first"
        ),
    )
else:
    licenses_df = spark.table("silver_user_licenses")
    total_licenses = licenses_df.count()
    # Retrieve total_users for cross-table consistency check (may be 0 if silver_users skipped)
    _users_count = (
        spark.table("silver_users").count()
        if spark.catalog.tableExists("silver_users")
        else 0
    )

    # DQ-L01: Row count — at least 1 row in silver_user_licenses
    write_dq_result(
        spark,
        run_id=run_id,
        table_name="silver_user_licenses",
        check_name="row_count",
        check_result="PASS" if total_licenses > 0 else "FAIL",
        metric_value=float(total_licenses),
        threshold=1.0,
        details=f"silver_user_licenses row count: {total_licenses:,}",
    )

    # DQ-L02: Row count parity — every user in silver_users should have a license record
    user_license_diff = abs(_users_count - total_licenses)
    write_dq_result(
        spark,
        run_id=run_id,
        table_name="silver_user_licenses",
        check_name="user_license_row_count_match",
        check_result="PASS" if user_license_diff == 0 else "WARN",
        metric_value=float(user_license_diff),
        threshold=0.0,
        details=(
            f"silver_users={_users_count:,}, "
            f"silver_user_licenses={total_licenses:,}, "
            f"diff={user_license_diff:,}"
        ),
    )

    # DQ-L03: Copilot-licensed users must have a non-null license_assignment_date
    copilot_missing_date = (
        licenses_df.filter(
            (F.col("has_copilot_license") == True)  # noqa: E712
            & F.col("license_assignment_date").isNull()
        ).count()
    )
    write_dq_result(
        spark,
        run_id=run_id,
        table_name="silver_user_licenses",
        check_name="copilot_license_assignment_date_not_null",
        check_result="PASS" if copilot_missing_date == 0 else "WARN",
        metric_value=float(copilot_missing_date),
        threshold=0.0,
        details=(
            f"Copilot-licensed users missing assignment date: {copilot_missing_date:,}"
        ),
    )

    print(
        f"[{NOTEBOOK_NAME}] silver_user_licenses DQ: rows={total_licenses:,}, "
        f"copilot_users={licenses_df.filter(F.col('has_copilot_license') == True).count():,}"  # noqa: E712
    )

# CELL ********************

write_audit_entry(
    spark,
    run_id=run_id,
    notebook_name=NOTEBOOK_NAME,
    operation="TABLE_WRITE",
    target="gold_copilot_dq_results",
    status="SUCCESS",
)
print(f"[{NOTEBOOK_NAME}] All DQ checks complete | Run ID: {run_id}")
