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
# ─────────────────────────────────────────────────────────────────────────────
# [US2/T022] DQ checks: silver_copilot_usage
# ─────────────────────────────────────────────────────────────────────────────

if not spark.catalog.tableExists("silver_copilot_usage"):
    write_dq_result(
        spark,
        run_id=run_id,
        table_name="silver_copilot_usage",
        check_name="table_exists",
        check_result="FAIL",
        details="silver_copilot_usage table does not exist — run 04_transform_silver first",
    )
else:
    usage_df = spark.table("silver_copilot_usage")
    total_usage = usage_df.count()

    # DQ-CU01: Row count — silver_copilot_usage must have at least 1 row
    write_dq_result(
        spark,
        run_id=run_id,
        table_name="silver_copilot_usage",
        check_name="row_count",
        check_result="PASS" if total_usage > 0 else "FAIL",
        metric_value=float(total_usage),
        threshold=1.0,
        details=f"silver_copilot_usage row count: {total_usage:,}",
    )

    # DQ-CU02: Null percentage on user_principal_name (required composite key component)
    null_upn_count = usage_df.filter(F.col("user_principal_name").isNull()).count()
    null_upn_pct = (null_upn_count / total_usage * 100.0) if total_usage > 0 else 0.0
    write_dq_result(
        spark,
        run_id=run_id,
        table_name="silver_copilot_usage",
        check_name="null_pct_user_principal_name",
        check_result="PASS" if null_upn_pct == 0.0 else "FAIL",
        metric_value=null_upn_pct,
        threshold=0.0,
        details=(
            f"Null user_principal_name count: {null_upn_count:,} "
            f"({null_upn_pct:.2f}%)"
        ),
    )

    # DQ-CU03: Data freshness — max(report_refresh_date) must be within 48 hours
    # Microsoft reports a 24-48 h data latency; flag WARN at 24 h, FAIL at 48 h
    max_report_date = usage_df.agg(F.max("report_refresh_date")).collect()[0][0]
    if max_report_date is None:
        usage_freshness_result = "FAIL"
        usage_hours_stale = None
    else:
        usage_hours_stale = (date.today() - max_report_date).days * 24
        if usage_hours_stale > FRESHNESS_THRESHOLD_HOURS:
            usage_freshness_result = "FAIL"
        elif usage_hours_stale > 24:
            usage_freshness_result = "WARN"
        else:
            usage_freshness_result = "PASS"

    write_dq_result(
        spark,
        run_id=run_id,
        table_name="silver_copilot_usage",
        check_name="report_refresh_date_freshness_hours",
        check_result=usage_freshness_result,
        metric_value=float(usage_hours_stale) if usage_hours_stale is not None else None,
        threshold=float(FRESHNESS_THRESHOLD_HOURS),
        details=(
            f"Max report_refresh_date: {max_report_date}, "
            f"hours stale: {usage_hours_stale}"
        ),
    )

    print(
        f"[{NOTEBOOK_NAME}] silver_copilot_usage DQ: rows={total_usage:,}, "
        f"freshness={usage_hours_stale}h"
    )

# CELL ********************
# ─────────────────────────────────────────────────────────────────────────────
# [US3/T025] DQ checks: gold_copilot_usage_scores
# ─────────────────────────────────────────────────────────────────────────────

SCORES_TABLE = "gold_copilot_usage_scores"
UNDERUTILIZATION_THRESHOLD = 20  # must match parameter.yml default

if not spark.catalog.tableExists(SCORES_TABLE):
    write_dq_result(
        spark,
        run_id=run_id,
        table_name=SCORES_TABLE,
        check_name="table_exists",
        check_result="FAIL",
        details=(
            f"{SCORES_TABLE} table does not exist — run 06_compute_scores first"
        ),
    )
else:
    scores_df = spark.table(SCORES_TABLE)
    today = date.today()

    # DQ-SC01: All licensed users should have a score row for today's score_date
    if spark.catalog.tableExists("silver_user_licenses"):
        licensed_count = (
            spark.table("silver_user_licenses")
            .filter(F.col("has_copilot_license") == True)  # noqa: E712
            .count()
        )
        today_scores_count = scores_df.filter(
            F.col("score_date") == F.lit(today)
        ).count()
        coverage_result = "PASS" if today_scores_count >= licensed_count else "WARN"
        write_dq_result(
            spark,
            run_id=run_id,
            table_name=SCORES_TABLE,
            check_name="licensed_user_score_coverage",
            check_result=coverage_result,
            metric_value=float(today_scores_count),
            threshold=float(licensed_count),
            details=(
                f"Licensed users: {licensed_count:,}, "
                f"score rows for today: {today_scores_count:,}"
            ),
        )
    else:
        write_dq_result(
            spark,
            run_id=run_id,
            table_name=SCORES_TABLE,
            check_name="licensed_user_score_coverage",
            check_result="WARN",
            details="silver_user_licenses not available — cannot verify coverage",
        )

    # DQ-SC02: Score range — all usage_score values must be in [0, 100]
    out_of_range_count = scores_df.filter(
        (F.col("usage_score") < 0) | (F.col("usage_score") > 100)
    ).count()
    write_dq_result(
        spark,
        run_id=run_id,
        table_name=SCORES_TABLE,
        check_name="score_range_0_100",
        check_result="PASS" if out_of_range_count == 0 else "FAIL",
        metric_value=float(out_of_range_count),
        threshold=0.0,
        details=f"Scores outside [0, 100] range: {out_of_range_count:,}",
    )

    # DQ-SC03: is_underutilized flag consistency
    # Rule: is_underutilized should be true ONLY when score ≤ threshold AND NOT
    # is_new_assignment. Detect any rows that violate this invariant.
    flag_violations = scores_df.filter(
        # Case 1: flagged underutilized but score > threshold
        (
            (F.col("is_underutilized") == True)  # noqa: E712
            & (F.col("usage_score") > UNDERUTILIZATION_THRESHOLD)
        )
        # Case 2: flagged underutilized but is a new assignment (grace period)
        | (
            (F.col("is_underutilized") == True)  # noqa: E712
            & (F.col("is_new_assignment") == True)  # noqa: E712
        )
        # Case 3: NOT flagged but score ≤ threshold AND not new assignment
        | (
            (F.col("is_underutilized") == False)  # noqa: E712
            & (F.col("usage_score") <= UNDERUTILIZATION_THRESHOLD)
            & (F.col("is_new_assignment") == False)  # noqa: E712
        )
    ).count()

    write_dq_result(
        spark,
        run_id=run_id,
        table_name=SCORES_TABLE,
        check_name="underutilized_flag_consistency",
        check_result="PASS" if flag_violations == 0 else "FAIL",
        metric_value=float(flag_violations),
        threshold=0.0,
        details=(
            f"Rows with inconsistent is_underutilized flag: {flag_violations:,} "
            f"(threshold={UNDERUTILIZATION_THRESHOLD})"
        ),
    )

    print(
        f"[{NOTEBOOK_NAME}] {SCORES_TABLE} DQ: "
        f"out_of_range={out_of_range_count:,}, "
        f"flag_violations={flag_violations:,}"
    )

# CELL ********************
# ─────────────────────────────────────────────────────────────────────────────
# [US4/T029] DQ checks: silver_audit_events
# ─────────────────────────────────────────────────────────────────────────────

if not spark.catalog.tableExists("silver_audit_events"):
    write_dq_result(
        spark,
        run_id=run_id,
        table_name="silver_audit_events",
        check_name="table_exists",
        check_result="FAIL",
        details="silver_audit_events table does not exist — run 04_transform_silver first",
    )
else:
    audit_df = spark.table("silver_audit_events")
    total_audit = audit_df.count()

    # DQ-AE01: Row count — silver_audit_events must have at least 1 row
    write_dq_result(
        spark,
        run_id=run_id,
        table_name="silver_audit_events",
        check_name="row_count",
        check_result="PASS" if total_audit > 0 else "FAIL",
        metric_value=float(total_audit),
        threshold=1.0,
        details=f"silver_audit_events row count: {total_audit:,}",
    )

    # DQ-AE02: Null percentage on event_id (primary key — must be 0%)
    null_event_id_count = audit_df.filter(F.col("event_id").isNull()).count()
    null_event_id_pct = (
        (null_event_id_count / total_audit * 100.0) if total_audit > 0 else 0.0
    )
    write_dq_result(
        spark,
        run_id=run_id,
        table_name="silver_audit_events",
        check_name="null_pct_event_id",
        check_result="PASS" if null_event_id_pct == 0.0 else "FAIL",
        metric_value=null_event_id_pct,
        threshold=0.0,
        details=(
            f"Null event_id count: {null_event_id_count:,} "
            f"({null_event_id_pct:.2f}%)"
        ),
    )

    # DQ-AE03: Null percentage on created_date_time (required — must be 0%)
    null_cdt_count = audit_df.filter(F.col("created_date_time").isNull()).count()
    null_cdt_pct = (
        (null_cdt_count / total_audit * 100.0) if total_audit > 0 else 0.0
    )
    write_dq_result(
        spark,
        run_id=run_id,
        table_name="silver_audit_events",
        check_name="null_pct_created_date_time",
        check_result="PASS" if null_cdt_pct == 0.0 else "FAIL",
        metric_value=null_cdt_pct,
        threshold=0.0,
        details=(
            f"Null created_date_time count: {null_cdt_count:,} "
            f"({null_cdt_pct:.2f}%)"
        ),
    )

    # DQ-AE04: Data freshness — max(created_date_time) should be recent
    max_audit_dt = audit_df.agg(F.max("created_date_time")).collect()[0][0]
    if max_audit_dt is None:
        audit_freshness_result = "FAIL"
        audit_hours_stale = None
    else:
        from datetime import datetime as dt

        now_utc = dt.now(timezone.utc)
        audit_hours_stale = (now_utc - max_audit_dt.replace(tzinfo=timezone.utc)).total_seconds() / 3600
        audit_hours_stale = round(audit_hours_stale, 1)
        if audit_hours_stale > FRESHNESS_THRESHOLD_HOURS:
            audit_freshness_result = "FAIL"
        elif audit_hours_stale > 24:
            audit_freshness_result = "WARN"
        else:
            audit_freshness_result = "PASS"

    write_dq_result(
        spark,
        run_id=run_id,
        table_name="silver_audit_events",
        check_name="data_freshness_hours",
        check_result=audit_freshness_result,
        metric_value=float(audit_hours_stale) if audit_hours_stale is not None else None,
        threshold=float(FRESHNESS_THRESHOLD_HOURS),
        details=(
            f"Max created_date_time: {max_audit_dt}, "
            f"hours stale: {audit_hours_stale}"
        ),
    )

    print(
        f"[{NOTEBOOK_NAME}] silver_audit_events DQ: rows={total_audit:,}, "
        f"freshness={audit_hours_stale}h"
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
