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
# 05_transform_gold — Gold layer transformations
#
# [US1/T017] License summary: join silver_users + silver_user_licenses
#            → Delta MERGE into gold_copilot_license_summary
# [US2/T021] (Phase 4) Usage enrichment: enrich gold_copilot_license_summary
#            with last_activity_date, days_since_last_activity, apps_used_count
# [US3/T024] (Phase 5) Score enrichment: enrich gold_copilot_license_summary
#            with latest_usage_score and is_underutilized
# [US4/T028] (Phase 6) Audit summary: aggregate silver_audit_events
#            → Delta MERGE into gold_copilot_audit_summary
#
# Principle IV:  Delta MERGE for dimension updates — no full refreshes
# Principle VII: Audit log entries for every write operation

# noqa: E402
%run helpers

# CELL ********************

from datetime import datetime, timezone

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

NOTEBOOK_NAME = "05_transform_gold"
GOLD_LICENSE_TABLE = "gold_copilot_license_summary"

spark = SparkSession.builder.getOrCreate()
run_id = generate_run_id()
params = load_parameters()
NEW_ASSIGNMENT_GRACE_DAYS: int = params["new_assignment_grace_days"]  # default 14

# CELL ********************
# ─────────────────────────────────────────────────────────────────────────────
# [US1/T017] Gold license summary: join silver_users + silver_user_licenses
#            → gold_copilot_license_summary
# ─────────────────────────────────────────────────────────────────────────────

if not spark.catalog.tableExists("silver_users"):
    raise RuntimeError(
        "silver_users table does not exist. Run 04_transform_silver first."
    )
if not spark.catalog.tableExists("silver_user_licenses"):
    raise RuntimeError(
        "silver_user_licenses table does not exist. Run 04_transform_silver first."
    )

users_df = spark.table("silver_users")
licenses_df = spark.table("silver_user_licenses")

# Left join: all users appear in Gold regardless of license state
gold_df = (
    users_df.alias("u")
    .join(licenses_df.alias("l"), on="user_id", how="left")
    .select(
        F.col("u.user_id"),
        F.col("u.user_principal_name"),
        F.col("u.display_name"),
        F.col("u.department"),
        F.col("u.job_title"),
        # account_status: "departed" when account_enabled=false, "active" otherwise (FR-014)
        F.when(~F.col("u.account_enabled"), F.lit("departed"))
        .otherwise(F.lit("active"))
        .alias("account_status"),
        F.coalesce(F.col("l.has_copilot_license"), F.lit(False)).alias(
            "has_copilot_license"
        ),
        F.col("l.license_assignment_date"),
        # license_days_held: null for users without a Copilot license
        F.when(
            F.coalesce(F.col("l.has_copilot_license"), F.lit(False))
            & F.col("l.license_assignment_date").isNotNull(),
            F.datediff(F.current_date(), F.col("l.license_assignment_date")),
        )
        .otherwise(F.lit(None).cast("int"))
        .alias("license_days_held"),
        # is_new_assignment: true when license held < 14 days (FR-010)
        F.when(
            F.coalesce(F.col("l.has_copilot_license"), F.lit(False))
            & F.col("l.license_assignment_date").isNotNull(),
            F.datediff(F.current_date(), F.col("l.license_assignment_date"))
            < NEW_ASSIGNMENT_GRACE_DAYS,
        )
        .otherwise(F.lit(False))
        .alias("is_new_assignment"),
        # Activity + score fields populated by US2 and US3 — null / false on initial load
        F.lit(None).cast("date").alias("last_activity_date"),
        F.lit(None).cast("int").alias("days_since_last_activity"),
        F.lit(None).cast("int").alias("apps_used_count"),
        F.lit(None).cast("int").alias("latest_usage_score"),
        F.lit(False).alias("is_underutilized"),
        F.current_timestamp().alias("_last_updated"),
    )
)

# Delta MERGE into gold_copilot_license_summary on user_id
# whenMatchedUpdate: refresh user/license fields only; preserve activity+score fields
# set by US2 (T021) and US3 (T024) in the target table.
merge_start = datetime.now(timezone.utc)

if spark.catalog.tableExists(GOLD_LICENSE_TABLE):
    (
        DeltaTable.forName(spark, GOLD_LICENSE_TABLE)
        .alias("tgt")
        .merge(gold_df.alias("src"), "tgt.user_id = src.user_id")
        .whenMatchedUpdate(
            set={
                # User profile fields — always refreshed from Silver
                "user_principal_name": "src.user_principal_name",
                "display_name": "src.display_name",
                "department": "src.department",
                "job_title": "src.job_title",
                "account_status": "src.account_status",
                # License fields — always refreshed from Silver
                "has_copilot_license": "src.has_copilot_license",
                "license_assignment_date": "src.license_assignment_date",
                "license_days_held": "src.license_days_held",
                "is_new_assignment": "src.is_new_assignment",
                # Preserve activity (last_activity_date, days_since_last_activity,
                # apps_used_count) and score (latest_usage_score, is_underutilized)
                # fields — updated by US2/T021 and US3/T024 respectively.
                "_last_updated": "src._last_updated",
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    # First run: create the Gold table
    gold_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_LICENSE_TABLE)

merge_ms = int((datetime.now(timezone.utc) - merge_start).total_seconds() * 1000)
gold_count = gold_df.count()

write_audit_entry(
    spark,
    run_id=run_id,
    notebook_name=NOTEBOOK_NAME,
    operation="TABLE_WRITE",
    target=GOLD_LICENSE_TABLE,
    status="SUCCESS",
    records_affected=gold_count,
    duration_ms=merge_ms,
)
print(
    f"[{NOTEBOOK_NAME}] {GOLD_LICENSE_TABLE}: merged {gold_count:,} rows | Run ID: {run_id}"
)

# CELL ********************
# ─────────────────────────────────────────────────────────────────────────────
# [US2/T021] Gold usage enrichment: enrich gold_copilot_license_summary
#            with last_activity_date, days_since_last_activity, apps_used_count
#
# Takes the latest usage record per user (max report_refresh_date), computes
# apps_used_count from non-null per-app last-activity columns (8 apps total),
# and Delta MERGE-updates only the activity fields — preserving license and
# score fields set by US1 (T017) and US3 (T024) respectively.
# ─────────────────────────────────────────────────────────────────────────────

if not spark.catalog.tableExists("silver_copilot_usage"):
    raise RuntimeError(
        "silver_copilot_usage table does not exist. Run 04_transform_silver (US2/T020) first."
    )
if not spark.catalog.tableExists(GOLD_LICENSE_TABLE):
    raise RuntimeError(
        f"{GOLD_LICENSE_TABLE} does not exist. Run US1 (T017) before US2 (T021)."
    )

from functools import reduce

from pyspark.sql import Window

# Per-app last-activity columns (8 apps) — count of non-null = apps_used_count
_APP_ACTIVITY_COLS = [
    "teams_last_activity_date",
    "word_last_activity_date",
    "excel_last_activity_date",
    "powerpoint_last_activity_date",
    "outlook_last_activity_date",
    "onenote_last_activity_date",
    "loop_last_activity_date",
    "copilot_chat_last_activity_date",
]

# Take the latest usage record per user (highest report_refresh_date)
usage_w = Window.partitionBy("user_principal_name").orderBy(
    F.desc("report_refresh_date")
)
usage_latest_df = (
    spark.table("silver_copilot_usage")
    .withColumn("_rnk", F.rank().over(usage_w))
    .filter(F.col("_rnk") == 1)
    .drop("_rnk")
)

# apps_used_count: count of non-null per-app last-activity columns
apps_used_expr = reduce(
    lambda a, b: a + b,
    [
        F.when(F.col(c).isNotNull(), F.lit(1)).otherwise(F.lit(0))
        for c in _APP_ACTIVITY_COLS
    ],
)
usage_enriched_df = usage_latest_df.withColumn("apps_used_count", apps_used_expr)

# Join gold_copilot_license_summary with latest usage records on user_principal_name
gold_current_df = spark.table(GOLD_LICENSE_TABLE)
usage_update_df = (
    gold_current_df.alias("g")
    .join(
        usage_enriched_df.alias("u"),
        on=F.col("g.user_principal_name") == F.col("u.user_principal_name"),
        how="inner",
    )
    .select(
        F.col("g.user_id"),
        F.col("u.last_activity_date"),
        # days_since_last_activity: null when user has no recorded activity
        F.when(
            F.col("u.last_activity_date").isNotNull(),
            F.datediff(F.current_date(), F.col("u.last_activity_date")),
        )
        .otherwise(F.lit(None).cast("int"))
        .alias("days_since_last_activity"),
        F.col("u.apps_used_count"),
        F.current_timestamp().alias("_last_updated"),
    )
)

# Delta MERGE: update only activity fields — preserve license and score fields
usage_enrich_start = datetime.now(timezone.utc)

(
    DeltaTable.forName(spark, GOLD_LICENSE_TABLE)
    .alias("tgt")
    .merge(usage_update_df.alias("src"), "tgt.user_id = src.user_id")
    .whenMatchedUpdate(
        set={
            "last_activity_date": "src.last_activity_date",
            "days_since_last_activity": "src.days_since_last_activity",
            "apps_used_count": "src.apps_used_count",
            "_last_updated": "src._last_updated",
        }
    )
    .execute()
)

usage_enrich_ms = int(
    (datetime.now(timezone.utc) - usage_enrich_start).total_seconds() * 1000
)
usage_enrich_count = usage_update_df.count()

write_audit_entry(
    spark,
    run_id=run_id,
    notebook_name=NOTEBOOK_NAME,
    operation="TABLE_WRITE",
    target=GOLD_LICENSE_TABLE,
    status="SUCCESS",
    records_affected=usage_enrich_count,
    duration_ms=usage_enrich_ms,
)
print(
    f"[{NOTEBOOK_NAME}] {GOLD_LICENSE_TABLE} usage enrichment: "
    f"updated {usage_enrich_count:,} rows | Run ID: {run_id}"
)

# CELL ********************
# ─────────────────────────────────────────────────────────────────────────────
# [US3/T024] Gold score enrichment: enrich gold_copilot_license_summary
#            with latest_usage_score and is_underutilized from
#            gold_copilot_usage_scores
#
# Takes the latest score row per user (max score_date) and Delta MERGE-updates
# only the score fields — preserving license and activity fields set by
# US1 (T017) and US2 (T021) respectively.
# ─────────────────────────────────────────────────────────────────────────────

SCORES_TABLE = "gold_copilot_usage_scores"

if not spark.catalog.tableExists(SCORES_TABLE):
    print(
        f"[{NOTEBOOK_NAME}] {SCORES_TABLE} does not exist — "
        "skipping score enrichment (run 06_compute_scores first)"
    )
else:
    # Take the latest score row per user (highest score_date)
    score_w = Window.partitionBy("user_id").orderBy(F.desc("score_date"))
    latest_scores_df = (
        spark.table(SCORES_TABLE)
        .withColumn("_rnk", F.rank().over(score_w))
        .filter(F.col("_rnk") == 1)
        .drop("_rnk")
        .select(
            "user_id",
            F.col("usage_score").alias("latest_usage_score"),
            "is_underutilized",
            F.current_timestamp().alias("_last_updated"),
        )
    )

    # Delta MERGE: update only score fields — preserve license and activity fields
    score_enrich_start = datetime.now(timezone.utc)

    (
        DeltaTable.forName(spark, GOLD_LICENSE_TABLE)
        .alias("tgt")
        .merge(latest_scores_df.alias("src"), "tgt.user_id = src.user_id")
        .whenMatchedUpdate(
            set={
                "latest_usage_score": "src.latest_usage_score",
                "is_underutilized": "src.is_underutilized",
                "_last_updated": "src._last_updated",
            }
        )
        .execute()
    )

    score_enrich_ms = int(
        (datetime.now(timezone.utc) - score_enrich_start).total_seconds() * 1000
    )
    score_enrich_count = latest_scores_df.count()

    write_audit_entry(
        spark,
        run_id=run_id,
        notebook_name=NOTEBOOK_NAME,
        operation="TABLE_WRITE",
        target=GOLD_LICENSE_TABLE,
        status="SUCCESS",
        records_affected=score_enrich_count,
        duration_ms=score_enrich_ms,
    )
    print(
        f"[{NOTEBOOK_NAME}] {GOLD_LICENSE_TABLE} score enrichment: "
        f"updated {score_enrich_count:,} rows | Run ID: {run_id}"
    )

# CELL ********************
# ─────────────────────────────────────────────────────────────────────────────
# [US4/T028] Gold audit summary: aggregate silver_audit_events
#            → Delta MERGE into gold_copilot_audit_summary
#
# Aggregates audit events by user_id + activity_date + application_name,
# computing event_count, success_count, and failure_count. Uses Delta MERGE
# (upsert) to handle late-arriving audit events gracefully.
# ─────────────────────────────────────────────────────────────────────────────

AUDIT_SUMMARY_TABLE = "gold_copilot_audit_summary"

if not spark.catalog.tableExists("silver_audit_events"):
    print(
        f"[{NOTEBOOK_NAME}] silver_audit_events does not exist — "
        "skipping audit summary (run 04_transform_silver US4/T027 first)"
    )
else:
    audit_events_df = spark.table("silver_audit_events")

    # Aggregate by user_id + activity_date (from created_date_time) + application_name (service)
    # success_count / failure_count derived from operation containing "Failed" pattern
    audit_summary_df = (
        audit_events_df.withColumn(
            "activity_date", F.to_date(F.col("created_date_time"))
        )
        .withColumn(
            "application_name", F.coalesce(F.col("service"), F.lit("Unknown"))
        )
        .groupBy("user_id", "activity_date", "application_name")
        .agg(
            F.count("*").alias("event_count"),
            # Success: events where operation does NOT contain "Failed"
            F.sum(
                F.when(
                    ~F.lower(F.col("operation")).contains("failed"), F.lit(1)
                ).otherwise(F.lit(0))
            ).alias("success_count"),
            # Failure: events where operation contains "Failed"
            F.sum(
                F.when(
                    F.lower(F.col("operation")).contains("failed"), F.lit(1)
                ).otherwise(F.lit(0))
            ).alias("failure_count"),
        )
        .withColumn("_run_id", F.lit(run_id))
    )

    # Delta MERGE on composite key — upsert for late-arriving events
    audit_summary_start = datetime.now(timezone.utc)

    if spark.catalog.tableExists(AUDIT_SUMMARY_TABLE):
        (
            DeltaTable.forName(spark, AUDIT_SUMMARY_TABLE)
            .alias("tgt")
            .merge(
                audit_summary_df.alias("src"),
                "tgt.user_id = src.user_id "
                "AND tgt.activity_date = src.activity_date "
                "AND tgt.application_name = src.application_name",
            )
            .whenMatchedUpdate(
                set={
                    "event_count": "src.event_count",
                    "success_count": "src.success_count",
                    "failure_count": "src.failure_count",
                    "_run_id": "src._run_id",
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        audit_summary_df.write.format("delta").mode("overwrite").saveAsTable(
            AUDIT_SUMMARY_TABLE
        )

    audit_summary_ms = int(
        (datetime.now(timezone.utc) - audit_summary_start).total_seconds() * 1000
    )
    audit_summary_count = audit_summary_df.count()

    write_audit_entry(
        spark,
        run_id=run_id,
        notebook_name=NOTEBOOK_NAME,
        operation="TABLE_WRITE",
        target=AUDIT_SUMMARY_TABLE,
        status="SUCCESS",
        records_affected=audit_summary_count,
        duration_ms=audit_summary_ms,
    )
    print(
        f"[{NOTEBOOK_NAME}] {AUDIT_SUMMARY_TABLE}: merged {audit_summary_count:,} rows "
        f"| Run ID: {run_id}"
    )
