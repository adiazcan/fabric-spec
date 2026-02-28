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
# 06_compute_scores — Recency-weighted usage score computation (US3)
#
# [US3/T023] Compute per-user usage scores (0–100) with three weighted components:
#   - recency_score  (50%): based on days_since_last_activity
#   - frequency_score (30%): based on active_days_in_period (last 30 days)
#   - breadth_score   (20%): based on apps_used_count (out of 8 Copilot apps)
#
# Output: insert-only rows into gold_copilot_usage_scores (one per user per score_date)
# Flags:  is_underutilized (score ≤ threshold AND license ≥ 14 days)
#         is_new_assignment (license < 14 days — grace period, FR-010)
#
# Principle IV:  Delta Lake insert-only fact table — historical scores never updated
# Principle VII: Audit log entries for every write operation

# noqa: E402
%run helpers

# CELL ********************

from datetime import datetime, timezone
from functools import reduce

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, Window

NOTEBOOK_NAME = "06_compute_scores"
SCORES_TABLE = "gold_copilot_usage_scores"

# Number of Copilot-enabled apps tracked for breadth score
TOTAL_COPILOT_APPS = 8

# Per-app last-activity columns in silver_copilot_usage (8 apps)
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

spark = SparkSession.builder.getOrCreate()
run_id = generate_run_id()
params = load_parameters()

# Configurable thresholds from parameter.yml
UNDERUTILIZATION_THRESHOLD: int = params["underutilization_threshold"]  # default 20
NEW_ASSIGNMENT_GRACE_DAYS: int = params["new_assignment_grace_days"]  # default 14

# Score component weights (FR-008)
RECENCY_WEIGHT = 0.50
FREQUENCY_WEIGHT = 0.30
BREADTH_WEIGHT = 0.20

# CELL ********************
# ─────────────────────────────────────────────────────────────────────────────
# Prerequisite checks
# ─────────────────────────────────────────────────────────────────────────────

if not spark.catalog.tableExists("silver_copilot_usage"):
    raise RuntimeError(
        "silver_copilot_usage table does not exist. "
        "Run 04_transform_silver (US2/T020) first."
    )
if not spark.catalog.tableExists("silver_user_licenses"):
    raise RuntimeError(
        "silver_user_licenses table does not exist. "
        "Run 04_transform_silver (US1/T016) first."
    )

# CELL ********************
# ─────────────────────────────────────────────────────────────────────────────
# [US3/T023] Score computation
# ─────────────────────────────────────────────────────────────────────────────

score_date = F.current_date()

# ── 1. Get licensed users with license tenure ────────────────────────────────
licenses_df = (
    spark.table("silver_user_licenses")
    .filter(F.col("has_copilot_license") == True)  # noqa: E712
    .select(
        "user_id",
        "license_assignment_date",
    )
    .withColumn(
        "license_days_held",
        F.when(
            F.col("license_assignment_date").isNotNull(),
            F.datediff(F.current_date(), F.col("license_assignment_date")),
        ).otherwise(F.lit(0)),
    )
)

# ── 2. Get latest usage record per user (most recent report_refresh_date) ────
# Join silver_copilot_usage with silver_users to get user_id from UPN
users_df = spark.table("silver_users").select("user_id", "user_principal_name")

usage_w = Window.partitionBy("user_principal_name").orderBy(
    F.desc("report_refresh_date")
)
latest_usage_df = (
    spark.table("silver_copilot_usage")
    .withColumn("_rnk", F.rank().over(usage_w))
    .filter(F.col("_rnk") == 1)
    .drop("_rnk")
)

# Compute apps_used_count: count of non-null per-app last-activity columns
apps_used_expr = reduce(
    lambda a, b: a + b,
    [
        F.when(F.col(c).isNotNull(), F.lit(1)).otherwise(F.lit(0))
        for c in _APP_ACTIVITY_COLS
    ],
)

latest_usage_with_id_df = (
    latest_usage_df.alias("u")
    .join(users_df.alias("uid"), on="user_principal_name", how="inner")
    .select(
        F.col("uid.user_id"),
        F.col("u.last_activity_date"),
        apps_used_expr.alias("apps_used_count"),
    )
)

# ── 3. Compute active_days_in_period (distinct active days in last 30 days) ──
# Count distinct report_refresh_date values per user where last_activity_date changed
# (i.e., activity was recorded) within the last 30 days
thirty_days_ago = F.date_sub(F.current_date(), 30)

active_days_df = (
    spark.table("silver_copilot_usage")
    .join(users_df, on="user_principal_name", how="inner")
    .filter(
        F.col("report_refresh_date") >= thirty_days_ago
    )
    .filter(F.col("last_activity_date").isNotNull())
    .groupBy("user_id")
    .agg(
        F.countDistinct("report_refresh_date").alias("active_days_in_period"),
    )
)

# ── 4. Assemble score inputs for all licensed users ──────────────────────────
# Left join: all licensed users get a score row (even those with zero activity)
score_inputs_df = (
    licenses_df.alias("lic")
    .join(latest_usage_with_id_df.alias("usg"), on="user_id", how="left")
    .join(active_days_df.alias("ad"), on="user_id", how="left")
    .select(
        F.col("lic.user_id"),
        F.col("lic.license_assignment_date"),
        F.col("lic.license_days_held"),
        # days_since_last_activity: null → treat as max (no activity)
        F.when(
            F.col("usg.last_activity_date").isNotNull(),
            F.datediff(F.current_date(), F.col("usg.last_activity_date")),
        )
        .otherwise(F.lit(None).cast("int"))
        .alias("days_since_last_activity"),
        F.coalesce(F.col("ad.active_days_in_period"), F.lit(0)).alias(
            "active_days_in_period"
        ),
        F.coalesce(F.col("usg.apps_used_count"), F.lit(0)).alias("apps_used_count"),
    )
)

# ── 5. Calculate score components (FR-008) ───────────────────────────────────
# recency_score = max(0, 100 − (days_since_last_activity × 100 / 30)) clamped [0, 100]
# frequency_score = min(100, active_days_in_period × 100 / 30)
# breadth_score = apps_used_count × 100 / 8
# Final: usage_score = round(0.50 × recency + 0.30 × frequency + 0.20 × breadth)

scored_df = score_inputs_df.select(
    F.col("user_id"),
    score_date.alias("score_date"),
    # Recency score: users with no activity get 0
    F.when(
        F.col("days_since_last_activity").isNull(),
        F.lit(0.0),
    )
    .otherwise(
        F.greatest(
            F.lit(0.0),
            F.least(
                F.lit(100.0),
                F.lit(100.0) - (F.col("days_since_last_activity") * 100.0 / 30.0),
            ),
        )
    )
    .alias("recency_score"),
    # Frequency score
    F.least(
        F.lit(100.0),
        F.col("active_days_in_period") * 100.0 / 30.0,
    ).alias("frequency_score"),
    # Breadth score
    F.least(
        F.lit(100.0),
        F.col("apps_used_count") * 100.0 / F.lit(TOTAL_COPILOT_APPS).cast("double"),
    ).alias("breadth_score"),
    # Raw inputs for auditability
    F.col("days_since_last_activity"),
    F.col("active_days_in_period"),
    F.col("apps_used_count"),
    F.col("license_days_held"),
)

# Final composite score and flags
final_df = scored_df.select(
    "user_id",
    "score_date",
    # usage_score = round(weighted sum)
    F.round(
        F.col("recency_score") * F.lit(RECENCY_WEIGHT)
        + F.col("frequency_score") * F.lit(FREQUENCY_WEIGHT)
        + F.col("breadth_score") * F.lit(BREADTH_WEIGHT)
    )
    .cast("int")
    .alias("usage_score"),
    "recency_score",
    "frequency_score",
    "breadth_score",
    # Store weights for reproducibility
    F.lit(RECENCY_WEIGHT).alias("recency_weight"),
    F.lit(FREQUENCY_WEIGHT).alias("frequency_weight"),
    F.lit(BREADTH_WEIGHT).alias("breadth_weight"),
    "days_since_last_activity",
    "active_days_in_period",
    "apps_used_count",
    # is_underutilized: score ≤ threshold AND license held ≥ 14 days
    (
        (
            F.round(
                F.col("recency_score") * F.lit(RECENCY_WEIGHT)
                + F.col("frequency_score") * F.lit(FREQUENCY_WEIGHT)
                + F.col("breadth_score") * F.lit(BREADTH_WEIGHT)
            ).cast("int")
            <= UNDERUTILIZATION_THRESHOLD
        )
        & (F.col("license_days_held") >= NEW_ASSIGNMENT_GRACE_DAYS)
    ).alias("is_underutilized"),
    # is_new_assignment: license held < 14 days (FR-010 grace period)
    (F.col("license_days_held") < NEW_ASSIGNMENT_GRACE_DAYS).alias(
        "is_new_assignment"
    ),
    F.lit(run_id).alias("_run_id"),
)

# CELL ********************
# ─────────────────────────────────────────────────────────────────────────────
# Write to gold_copilot_usage_scores (insert-only — one row per user per day)
# ─────────────────────────────────────────────────────────────────────────────

insert_start = datetime.now(timezone.utc)

if spark.catalog.tableExists(SCORES_TABLE):
    # MERGE to enforce one-row-per-user-per-score_date (idempotent on reruns)
    (
        DeltaTable.forName(spark, SCORES_TABLE)
        .alias("tgt")
        .merge(
            final_df.alias("src"),
            "tgt.user_id = src.user_id AND tgt.score_date = src.score_date",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    final_df.write.format("delta").mode("overwrite").saveAsTable(SCORES_TABLE)

insert_ms = int((datetime.now(timezone.utc) - insert_start).total_seconds() * 1000)
score_count = final_df.count()

write_audit_entry(
    spark,
    run_id=run_id,
    notebook_name=NOTEBOOK_NAME,
    operation="TABLE_WRITE",
    target=SCORES_TABLE,
    status="SUCCESS",
    records_affected=score_count,
    duration_ms=insert_ms,
)
print(
    f"[{NOTEBOOK_NAME}] {SCORES_TABLE}: inserted {score_count:,} score rows "
    f"for {F.current_date()} | Run ID: {run_id}"
)
