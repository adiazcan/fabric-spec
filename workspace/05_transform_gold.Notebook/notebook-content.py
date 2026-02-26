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
