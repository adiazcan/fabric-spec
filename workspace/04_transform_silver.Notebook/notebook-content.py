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
# 04_transform_silver — Silver layer transformations
#
# [US1/T015] User transform:    validate + Delta MERGE bronze_graph_users → silver_users
# [US1/T016] License transform: extract licenseDetails → Delta MERGE silver_user_licenses
# [US2/T020] (Phase 4) Usage transform:  validate + MERGE bronze_graph_usage_reports → silver_copilot_usage
# [US4/T027] (Phase 6) Audit transform:  validate + MERGE bronze_graph_audit_logs → silver_audit_events
#
# Principle III: Schema validation + quarantine of all rejected records (FR-012)
# Principle IV:  Delta MERGE only — no full table refreshes after initial load
# Principle VII: Audit log entry for every read and write operation

# noqa: E402
%run helpers

# CELL ********************

from datetime import datetime, timezone

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    StringType,
    StructField,
    StructType,
)

NOTEBOOK_NAME = "04_transform_silver"

# JSON schema for parsing bronze_graph_users.raw_json
# Mirrors the fields selected in 01_ingest_users via $select + $expand=licenseDetails
_USER_JSON_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("displayName", StringType(), True),
        StructField("mail", StringType(), True),
        StructField("userPrincipalName", StringType(), True),
        StructField("department", StringType(), True),
        StructField("jobTitle", StringType(), True),
        StructField("accountEnabled", BooleanType(), True),
        StructField("companyName", StringType(), True),
        StructField("officeLocation", StringType(), True),
        StructField(
            "licenseDetails",
            ArrayType(
                StructType(
                    [
                        StructField("id", StringType(), True),
                        StructField("skuId", StringType(), True),
                        StructField("skuPartNumber", StringType(), True),
                    ]
                )
            ),
            True,
        ),
    ]
)

# CELL ********************
# ─────────────────────────────────────────────────────────────────────────────
# [US1/T015 + T016] Read latest Bronze partition
# ─────────────────────────────────────────────────────────────────────────────

spark = SparkSession.builder.getOrCreate()
run_id = generate_run_id()

if not spark.catalog.tableExists("bronze_graph_users"):
    raise RuntimeError(
        "bronze_graph_users table does not exist. Run 01_ingest_users first."
    )

read_start = datetime.now(timezone.utc)

latest_date = (
    spark.table("bronze_graph_users")
    .agg(F.max("ingestion_date").alias("max_date"))
    .collect()[0]["max_date"]
)

bronze_df = spark.table("bronze_graph_users").filter(
    F.col("ingestion_date") == latest_date
)
bronze_count = bronze_df.count()

read_ms = int((datetime.now(timezone.utc) - read_start).total_seconds() * 1000)
write_audit_entry(
    spark,
    run_id=run_id,
    notebook_name=NOTEBOOK_NAME,
    operation="TABLE_READ",
    target="bronze_graph_users",
    status="SUCCESS",
    records_affected=bronze_count,
    duration_ms=read_ms,
)

# Parse the raw_json column into a typed struct for downstream column extraction
parsed_df = bronze_df.withColumn(
    "parsed",
    F.from_json(F.col("raw_json"), _USER_JSON_SCHEMA),
)

# CELL ********************
# ─────────────────────────────────────────────────────────────────────────────
# [US1/T015] Silver user transform: validate + MERGE → silver_users
# ─────────────────────────────────────────────────────────────────────────────

users_df = parsed_df.select(
    F.col("parsed.id").alias("user_id"),
    F.col("parsed.userPrincipalName").alias("user_principal_name"),
    F.col("parsed.displayName").alias("display_name"),
    F.col("parsed.mail").alias("mail"),
    F.col("parsed.department").alias("department"),
    F.col("parsed.jobTitle").alias("job_title"),
    F.col("parsed.companyName").alias("company_name"),
    F.col("parsed.officeLocation").alias("office_location"),
    F.col("parsed.accountEnabled").alias("account_enabled"),
    # is_departed derived from account_enabled (FR-014): true when account disabled
    (~F.col("parsed.accountEnabled")).alias("is_departed"),
    F.col("ingestion_date").alias("_ingestion_date"),
    F.lit(run_id).alias("_run_id"),
)

# Schema validation: required fields per data-model.md silver_users definition
users_valid_df, users_rejected = validate_schema(
    users_df,
    required_fields=["user_id", "user_principal_name", "display_name", "account_enabled"],
    type_checks={"user_id": "string", "account_enabled": "boolean"},
)

# Quarantine any records failing validation (Principle III, FR-012)
quarantine_rejected(
    spark,
    run_id=run_id,
    source_table="bronze_graph_users",
    rejection_metadata=users_rejected,
    quarantine_table="silver_users_rejected",
)

# Delta MERGE into silver_users on user_id (natural key)
# Insert on no match; update all fields on match (latest state wins)
users_merge_start = datetime.now(timezone.utc)

if spark.catalog.tableExists("silver_users"):
    (
        DeltaTable.forName(spark, "silver_users")
        .alias("tgt")
        .merge(users_valid_df.alias("src"), "tgt.user_id = src.user_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    # First run: create table via write (no existing table to merge into)
    users_valid_df.write.format("delta").mode("overwrite").saveAsTable("silver_users")

users_merge_ms = int((datetime.now(timezone.utc) - users_merge_start).total_seconds() * 1000)
users_count = users_valid_df.count()

write_audit_entry(
    spark,
    run_id=run_id,
    notebook_name=NOTEBOOK_NAME,
    operation="TABLE_WRITE",
    target="silver_users",
    status="SUCCESS",
    records_affected=users_count,
    duration_ms=users_merge_ms,
)
print(
    f"[{NOTEBOOK_NAME}] silver_users: merged {users_count:,} rows "
    f"(rejected: {len(users_rejected)})"
)

# CELL ********************
# ─────────────────────────────────────────────────────────────────────────────
# [US1/T016] Silver license transform: extract licenseDetails → silver_user_licenses
#
# Copilot SKU detection: case-insensitive match on 'MICROSOFT_365_COPILOT'
# license_assignment_date: earliest observed date for Copilot license (retained in MERGE)
# license_removal_date:   set when Copilot SKU transitions from present → absent
# ─────────────────────────────────────────────────────────────────────────────

licenses_df = (
    parsed_df.select(
        F.col("parsed.id").alias("user_id"),
        F.col("parsed.licenseDetails").alias("license_details"),
        F.col("ingestion_date").alias("_ingestion_date"),
        F.lit(run_id).alias("_run_id"),
    )
    .withColumn(
        "has_copilot_license",
        F.expr(
            "exists(license_details, x -> upper(x.skuPartNumber) = 'MICROSOFT_365_COPILOT')"
        ),
    )
    .withColumn(
        "copilot_sku_id",
        F.expr(
            "CASE WHEN exists(license_details, x -> upper(x.skuPartNumber) = 'MICROSOFT_365_COPILOT') "
            "THEN filter(license_details, x -> upper(x.skuPartNumber) = 'MICROSOFT_365_COPILOT')[0].skuId "
            "ELSE NULL END"
        ),
    )
    .withColumn(
        # Collect all assigned SKU part numbers as an array
        "all_license_skus",
        F.expr(
            "CASE WHEN license_details IS NOT NULL "
            "THEN transform(license_details, x -> x.skuPartNumber) "
            "ELSE array() END"
        ),
    )
    .withColumn(
        # Assignment date defaults to today on first detection; MERGE retains the earliest
        "license_assignment_date",
        F.when(
            F.col("has_copilot_license"),
            F.col("_ingestion_date"),
        ).otherwise(F.lit(None).cast(DateType())),
    )
    .withColumn(
        # Removal date is managed entirely in the MERGE whenMatchedUpdate logic below
        "license_removal_date",
        F.lit(None).cast(DateType()),
    )
    .select(
        "user_id",
        "has_copilot_license",
        "copilot_sku_id",
        "license_assignment_date",
        "license_removal_date",
        "all_license_skus",
        "_ingestion_date",
        "_run_id",
    )
    .filter(F.col("user_id").isNotNull())
)

# Delta MERGE into silver_user_licenses with special date-retention logic:
#   - license_assignment_date: LEAST(tgt, src) when both have license (retain earliest)
#   - license_removal_date:    set to current_date() when tgt had license but src does not
lic_merge_start = datetime.now(timezone.utc)

if spark.catalog.tableExists("silver_user_licenses"):
    (
        DeltaTable.forName(spark, "silver_user_licenses")
        .alias("tgt")
        .merge(licenses_df.alias("src"), "tgt.user_id = src.user_id")
        .whenMatchedUpdate(
            set={
                "has_copilot_license": "src.has_copilot_license",
                "copilot_sku_id": "src.copilot_sku_id",
                # Retain the earliest observed assignment date (FR per data-model.md)
                "license_assignment_date": """
                    CASE
                        WHEN tgt.has_copilot_license AND src.has_copilot_license
                            THEN LEAST(tgt.license_assignment_date, src.license_assignment_date)
                        WHEN src.has_copilot_license THEN src.license_assignment_date
                        ELSE tgt.license_assignment_date
                    END
                """,
                # Stamp removal date when license transitions active → removed
                "license_removal_date": """
                    CASE
                        WHEN tgt.has_copilot_license AND NOT src.has_copilot_license
                            THEN current_date()
                        WHEN src.has_copilot_license THEN NULL
                        ELSE tgt.license_removal_date
                    END
                """,
                "all_license_skus": "src.all_license_skus",
                "_ingestion_date": "src._ingestion_date",
                "_run_id": "src._run_id",
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    licenses_df.write.format("delta").mode("overwrite").saveAsTable(
        "silver_user_licenses"
    )

lic_merge_ms = int((datetime.now(timezone.utc) - lic_merge_start).total_seconds() * 1000)
lic_count = licenses_df.count()

write_audit_entry(
    spark,
    run_id=run_id,
    notebook_name=NOTEBOOK_NAME,
    operation="TABLE_WRITE",
    target="silver_user_licenses",
    status="SUCCESS",
    records_affected=lic_count,
    duration_ms=lic_merge_ms,
)
print(f"[{NOTEBOOK_NAME}] silver_user_licenses: merged {lic_count:,} rows")
