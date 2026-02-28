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
# [US5/T032] Data freshness gate (FR-013)
#
# Before Silver processing, check max(ingestion_date) of each Bronze table
# against current date. If stale (> 48 hours), log WARNING to both
# gold_copilot_dq_results and gold_copilot_audit_log. Proceed with available data.
# ─────────────────────────────────────────────────────────────────────────────

from datetime import date as _date_type

spark = SparkSession.builder.getOrCreate()
run_id = generate_run_id()
params = load_parameters()
_FRESHNESS_HOURS: int = params["freshness_threshold_hours"]  # 48

_BRONZE_TABLES = [
    "bronze_graph_users",
    "bronze_graph_usage_reports",
    "bronze_graph_audit_logs",
]

for _btable in _BRONZE_TABLES:
    if not spark.catalog.tableExists(_btable):
        continue

    _max_date = (
        spark.table(_btable)
        .agg(F.max("ingestion_date").alias("max_date"))
        .collect()[0]["max_date"]
    )

    if _max_date is None:
        continue

    _hours_stale = (_date_type.today() - _max_date).days * 24

    if _hours_stale > _FRESHNESS_HOURS:
        write_dq_result(
            spark,
            run_id=run_id,
            table_name=_btable,
            check_name="bronze_freshness_gate",
            check_result="WARN",
            metric_value=float(_hours_stale),
            threshold=float(_FRESHNESS_HOURS),
            details=(
                f"Bronze table {_btable} is stale: max ingestion_date={_max_date}, "
                f"hours_stale={_hours_stale} (threshold={_FRESHNESS_HOURS}h). "
                "Proceeding with available data."
            ),
        )
        write_audit_entry(
            spark,
            run_id=run_id,
            notebook_name=NOTEBOOK_NAME,
            operation="TABLE_READ",
            target=_btable,
            status="WARNING",
            error_message=(
                f"FR-013 freshness gate: {_btable} stale by {_hours_stale}h "
                f"(threshold={_FRESHNESS_HOURS}h) — proceeding with available data"
            ),
        )
        print(
            f"[{NOTEBOOK_NAME}] WARNING: {_btable} stale by {_hours_stale}h "
            f"(threshold={_FRESHNESS_HOURS}h) — proceeding with available data"
        )

# CELL ********************
# ─────────────────────────────────────────────────────────────────────────────
# [US1/T015 + T016] Read latest Bronze partition
# ─────────────────────────────────────────────────────────────────────────────

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

# CELL ********************
# ─────────────────────────────────────────────────────────────────────────────
# [US2/T020] Silver usage transform: validate + MERGE → silver_copilot_usage
#
# Reads the latest Bronze partition from bronze_graph_usage_reports, parses
# raw_json, maps API fields to Silver columns per the contract field mapping,
# validates required fields, quarantines rejected records, and Delta MERGEs into
# silver_copilot_usage on the composite key (user_principal_name + report_refresh_date).
# ─────────────────────────────────────────────────────────────────────────────

if not spark.catalog.tableExists("bronze_graph_usage_reports"):
    raise RuntimeError(
        "bronze_graph_usage_reports table does not exist. Run 02_ingest_usage first."
    )

# JSON schema for parsing bronze_graph_usage_reports.raw_json
# All date fields stored as strings in Bronze (API returns ISO-8601 date strings)
# reportPeriod stored as StringType — integer 7 coerces to "7" via from_json
_USAGE_JSON_SCHEMA = StructType(
    [
        StructField("reportRefreshDate", StringType(), True),
        StructField("reportPeriod", StringType(), True),
        StructField("userPrincipalName", StringType(), True),
        StructField("lastActivityDate", StringType(), True),
        StructField("microsoftTeamsCopilotLastActivityDate", StringType(), True),
        StructField("wordCopilotLastActivityDate", StringType(), True),
        StructField("excelCopilotLastActivityDate", StringType(), True),
        StructField("powerPointCopilotLastActivityDate", StringType(), True),
        StructField("outlookCopilotLastActivityDate", StringType(), True),
        StructField("oneNoteCopilotLastActivityDate", StringType(), True),
        StructField("loopCopilotLastActivityDate", StringType(), True),
        StructField("copilotChatLastActivityDate", StringType(), True),
    ]
)

usage_read_start = datetime.now(timezone.utc)

latest_usage_date = (
    spark.table("bronze_graph_usage_reports")
    .agg(F.max("ingestion_date").alias("max_date"))
    .collect()[0]["max_date"]
)

bronze_usage_df = spark.table("bronze_graph_usage_reports").filter(
    F.col("ingestion_date") == latest_usage_date
)
bronze_usage_count = bronze_usage_df.count()

usage_read_ms = int((datetime.now(timezone.utc) - usage_read_start).total_seconds() * 1000)
write_audit_entry(
    spark,
    run_id=run_id,
    notebook_name=NOTEBOOK_NAME,
    operation="TABLE_READ",
    target="bronze_graph_usage_reports",
    status="SUCCESS",
    records_affected=bronze_usage_count,
    duration_ms=usage_read_ms,
)

# Parse raw_json and map to Silver schema per contract field mapping
parsed_usage_df = bronze_usage_df.withColumn(
    "parsed",
    F.from_json(F.col("raw_json"), _USAGE_JSON_SCHEMA),
)

usage_silver_df = parsed_usage_df.select(
    F.col("parsed.userPrincipalName").alias("user_principal_name"),
    # Cast date string to DATE; returns null for empty/missing strings (null-safe)
    F.to_date(F.col("parsed.reportRefreshDate"), "yyyy-MM-dd").alias("report_refresh_date"),
    # Prefix period value with "D" → "D7" per contract field mapping
    F.concat(F.lit("D"), F.col("parsed.reportPeriod")).alias("report_period"),
    F.to_date(F.col("parsed.lastActivityDate"), "yyyy-MM-dd").alias("last_activity_date"),
    F.to_date(
        F.col("parsed.microsoftTeamsCopilotLastActivityDate"), "yyyy-MM-dd"
    ).alias("teams_last_activity_date"),
    F.to_date(F.col("parsed.wordCopilotLastActivityDate"), "yyyy-MM-dd").alias(
        "word_last_activity_date"
    ),
    F.to_date(F.col("parsed.excelCopilotLastActivityDate"), "yyyy-MM-dd").alias(
        "excel_last_activity_date"
    ),
    F.to_date(F.col("parsed.powerPointCopilotLastActivityDate"), "yyyy-MM-dd").alias(
        "powerpoint_last_activity_date"
    ),
    F.to_date(F.col("parsed.outlookCopilotLastActivityDate"), "yyyy-MM-dd").alias(
        "outlook_last_activity_date"
    ),
    F.to_date(F.col("parsed.oneNoteCopilotLastActivityDate"), "yyyy-MM-dd").alias(
        "onenote_last_activity_date"
    ),
    F.to_date(F.col("parsed.loopCopilotLastActivityDate"), "yyyy-MM-dd").alias(
        "loop_last_activity_date"
    ),
    F.to_date(F.col("parsed.copilotChatLastActivityDate"), "yyyy-MM-dd").alias(
        "copilot_chat_last_activity_date"
    ),
    F.col("ingestion_date").alias("_ingestion_date"),
    F.lit(run_id).alias("_run_id"),
)

# Schema validation: required composite key fields
usage_valid_df, usage_rejected = validate_schema(
    usage_silver_df,
    required_fields=["user_principal_name", "report_refresh_date"],
    type_checks={"user_principal_name": "string"},
)

# Quarantine invalid records (Principle III, FR-012)
quarantine_rejected(
    spark,
    run_id=run_id,
    source_table="bronze_graph_usage_reports",
    rejection_metadata=usage_rejected,
    quarantine_table="silver_usage_rejected",
)

# Delta MERGE into silver_copilot_usage on composite key (upn + report_refresh_date)
# whenMatchedUpdate: refresh all columns with latest data
# whenNotMatchedInsert: insert new user-report rows
usage_merge_start = datetime.now(timezone.utc)

if spark.catalog.tableExists("silver_copilot_usage"):
    (
        DeltaTable.forName(spark, "silver_copilot_usage")
        .alias("tgt")
        .merge(
            usage_valid_df.alias("src"),
            "tgt.user_principal_name = src.user_principal_name "
            "AND tgt.report_refresh_date = src.report_refresh_date",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    # First run: create table via write
    usage_valid_df.write.format("delta").mode("overwrite").saveAsTable(
        "silver_copilot_usage"
    )

usage_merge_ms = int(
    (datetime.now(timezone.utc) - usage_merge_start).total_seconds() * 1000
)
usage_count = usage_valid_df.count()

write_audit_entry(
    spark,
    run_id=run_id,
    notebook_name=NOTEBOOK_NAME,
    operation="TABLE_WRITE",
    target="silver_copilot_usage",
    status="SUCCESS",
    records_affected=usage_count,
    duration_ms=usage_merge_ms,
)
print(
    f"[{NOTEBOOK_NAME}] silver_copilot_usage: merged {usage_count:,} rows "
    f"(rejected: {len(usage_rejected)})"
)

# CELL ********************
# ─────────────────────────────────────────────────────────────────────────────
# [US4/T027] Silver audit transform: validate + MERGE → silver_audit_events
#
# Reads the latest Bronze partition from bronze_graph_audit_logs, parses
# raw_json, maps API fields to Silver columns per the contract field mapping
# (contracts/graph-audit-logs.md), validates required fields, quarantines
# rejected records, and Delta MERGEs into silver_audit_events on event_id
# (insert-only — audit events are immutable; skip existing event_id values).
# ─────────────────────────────────────────────────────────────────────────────

if not spark.catalog.tableExists("bronze_graph_audit_logs"):
    raise RuntimeError(
        "bronze_graph_audit_logs table does not exist. Run 03_ingest_audit_logs first."
    )

# JSON schema for parsing bronze_graph_audit_logs.raw_json
_AUDIT_JSON_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("createdDateTime", StringType(), True),
        StructField("operation", StringType(), True),
        StructField("service", StringType(), True),
        StructField("auditLogRecordType", StringType(), True),
        StructField("userPrincipalName", StringType(), True),
        StructField("userId", StringType(), True),
        StructField("userType", StringType(), True),
        StructField("clientIp", StringType(), True),
        StructField("objectId", StringType(), True),
        StructField("organizationId", StringType(), True),
        StructField("auditData", StringType(), True),
    ]
)

audit_read_start = datetime.now(timezone.utc)

latest_audit_date = (
    spark.table("bronze_graph_audit_logs")
    .agg(F.max("ingestion_date").alias("max_date"))
    .collect()[0]["max_date"]
)

bronze_audit_df = spark.table("bronze_graph_audit_logs").filter(
    F.col("ingestion_date") == latest_audit_date
)
bronze_audit_count = bronze_audit_df.count()

audit_read_ms = int(
    (datetime.now(timezone.utc) - audit_read_start).total_seconds() * 1000
)
write_audit_entry(
    spark,
    run_id=run_id,
    notebook_name=NOTEBOOK_NAME,
    operation="TABLE_READ",
    target="bronze_graph_audit_logs",
    status="SUCCESS",
    records_affected=bronze_audit_count,
    duration_ms=audit_read_ms,
)

# Parse raw_json and map to Silver schema per contract field mapping
parsed_audit_df = bronze_audit_df.withColumn(
    "parsed",
    F.from_json(F.col("raw_json"), _AUDIT_JSON_SCHEMA),
)

audit_silver_df = parsed_audit_df.select(
    F.col("parsed.id").alias("event_id"),
    # Cast datetime string to TIMESTAMP
    F.to_timestamp(F.col("parsed.createdDateTime")).alias("created_date_time"),
    F.col("parsed.operation").alias("operation"),
    F.col("parsed.service").alias("service"),
    F.col("parsed.auditLogRecordType").alias("record_type"),
    F.col("parsed.userPrincipalName").alias("user_principal_name"),
    F.col("parsed.userId").alias("user_id"),
    F.col("parsed.userType").alias("user_type"),
    F.col("parsed.clientIp").alias("client_ip"),
    # Null-safe: objectId may be null
    F.col("parsed.objectId").alias("object_id"),
    F.col("parsed.organizationId").alias("organization_id"),
    # auditData already serialized as JSON string in Bronze
    F.col("parsed.auditData").alias("audit_data_json"),
    F.col("ingestion_date").alias("_ingestion_date"),
    F.lit(run_id).alias("_run_id"),
)

# Schema validation: required fields per data-model.md silver_audit_events
audit_valid_df, audit_rejected = validate_schema(
    audit_silver_df,
    required_fields=["event_id", "created_date_time", "operation", "service", "record_type"],
    type_checks={"event_id": "string"},
)

# Quarantine invalid records (Principle III, FR-012)
quarantine_rejected(
    spark,
    run_id=run_id,
    source_table="bronze_graph_audit_logs",
    rejection_metadata=audit_rejected,
    quarantine_table="silver_audit_rejected",
)

# Delta MERGE into silver_audit_events on event_id
# Insert-only: audit events are immutable — skip existing event_id values
audit_merge_start = datetime.now(timezone.utc)

if spark.catalog.tableExists("silver_audit_events"):
    (
        DeltaTable.forName(spark, "silver_audit_events")
        .alias("tgt")
        .merge(audit_valid_df.alias("src"), "tgt.event_id = src.event_id")
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    # First run: create table via write
    audit_valid_df.write.format("delta").mode("overwrite").saveAsTable(
        "silver_audit_events"
    )

audit_merge_ms = int(
    (datetime.now(timezone.utc) - audit_merge_start).total_seconds() * 1000
)
audit_count = audit_valid_df.count()

write_audit_entry(
    spark,
    run_id=run_id,
    notebook_name=NOTEBOOK_NAME,
    operation="TABLE_WRITE",
    target="silver_audit_events",
    status="SUCCESS",
    records_affected=audit_count,
    duration_ms=audit_merge_ms,
)
print(
    f"[{NOTEBOOK_NAME}] silver_audit_events: merged {audit_count:,} rows "
    f"(rejected: {len(audit_rejected)})"
)
