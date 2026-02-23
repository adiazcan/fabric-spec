# Fabric notebook source

# METADATA **{"version":"0.9","dependencies":{"spark_connector":{},"lakehouse":{"default_lakehouse_name":"copilot_analytics","known_lakehouses":[]}}}

# CELL **{"type":"code","source":"attached"}

import os

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F
from delta.tables import DeltaTable


spark = SparkSession.builder.getOrCreate()


# CELL **{"type":"code","source":"attached"}

def _read_yaml(path):
    with open(path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def load_config():
    candidates = [
        "/lakehouse/default/Files/config/parameter.yml",
        "config/parameter.yml",
        "../config/parameter.yml",
    ]
    for candidate in candidates:
        if os.path.exists(candidate):
            return _read_yaml(candidate)
    raise FileNotFoundError("parameter.yml was not found in expected locations")


def merge_into(table_name, source_df, merge_condition):
    if not spark.catalog.tableExists(table_name):
        source_df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        return
    target = DeltaTable.forName(spark, table_name)
    (
        target.alias("t")
        .merge(source_df.alias("s"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def transform_users():
    bronze = spark.table("bronze_users")
    window_spec = Window.partitionBy("id").orderBy(F.col("_ingested_at").desc())
    latest = (
        bronze.withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )
    silver = (
        latest.select(
            F.col("id").alias("user_id"),
            F.col("userPrincipalName").alias("user_principal_name"),
            F.col("displayName").alias("display_name"),
            F.col("mail").alias("email"),
            F.col("department").alias("department"),
            F.col("officeLocation").alias("office_location"),
            F.col("jobTitle").alias("job_title"),
            F.coalesce(F.col("accountEnabled"), F.lit(True)).alias("is_enabled"),
            F.lit(False).alias("is_deleted"),
            F.col("_ingested_at").alias("first_seen_at"),
            F.current_timestamp().alias("last_updated_at"),
        )
        .filter(F.col("user_id").isNotNull() & F.col("user_principal_name").isNotNull())
    )
    merge_into("silver_users", silver, "t.user_id = s.user_id")


def transform_licenses(copilot_sku_ids):
    bronze = spark.table("bronze_licenses")
    window_spec = Window.partitionBy("userId", "skuId").orderBy(F.col("_ingested_at").desc())
    latest = (
        bronze.withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )
    silver = latest.select(
        F.col("userId").alias("user_id"),
        F.col("skuId").alias("sku_id"),
        F.col("skuId").alias("sku_name"),
        F.col("skuId").isin(copilot_sku_ids).alias("is_copilot_license"),
        F.to_date(F.col("_ingested_at")).alias("assigned_date"),
        F.lit(None).cast("date").alias("removed_date"),
        F.lit(True).alias("is_active"),
        F.current_timestamp().alias("last_updated_at"),
    ).filter(F.col("user_id").isNotNull() & F.col("sku_id").isNotNull())
    merge_into("silver_licenses", silver, "t.user_id = s.user_id AND t.sku_id = s.sku_id")


def transform_copilot_usage():
    bronze = spark.table("bronze_usage_reports")
    app_cols = [
        "teamsLastActivityDate",
        "wordLastActivityDate",
        "excelLastActivityDate",
        "powerPointLastActivityDate",
        "outlookLastActivityDate",
        "oneNoteLastActivityDate",
        "loopLastActivityDate",
        "copilotChatLastActivityDate",
    ]

    apps_used_count_expr = None
    for col_name in app_cols:
        is_active = F.when(F.col(col_name).isNotNull() & (F.col(col_name) != ""), 1).otherwise(0)
        apps_used_count_expr = is_active if apps_used_count_expr is None else apps_used_count_expr + is_active

    silver = bronze.select(
        F.col("userPrincipalName").alias("user_principal_name"),
        F.to_date("reportRefreshDate").alias("report_refresh_date"),
        F.col("reportPeriod").alias("report_period"),
        F.to_date("lastActivityDate").alias("last_activity_date"),
        F.to_date("teamsLastActivityDate").alias("teams_last_activity"),
        F.to_date("wordLastActivityDate").alias("word_last_activity"),
        F.to_date("excelLastActivityDate").alias("excel_last_activity"),
        F.to_date("powerPointLastActivityDate").alias("powerpoint_last_activity"),
        F.to_date("outlookLastActivityDate").alias("outlook_last_activity"),
        F.to_date("oneNoteLastActivityDate").alias("onenote_last_activity"),
        F.to_date("loopLastActivityDate").alias("loop_last_activity"),
        F.to_date("copilotChatLastActivityDate").alias("copilot_chat_last_activity"),
        apps_used_count_expr.cast("int").alias("apps_used_count"),
        F.current_timestamp().alias("last_updated_at"),
    ).filter(F.col("user_principal_name").isNotNull() & F.col("report_refresh_date").isNotNull())
    merge_into(
        "silver_copilot_usage",
        silver,
        "t.user_principal_name = s.user_principal_name AND t.report_refresh_date = s.report_refresh_date",
    )


def transform_copilot_interactions():
    bronze = spark.table("bronze_audit_logs")
    window_spec = Window.partitionBy("id").orderBy(F.col("_ingested_at").desc())
    latest = (
        bronze.withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )
    silver = latest.select(
        F.col("id").alias("event_id"),
        F.col("userId").alias("user_id"),
        F.col("userPrincipalName").alias("user_principal_name"),
        F.to_timestamp("createdDateTime").alias("event_timestamp"),
        F.to_date(F.to_timestamp("createdDateTime")).alias("event_date"),
        F.col("operation").alias("operation"),
        F.get_json_object("auditData", "$.AppHost").alias("app_host"),
        F.col("service").alias("workload"),
        F.size(F.from_json(F.get_json_object("auditData", "$.AccessedResources"), "array<map<string,string>>")).alias(
            "accessed_resources_count"
        ),
        F.current_timestamp().alias("last_updated_at"),
    ).filter(F.col("event_id").isNotNull() & F.col("operation").isNotNull())
    merge_into("silver_copilot_interactions", silver, "t.event_id = s.event_id")


def transform_m365_activity():
    bronze = spark.table("bronze_m365_activity")
    service_cols = [
        "exchangeLastActivityDate",
        "oneDriveLastActivityDate",
        "sharePointLastActivityDate",
        "teamsLastActivityDate",
        "wordLastActivityDate",
        "excelLastActivityDate",
        "powerPointLastActivityDate",
    ]

    active_services_expr = None
    for col_name in service_cols:
        is_active = F.when(F.col(col_name).isNotNull() & (F.col(col_name) != ""), 1).otherwise(0)
        active_services_expr = is_active if active_services_expr is None else active_services_expr + is_active

    silver = bronze.select(
        F.col("userPrincipalName").alias("user_principal_name"),
        F.to_date("reportRefreshDate").alias("report_refresh_date"),
        F.col("reportPeriod").alias("report_period"),
        F.to_date("exchangeLastActivityDate").alias("exchange_last_activity"),
        F.to_date("oneDriveLastActivityDate").alias("onedrive_last_activity"),
        F.to_date("sharePointLastActivityDate").alias("sharepoint_last_activity"),
        F.to_date("teamsLastActivityDate").alias("teams_last_activity"),
        F.to_date("wordLastActivityDate").alias("word_last_activity"),
        F.to_date("excelLastActivityDate").alias("excel_last_activity"),
        F.to_date("powerPointLastActivityDate").alias("powerpoint_last_activity"),
        active_services_expr.cast("int").alias("active_services_count"),
        F.current_timestamp().alias("last_updated_at"),
    ).filter(F.col("user_principal_name").isNotNull() & F.col("report_refresh_date").isNotNull())
    merge_into(
        "silver_m365_activity",
        silver,
        "t.user_principal_name = s.user_principal_name AND t.report_refresh_date = s.report_refresh_date",
    )


# CELL **{"type":"code","source":"attached"}

def main():
    config = load_config()
    transform_users()
    transform_licenses(config.get("copilot_sku_ids", []))
    transform_copilot_usage()
    transform_copilot_interactions()
    transform_m365_activity()


if __name__ == "__main__":
    main()
