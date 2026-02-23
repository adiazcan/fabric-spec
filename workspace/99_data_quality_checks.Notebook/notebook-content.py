# Fabric notebook source

# METADATA **{"version":"0.9","dependencies":{"spark_connector":{},"lakehouse":{"default_lakehouse_name":"copilot_analytics","known_lakehouses":[]}}}

# CELL **{"type":"code","source":"attached"}

import json
import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder.getOrCreate()


# CELL **{"type":"code","source":"attached"}

def ensure_quality_log_table():
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS logs_data_quality (
            check_id STRING,
            check_timestamp TIMESTAMP,
            table_name STRING,
            check_type STRING,
            check_description STRING,
            expected_value STRING,
            actual_value STRING,
            passed BOOLEAN,
            batch_id STRING
        ) USING DELTA
        """
    )


def add_result(results, table_name, check_type, description, expected_value, actual_value, passed, batch_id):
    results.append(
        {
            "check_id": str(uuid.uuid4()),
            "check_timestamp": None,
            "table_name": table_name,
            "check_type": check_type,
            "check_description": description,
            "expected_value": str(expected_value),
            "actual_value": str(actual_value),
            "passed": bool(passed),
            "batch_id": batch_id,
        }
    )


def run_checks():
    batch_id = str(uuid.uuid4())
    results = []

    row_count_targets = [
        "bronze_users",
        "bronze_licenses",
        "bronze_usage_reports",
        "bronze_m365_activity",
        "bronze_audit_logs",
        "silver_users",
        "silver_licenses",
        "silver_copilot_usage",
        "silver_copilot_interactions",
        "silver_m365_activity",
        "gold_user_license_usage",
        "gold_daily_metrics",
        "gold_department_summary",
    ]
    for table_name in row_count_targets:
        if not spark.catalog.tableExists(table_name):
            add_result(results, table_name, "ROW_COUNT", "Table exists and has rows", ">0", "missing", False, batch_id)
            continue
        count = spark.table(table_name).count()
        add_result(results, table_name, "ROW_COUNT", "Table has at least one row", ">0", count, count > 0, batch_id)

    if spark.catalog.tableExists("silver_users"):
        users_df = spark.table("silver_users")
        null_rate = users_df.filter(F.col("user_id").isNull()).count() / max(users_df.count(), 1)
        add_result(results, "silver_users", "NULL_PERCENTAGE", "user_id null percentage <= 0%", "0", round(null_rate, 6), null_rate <= 0.0, batch_id)

    freshness_targets = {
        "bronze_users": "_ingested_at",
        "bronze_licenses": "_ingested_at",
        "bronze_usage_reports": "_ingested_at",
        "bronze_m365_activity": "_ingested_at",
        "bronze_audit_logs": "_ingested_at",
        "gold_user_license_usage": "last_calculated_at",
    }
    for table_name, ts_col in freshness_targets.items():
        if not spark.catalog.tableExists(table_name):
            continue
        latest_ts = spark.table(table_name).agg(F.max(F.col(ts_col)).alias("mx")).collect()[0]["mx"]
        freshness_hours = None
        passed = False
        if latest_ts is not None:
            freshness_hours = (
                spark.sql("SELECT (unix_timestamp(current_timestamp()) - unix_timestamp(current_timestamp()))/3600.0 as hours").collect()[0]["hours"]
            )
            freshness_hours = (
                spark.sql(
                    f"SELECT (unix_timestamp(current_timestamp()) - unix_timestamp(max({ts_col})))/3600.0 as hours FROM {table_name}"
                ).collect()[0]["hours"]
            )
            passed = freshness_hours <= 24.0
        add_result(
            results,
            table_name,
            "FRESHNESS",
            f"{ts_col} within the last 24 hours",
            "<=24",
            freshness_hours if freshness_hours is not None else "null",
            passed,
            batch_id,
        )

    if spark.catalog.tableExists("silver_licenses") and spark.catalog.tableExists("silver_users"):
        orphan_count = (
            spark.table("silver_licenses").alias("l")
            .join(spark.table("silver_users").alias("u"), F.col("l.user_id") == F.col("u.user_id"), "left_anti")
            .count()
        )
        add_result(
            results,
            "silver_licenses",
            "REFERENTIAL_INTEGRITY",
            "All silver_licenses.user_id exist in silver_users",
            "0",
            orphan_count,
            orphan_count == 0,
            batch_id,
        )

    if spark.catalog.tableExists("gold_user_license_usage"):
        invalid_scores = spark.table("gold_user_license_usage").filter(
            (F.col("usage_score") < 0.0) | (F.col("usage_score") > 1.0)
        ).count()
        add_result(
            results,
            "gold_user_license_usage",
            "RANGE_VALIDATION",
            "usage_score within [0.0, 1.0]",
            "0 invalid rows",
            invalid_scores,
            invalid_scores == 0,
            batch_id,
        )

    result_df = spark.createDataFrame(results).withColumn("check_timestamp", F.current_timestamp())
    result_df.write.format("delta").mode("append").saveAsTable("logs_data_quality")

    failed = result_df.filter(~F.col("passed")).count()
    print(json.dumps({"batch_id": batch_id, "failed_checks": failed, "total_checks": len(results)}))
    if failed > 0:
        raise RuntimeError(f"Data quality checks failed: {failed} checks")


# CELL **{"type":"code","source":"attached"}

def main():
    ensure_quality_log_table()
    run_checks()


if __name__ == "__main__":
    main()
