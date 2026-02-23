# Fabric notebook source

# METADATA **{"version":"0.9","dependencies":{"spark_connector":{},"lakehouse":{"default_lakehouse_name":"copilot_analytics","known_lakehouses":[]}}}

# CELL **{"type":"code","source":"attached"}

import os

import yaml
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


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


def merge_by_key(table_name, source_df, condition):
    if not spark.catalog.tableExists(table_name):
        source_df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        return
    target = DeltaTable.forName(spark, table_name)
    (
        target.alias("t")
        .merge(source_df.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def build_gold_user_license_usage(config):
    usage_cfg = config.get("usage_score", {})
    frequency_weight = float(usage_cfg.get("frequency_weight", 0.4))
    breadth_weight = float(usage_cfg.get("breadth_weight", 0.3))
    recency_weight = float(usage_cfg.get("recency_weight", 0.3))
    active_threshold = float(usage_cfg.get("active_threshold", 0.5))
    low_usage_threshold = float(usage_cfg.get("low_usage_threshold", 0.1))
    inactivity_days = int(usage_cfg.get("inactivity_days", 30))

    users = spark.table("silver_users")
    licenses = spark.table("silver_licenses")
    usage = spark.table("silver_copilot_usage")
    interactions = spark.table("silver_copilot_interactions")
    activity = spark.table("silver_m365_activity")

    latest_usage = usage.groupBy("user_principal_name").agg(
        F.max("report_refresh_date").alias("report_refresh_date"),
        F.max("last_activity_date").alias("last_activity_date"),
        F.max("apps_used_count").alias("apps_used_count"),
    )

    interactions_30d = interactions.filter(
        F.col("event_date") >= F.date_sub(F.current_date(), 30)
    ).groupBy("user_principal_name").agg(F.count("event_id").alias("interaction_count_30d"))

    active_copilot_license = licenses.filter(
        F.col("is_copilot_license") & F.col("is_active")
    ).groupBy("user_id").agg(
        F.lit(True).alias("has_copilot_license"),
        F.min("assigned_date").alias("license_assigned_date"),
    )

    latest_activity = activity.groupBy("user_principal_name").agg(
        F.max("active_services_count").alias("active_services_count")
    )

    base = (
        users.alias("u")
        .join(active_copilot_license.alias("l"), F.col("u.user_id") == F.col("l.user_id"), "left")
        .join(latest_usage.alias("us"), F.col("u.user_principal_name") == F.col("us.user_principal_name"), "left")
        .join(interactions_30d.alias("i"), F.col("u.user_principal_name") == F.col("i.user_principal_name"), "left")
        .join(latest_activity.alias("a"), F.col("u.user_principal_name") == F.col("a.user_principal_name"), "left")
        .select(
            F.col("u.user_id"),
            F.col("u.user_principal_name"),
            F.col("u.display_name"),
            F.col("u.email"),
            F.col("u.department"),
            F.col("u.office_location"),
            F.col("u.job_title"),
            F.col("u.is_enabled"),
            F.coalesce(F.col("l.has_copilot_license"), F.lit(False)).alias("has_copilot_license"),
            F.col("l.license_assigned_date"),
            F.col("us.last_activity_date"),
            F.when(F.col("us.last_activity_date").isNotNull(), F.datediff(F.current_date(), F.col("us.last_activity_date"))).otherwise(None).alias(
                "days_since_last_activity"
            ),
            F.coalesce(F.col("i.interaction_count_30d"), F.lit(0)).alias("interaction_count_30d"),
            F.coalesce(F.col("us.apps_used_count"), F.lit(0)).alias("apps_used_count"),
            F.coalesce(F.col("a.active_services_count"), F.lit(0)).alias("active_services_count"),
        )
    )

    scored = (
        base.withColumn("frequency_score", F.least(F.col("interaction_count_30d") / F.lit(20.0), F.lit(1.0)))
        .withColumn("breadth_score", F.least(F.col("apps_used_count") / F.lit(8.0), F.lit(1.0)))
        .withColumn(
            "recency_score",
            F.when(
                F.col("days_since_last_activity").isNull(),
                F.lit(0.0),
            ).otherwise(F.greatest(F.lit(0.0), F.lit(1.0) - (F.col("days_since_last_activity") / F.lit(float(inactivity_days))))),
        )
        .withColumn(
            "usage_score",
            (F.col("frequency_score") * F.lit(frequency_weight))
            + (F.col("breadth_score") * F.lit(breadth_weight))
            + (F.col("recency_score") * F.lit(recency_weight)),
        )
        .withColumn(
            "usage_category",
            F.when(F.col("usage_score") >= F.lit(active_threshold), F.lit("Active"))
            .when(F.col("usage_score") >= F.lit(low_usage_threshold), F.lit("Low Usage"))
            .otherwise(F.lit("Inactive")),
        )
        .withColumn("m365_activity_score", F.least(F.col("active_services_count") / F.lit(7.0), F.lit(1.0)))
        .withColumn(
            "recommendation",
            F.when(F.col("has_copilot_license") & (F.col("usage_category") == "Inactive"), F.lit("Recommend Remove"))
            .when(F.col("has_copilot_license") & (F.col("usage_category") == "Low Usage"), F.lit("Monitor"))
            .when(F.col("has_copilot_license") & (F.col("usage_category") == "Active"), F.lit("Retain"))
            .when(~F.col("has_copilot_license") & (F.col("m365_activity_score") >= F.lit(0.5)), F.lit("Candidate for License"))
            .otherwise(F.lit("No Action")),
        )
        .withColumn("last_calculated_at", F.current_timestamp())
    )

    gold = scored.select(
        "user_id",
        "user_principal_name",
        "display_name",
        "email",
        "department",
        "office_location",
        "job_title",
        "is_enabled",
        "has_copilot_license",
        "license_assigned_date",
        "last_activity_date",
        "days_since_last_activity",
        "interaction_count_30d",
        "apps_used_count",
        "frequency_score",
        "breadth_score",
        "recency_score",
        "usage_score",
        "usage_category",
        "recommendation",
        "m365_activity_score",
        "last_calculated_at",
    )

    gold.write.format("delta").mode("overwrite").saveAsTable("gold_user_license_usage")


def build_gold_daily_metrics(config):
    license_cost = float(config.get("license_cost", {}).get("cost_per_user_per_month", 30.0))
    gold = spark.table("gold_user_license_usage")
    today = spark.sql("SELECT current_date() AS metric_date").collect()[0]["metric_date"]

    daily = gold.agg(
        F.count(F.when(F.col("is_enabled"), True)).alias("total_users"),
        F.sum(F.when(F.col("has_copilot_license"), 1).otherwise(0)).alias("total_copilot_licensed"),
        F.sum(F.when(F.col("usage_category") == "Active", 1).otherwise(0)).alias("active_users"),
        F.sum(F.when(F.col("usage_category") == "Low Usage", 1).otherwise(0)).alias("low_usage_users"),
        F.sum(F.when(F.col("usage_category") == "Inactive", 1).otherwise(0)).alias("inactive_users"),
        F.sum("interaction_count_30d").alias("total_interactions"),
        F.avg("usage_score").alias("avg_usage_score"),
    ).withColumn("metric_date", F.lit(today)).withColumn(
        "utilization_rate",
        F.when(F.col("total_copilot_licensed") > 0, F.col("active_users") / F.col("total_copilot_licensed")).otherwise(F.lit(0.0)),
    ).withColumn(
        "estimated_monthly_savings",
        F.col("inactive_users") * F.lit(license_cost),
    ).withColumn("last_calculated_at", F.current_timestamp())

    merge_by_key("gold_daily_metrics", daily, "t.metric_date = s.metric_date")


def build_gold_department_summary():
    gold = spark.table("gold_user_license_usage")
    today = spark.sql("SELECT current_date() AS metric_date").collect()[0]["metric_date"]

    dept = (
        gold.groupBy("department")
        .agg(
            F.count(F.when(F.col("is_enabled"), True)).alias("total_users"),
            F.sum(F.when(F.col("has_copilot_license"), 1).otherwise(0)).alias("copilot_licensed_users"),
            F.sum(F.when(F.col("usage_category") == "Active", 1).otherwise(0)).alias("active_users"),
            F.sum(F.when(F.col("usage_category") == "Low Usage", 1).otherwise(0)).alias("low_usage_users"),
            F.sum(F.when(F.col("usage_category") == "Inactive", 1).otherwise(0)).alias("inactive_users"),
            F.avg("usage_score").alias("avg_usage_score"),
            F.sum("interaction_count_30d").alias("total_interactions"),
        )
        .withColumn("department", F.coalesce(F.col("department"), F.lit("Unknown")))
        .withColumn("metric_date", F.lit(today))
        .withColumn(
            "utilization_rate",
            F.when(F.col("copilot_licensed_users") > 0, F.col("active_users") / F.col("copilot_licensed_users")).otherwise(F.lit(0.0)),
        )
        .withColumn("last_calculated_at", F.current_timestamp())
    )

    merge_by_key(
        "gold_department_summary",
        dept,
        "t.department = s.department AND t.metric_date = s.metric_date",
    )


# CELL **{"type":"code","source":"attached"}

def main():
    config = load_config()
    build_gold_user_license_usage(config)
    build_gold_daily_metrics(config)
    build_gold_department_summary()


if __name__ == "__main__":
    main()
