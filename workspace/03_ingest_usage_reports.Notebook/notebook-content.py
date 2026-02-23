# Fabric notebook source

# METADATA **{"version":"0.9","dependencies":{"spark_connector":{},"lakehouse":{"default_lakehouse_name":"copilot_analytics","known_lakehouses":[]}}}

# CELL **{"type":"code","source":"attached"}

import os
import uuid
import json
from datetime import datetime, timezone

import requests
import yaml
from azure.identity import ClientSecretCredential
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


def get_secret(vault_name, secret_name):
    try:
        return mssparkutils.credentials.getSecret(vault_name, secret_name)
    except Exception:
        env_name = secret_name.upper().replace("-", "_")
        value = os.getenv(env_name)
        if value:
            return value
        raise


def get_graph_token(config):
    vault = config["key_vault"]["name"]
    tenant_id = get_secret(vault, config["graph_api"]["tenant_id_secret"])
    client_id = get_secret(vault, config["graph_api"]["client_id_secret"])
    client_secret = get_secret(vault, config["graph_api"]["client_secret_secret"])
    credential = ClientSecretCredential(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
    )
    return credential.get_token("https://graph.microsoft.com/.default").token


def ensure_control_table():
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS control_pipeline_state (
            source_name STRING,
            last_refresh_timestamp TIMESTAMP,
            delta_link STRING,
            last_content_uri STRING,
            records_processed BIGINT,
            status STRING,
            last_updated_at TIMESTAMP
        ) USING DELTA
        """
    )


def get_last_report_refresh_date(source_name):
    if not spark.catalog.tableExists("control_pipeline_state"):
        return None
    rows = spark.sql(
        f"SELECT last_refresh_timestamp FROM control_pipeline_state WHERE source_name = '{source_name}'"
    ).collect()
    if not rows or rows[0]["last_refresh_timestamp"] is None:
        return None
    return rows[0]["last_refresh_timestamp"].date().isoformat()


def upsert_control_state(source_name, records_processed, status, report_refresh_date=None):
    refresh_expr = "current_timestamp()"
    if report_refresh_date:
        refresh_expr = f"to_timestamp('{report_refresh_date}')"
    spark.sql(
        f"""
        MERGE INTO control_pipeline_state t
        USING (
            SELECT
                '{source_name}' AS source_name,
                {refresh_expr} AS last_refresh_timestamp,
                CAST(NULL AS STRING) AS delta_link,
                CAST(NULL AS STRING) AS last_content_uri,
                {int(records_processed)} AS records_processed,
                '{status}' AS status,
                current_timestamp() AS last_updated_at
        ) s
        ON t.source_name = s.source_name
        WHEN MATCHED THEN UPDATE SET
            t.last_refresh_timestamp = s.last_refresh_timestamp,
            t.records_processed = s.records_processed,
            t.status = s.status,
            t.last_updated_at = s.last_updated_at
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def fetch_usage_report(config, token):
    base_url = config["graph_api"]["base_url"].rstrip("/")
    next_link = (
        f"{base_url}/beta/reports/getMicrosoft365CopilotUsageUserDetail"
        "(period='D30')?$format=application/json"
    )
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    records = []
    while next_link:
        response = requests.get(next_link, headers=headers, timeout=120)
        response.raise_for_status()
        payload = response.json()
        records.extend(payload.get("value", []))
        next_link = payload.get("@odata.nextLink")

    return records


def write_bronze_usage(records, watermark):
    batch_id = str(uuid.uuid4())
    ingested_at = datetime.now(timezone.utc).isoformat()
    output = []
    for item in records:
        report_refresh_date = item.get("reportRefreshDate")
        if watermark and report_refresh_date and report_refresh_date <= watermark:
            continue
        output.append(
            {
                "reportRefreshDate": report_refresh_date,
                "reportPeriod": str(item.get("reportPeriod")) if item.get("reportPeriod") is not None else None,
                "userPrincipalName": item.get("userPrincipalName"),
                "displayName": item.get("displayName"),
                "lastActivityDate": item.get("lastActivityDate"),
                "teamsLastActivityDate": item.get("microsoftTeamsCopilotLastActivityDate"),
                "wordLastActivityDate": item.get("wordCopilotLastActivityDate"),
                "excelLastActivityDate": item.get("excelCopilotLastActivityDate"),
                "powerPointLastActivityDate": item.get("powerPointCopilotLastActivityDate"),
                "outlookLastActivityDate": item.get("outlookCopilotLastActivityDate"),
                "oneNoteLastActivityDate": item.get("oneNoteCopilotLastActivityDate"),
                "loopLastActivityDate": item.get("loopCopilotLastActivityDate"),
                "copilotChatLastActivityDate": item.get("copilotChatLastActivityDate"),
                "_ingested_at": ingested_at,
                "_source_api": "/beta/reports/getMicrosoft365CopilotUsageUserDetail(period='D30')",
                "_batch_id": batch_id,
            }
        )

    if not output:
        return 0, None

    df = spark.createDataFrame(output).withColumn(
        "_ingested_at", F.to_timestamp("_ingested_at")
    )
    (
        df.write.format("delta")
        .mode("append")
        .saveAsTable("bronze_usage_reports")
    )
    latest = max([row["reportRefreshDate"] for row in output if row["reportRefreshDate"]], default=None)
    return len(output), latest


# CELL **{"type":"code","source":"attached"}

def main():
    config = load_config()
    ensure_control_table()
    token = get_graph_token(config)
    watermark = get_last_report_refresh_date("usage_reports")
    records = fetch_usage_report(config, token)
    count, latest_report_refresh_date = write_bronze_usage(records, watermark)
    upsert_control_state("usage_reports", count, "SUCCESS", latest_report_refresh_date)
    print(json.dumps({"status": "SUCCESS", "records_processed": count}))


if __name__ == "__main__":
    main()
