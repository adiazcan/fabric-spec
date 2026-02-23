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


def upsert_control_state(source_name, records_processed, status, delta_link=None):
    delta_value = "NULL" if delta_link is None else f"'{delta_link}'"
    spark.sql(
        f"""
        MERGE INTO control_pipeline_state t
        USING (
            SELECT
                '{source_name}' AS source_name,
                current_timestamp() AS last_refresh_timestamp,
                {delta_value} AS delta_link,
                CAST(NULL AS STRING) AS last_content_uri,
                {int(records_processed)} AS records_processed,
                '{status}' AS status,
                current_timestamp() AS last_updated_at
        ) s
        ON t.source_name = s.source_name
        WHEN MATCHED THEN UPDATE SET
            t.last_refresh_timestamp = s.last_refresh_timestamp,
            t.delta_link = COALESCE(s.delta_link, t.delta_link),
            t.records_processed = s.records_processed,
            t.status = s.status,
            t.last_updated_at = s.last_updated_at
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def get_existing_delta_link(source_name):
    if not spark.catalog.tableExists("control_pipeline_state"):
        return None
    rows = spark.sql(
        f"SELECT delta_link FROM control_pipeline_state WHERE source_name = '{source_name}'"
    ).collect()
    if not rows:
        return None
    return rows[0]["delta_link"]


def fetch_users(config, token):
    base_url = config["graph_api"]["base_url"].rstrip("/")
    selected_columns = [
        "id",
        "displayName",
        "mail",
        "userPrincipalName",
        "department",
        "officeLocation",
        "jobTitle",
        "accountEnabled",
    ]
    next_link = get_existing_delta_link("graph_users")
    if not next_link:
        next_link = (
            f"{base_url}/v1.0/users/delta?"
            f"$select={','.join(selected_columns)}&$top=999"
        )

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    records = []
    final_delta_link = None
    while next_link:
        response = requests.get(next_link, headers=headers, timeout=120)
        response.raise_for_status()
        payload = response.json()
        records.extend(payload.get("value", []))
        next_link = payload.get("@odata.nextLink")
        if payload.get("@odata.deltaLink"):
            final_delta_link = payload["@odata.deltaLink"]

    return records, final_delta_link


def validate_schema(records):
    for item in records:
        if not item.get("id") or not item.get("userPrincipalName"):
            raise ValueError("Users ingestion failed schema validation for required keys")


def write_bronze_users(records):
    batch_id = str(uuid.uuid4())
    ingested_at = datetime.now(timezone.utc).isoformat()
    enriched = []
    for item in records:
        enriched.append(
            {
                "id": item.get("id"),
                "displayName": item.get("displayName"),
                "mail": item.get("mail"),
                "userPrincipalName": item.get("userPrincipalName"),
                "department": item.get("department"),
                "officeLocation": item.get("officeLocation"),
                "jobTitle": item.get("jobTitle"),
                "accountEnabled": item.get("accountEnabled"),
                "_ingested_at": ingested_at,
                "_source_api": "/v1.0/users/delta",
                "_batch_id": batch_id,
            }
        )
    df = spark.createDataFrame(enriched).withColumn(
        "_ingested_at", F.to_timestamp("_ingested_at")
    )
    (
        df.write.format("delta")
        .mode("append")
        .saveAsTable("bronze_users")
    )
    return len(enriched)


# CELL **{"type":"code","source":"attached"}

def main():
    config = load_config()
    ensure_control_table()
    token = get_graph_token(config)
    records, final_delta_link = fetch_users(config, token)
    if records:
        validate_schema(records)
        count = write_bronze_users(records)
    else:
        count = 0
    upsert_control_state("graph_users", count, "SUCCESS", final_delta_link)
    print(json.dumps({"status": "SUCCESS", "records_processed": count}))


if __name__ == "__main__":
    main()
