# Fabric notebook source

# METADATA **{"version":"0.9","dependencies":{"spark_connector":{},"lakehouse":{"default_lakehouse_name":"copilot_analytics","known_lakehouses":[]}}}

# CELL **{"type":"code","source":"attached"}

import os
import uuid
import json
import time
from datetime import datetime, timedelta, timezone

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


def get_window_start():
    if not spark.catalog.tableExists("control_pipeline_state"):
        return datetime.now(timezone.utc) - timedelta(days=1)
    rows = spark.sql(
        "SELECT last_refresh_timestamp FROM control_pipeline_state WHERE source_name = 'audit_logs'"
    ).collect()
    if not rows or rows[0]["last_refresh_timestamp"] is None:
        return datetime.now(timezone.utc) - timedelta(days=1)
    return rows[0]["last_refresh_timestamp"].replace(tzinfo=timezone.utc)


def upsert_control_state(records_processed, status, last_content_uri=None):
    last_content_uri_value = "NULL" if not last_content_uri else f"'{last_content_uri}'"
    spark.sql(
        f"""
        MERGE INTO control_pipeline_state t
        USING (
            SELECT
                'audit_logs' AS source_name,
                current_timestamp() AS last_refresh_timestamp,
                CAST(NULL AS STRING) AS delta_link,
                {last_content_uri_value} AS last_content_uri,
                {int(records_processed)} AS records_processed,
                '{status}' AS status,
                current_timestamp() AS last_updated_at
        ) s
        ON t.source_name = s.source_name
        WHEN MATCHED THEN UPDATE SET
            t.last_refresh_timestamp = s.last_refresh_timestamp,
            t.last_content_uri = COALESCE(s.last_content_uri, t.last_content_uri),
            t.records_processed = s.records_processed,
            t.status = s.status,
            t.last_updated_at = s.last_updated_at
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def create_audit_query(config, token, start_dt, end_dt):
    base_url = config["graph_api"]["base_url"].rstrip("/")
    url = f"{base_url}/beta/security/auditLog/queries"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {
        "displayName": f"CopilotInteraction-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
        "filterStartDateTime": start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "filterEndDateTime": end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "recordTypeFilters": ["CopilotInteraction"],
        "operationFilters": ["CopilotInteraction"],
        "serviceFilter": "Copilot",
    }
    response = requests.post(url, headers=headers, json=payload, timeout=120)
    response.raise_for_status()
    return response.json().get("id")


def wait_for_query_completion(config, token, query_id, max_attempts=60, wait_seconds=10):
    base_url = config["graph_api"]["base_url"].rstrip("/")
    url = f"{base_url}/beta/security/auditLog/queries/{query_id}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    for _ in range(max_attempts):
        response = requests.get(url, headers=headers, timeout=120)
        response.raise_for_status()
        status = response.json().get("status", "").lower()
        if status == "succeeded":
            return True
        if status in {"failed", "cancelled"}:
            return False
        time.sleep(wait_seconds)
    return False


def fetch_audit_records(config, token, query_id):
    base_url = config["graph_api"]["base_url"].rstrip("/")
    next_link = f"{base_url}/beta/security/auditLog/queries/{query_id}/records"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    records = []
    final_content_uri = None
    while next_link:
        response = requests.get(next_link, headers=headers, timeout=120)
        response.raise_for_status()
        payload = response.json()
        records.extend(payload.get("value", []))
        final_content_uri = next_link
        next_link = payload.get("@odata.nextLink")
    return records, final_content_uri


def write_bronze_audit_logs(records):
    if not records:
        return 0
    batch_id = str(uuid.uuid4())
    ingested_at = datetime.now(timezone.utc).isoformat()
    output = []
    for item in records:
        output.append(
            {
                "id": item.get("id"),
                "createdDateTime": item.get("createdDateTime"),
                "userId": item.get("userId"),
                "userPrincipalName": item.get("userPrincipalName"),
                "operation": item.get("operation"),
                "auditLogRecordType": item.get("auditLogRecordType"),
                "service": item.get("service"),
                "clientIp": item.get("clientIp"),
                "auditData": json.dumps(item.get("auditData")) if item.get("auditData") is not None else None,
                "_ingested_at": ingested_at,
                "_source_api": "/beta/security/auditLog/queries/{id}/records",
                "_batch_id": batch_id,
            }
        )
    df = spark.createDataFrame(output).withColumn(
        "_ingested_at", F.to_timestamp("_ingested_at")
    )
    (
        df.write.format("delta")
        .mode("append")
        .saveAsTable("bronze_audit_logs")
    )
    return len(output)


# CELL **{"type":"code","source":"attached"}

def main():
    config = load_config()
    ensure_control_table()
    token = get_graph_token(config)
    window_start = get_window_start()
    window_end = datetime.now(timezone.utc)
    query_id = create_audit_query(config, token, window_start, window_end)
    if not wait_for_query_completion(config, token, query_id):
        upsert_control_state(0, "FAILED")
        raise RuntimeError("Audit query did not complete successfully")
    records, content_uri = fetch_audit_records(config, token, query_id)
    count = write_bronze_audit_logs(records)
    upsert_control_state(count, "SUCCESS", content_uri)
    print(json.dumps({"status": "SUCCESS", "records_processed": count}))


if __name__ == "__main__":
    main()
