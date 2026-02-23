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


def upsert_control_state(source_name, records_processed, status):
    spark.sql(
        f"""
        MERGE INTO control_pipeline_state t
        USING (
            SELECT
                '{source_name}' AS source_name,
                current_timestamp() AS last_refresh_timestamp,
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


def fetch_licenses(config, token):
    base_url = config["graph_api"]["base_url"].rstrip("/")
    next_link = (
        f"{base_url}/v1.0/users?"
        "$select=id,userPrincipalName,assignedLicenses&$top=999"
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


def write_bronze_licenses(records, copilot_sku_ids):
    batch_id = str(uuid.uuid4())
    ingested_at = datetime.now(timezone.utc).isoformat()
    output = []
    for user in records:
        user_id = user.get("id")
        upn = user.get("userPrincipalName")
        for license_item in user.get("assignedLicenses", []):
            sku_id = license_item.get("skuId")
            output.append(
                {
                    "userId": user_id,
                    "userPrincipalName": upn,
                    "skuId": sku_id,
                    "disabledPlans": json.dumps(license_item.get("disabledPlans", [])),
                    "isCopilotSku": sku_id in set(copilot_sku_ids),
                    "_ingested_at": ingested_at,
                    "_source_api": "/v1.0/users?$select=id,userPrincipalName,assignedLicenses",
                    "_batch_id": batch_id,
                }
            )

    if not output:
        return 0

    df = spark.createDataFrame(output).withColumn(
        "_ingested_at", F.to_timestamp("_ingested_at")
    )
    (
        df.write.format("delta")
        .mode("append")
        .saveAsTable("bronze_licenses")
    )
    return len(output)


def main():
    config = load_config()
    ensure_control_table()
    token = get_graph_token(config)
    records = fetch_licenses(config, token)
    count = write_bronze_licenses(records, config.get("copilot_sku_ids", []))
    upsert_control_state("graph_licenses", count, "SUCCESS")
    print(json.dumps({"status": "SUCCESS", "records_processed": count}))


if __name__ == "__main__":
    main()
