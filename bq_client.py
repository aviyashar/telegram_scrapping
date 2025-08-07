from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from loguru import logger
from datetime import datetime
import os


def get_bq_client(config):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config["GOOGLE_APPLICATION_CREDENTIALS"]
    return bigquery.Client(project=config["BQ_PROJECT_ID"])


def load_messages_to_bq(messages, config):
    """
    Append messages to BigQuery table.
    """
    if not messages:
        return
    client = get_bq_client(config)
    table_id = f"{config['BQ_PROJECT_ID']}.{config['BQ_DATASET']}.{config['BQ_TABLE']}"
    errors = client.insert_rows_json(table_id, messages)
    if errors:
        logger.error(f"BigQuery insert errors: {errors}")
        raise Exception(f"BigQuery insert errors: {errors}")
    logger.info(f"Inserted {len(messages)} messages to {table_id}")


def update_last_ingestion(group_id, messages, config):
    """
    Update the last ingestion timestamp for the group in the metadata table.
    """
    if not messages:
        return
    last_ts = max(m["timestamp"] for m in messages)
    client = get_bq_client(config)
    table_id = f"{config['BQ_PROJECT_ID']}.{config['BQ_DATASET']}.{config['BQ_METADATA_TABLE']}"
    row = {"group_id": str(group_id), "last_ingestion": last_ts, "updated_at": datetime.utcnow().isoformat()}
    # Upsert logic: delete old, insert new
    query = f"""
        DELETE FROM `{table_id}` WHERE group_id = @group_id;
    """
    client.query(query, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("group_id", "STRING", str(group_id))]
    )).result()
    errors = client.insert_rows_json(table_id, [row])
    if errors:
        logger.error(f"BigQuery metadata insert errors: {errors}")
        raise Exception(f"BigQuery metadata insert errors: {errors}")
    logger.info(f"Updated last_ingestion for {group_id} to {last_ts}")


def get_last_ingestion(group_id, config):
    """
    Get the last ingestion timestamp for the group from the metadata table.
    """
    client = get_bq_client(config)
    table_id = f"{config['BQ_PROJECT_ID']}.{config['BQ_DATASET']}.{config['BQ_METADATA_TABLE']}"
    query = f"""
        SELECT last_ingestion FROM `{table_id}` WHERE group_id = @group_id ORDER BY last_ingestion DESC LIMIT 1
    """
    job = client.query(query, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("group_id", "STRING", str(group_id))]
    ))
    rows = list(job)
    if rows:
        return rows[0]["last_ingestion"]
    return None 