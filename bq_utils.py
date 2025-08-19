from google.cloud import bigquery
from loguru import logger


def get_group_ids_from_bq(bq_project, bq_dataset, bq_metadata_table):
    """Fetch group IDs from BigQuery metadata table."""
    client = bigquery.Client(project=bq_project)
    query = f"""
    SELECT
      DISTINCT group_id
    FROM
      `{bq_project}.{bq_dataset}.{bq_metadata_table}`;
    """
    try:
        query_job = client.query(query)
        results = query_job.result()
        group_ids = [row.group_id for row in results]
        logger.info(f"Found {len(group_ids)} groups in metadata table: {group_ids}")
        return group_ids
    except Exception as e:
        logger.error(f"Error fetching group IDs from BigQuery: {e}")
        return []
