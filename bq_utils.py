from google.cloud import bigquery
from loguru import logger


def get_entities_data_from_bq(
    bq_project: str, bq_dataset: str, bq_table: str
) -> list[dict[str, str | None]]:
    """Fetch entities data from BigQuery table."""
    client = bigquery.Client(project=bq_project)
    query = f"""
    SELECT DISTINCT
      group_id,
      MAX(timestamp) as last_fetch_time
    FROM
      `{bq_project}.{bq_dataset}.{bq_table}`
    GROUP BY
      group_id;
    """
    try:
        query_job = client.query(query)
        results = query_job.result()
        entities_data: list[dict[str, str | None]] = [
            {
                "id": str(row["group_id"]),  # type: ignore[union-attr]
                "last_fetch_time": row["last_fetch_time"].isoformat()  # type: ignore[union-attr]
                if row["last_fetch_time"]
                else None,
            }
            for row in results
        ]
        logger.debug(f"Entities data: {entities_data}")
        return entities_data
    except Exception as e:
        logger.error(f"Error fetching entities data from BigQuery: {e}")
        return []
