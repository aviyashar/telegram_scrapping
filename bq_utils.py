from google.cloud import bigquery
from loguru import logger


def get_entities_data_from_bq(
    bq_project: str,
    bq_dataset: str,
    bq_groups_table: str = "groups",
) -> list[dict[str, str | None]]:
    """Fetch entities data from BigQuery groups table.

    Args:
        bq_project: BigQuery project ID
        bq_dataset: BigQuery dataset name
        bq_groups_table: Name of the groups table (default: 'groups')

    Returns:
        List of entities with id, link, and last_fetch_time
    """
    client = bigquery.Client(project=bq_project)

    # Get groups from the groups table (only relevant ones with links)
    query = f"""
    SELECT
      group_id,
      group_link,
      last_fetch_time
    FROM
      `{bq_project}.{bq_dataset}.{bq_groups_table}`
    WHERE
      (is_relevant = TRUE OR is_relevant IS NULL)
      AND group_link IS NOT NULL;
    """
    try:
        query_job = client.query(query)
        results = query_job.result()
        entities_data: list[dict[str, str | None]] = [
            {
                "id": str(row["group_id"]),
                "link": str(row["group_link"]),
                "last_fetch_time": row["last_fetch_time"].isoformat()
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
