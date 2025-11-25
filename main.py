import argparse
from datetime import datetime, timedelta, timezone

from loguru import logger

from bq_utils import get_entities_data_from_bq
from config import load_config, validate_config
from telegram_bq_ingest import ingest_telegram_to_bq


def parse_args():
    parser = argparse.ArgumentParser(
        description="Telegram to BigQuery Pipeline with Date Range"
    )
    parser.add_argument(
        "--from-date",
        help="Start date in ISO format (optional, will use metadata if not provided)",
    )
    parser.add_argument("--to-date", help="End date in ISO format (default: now)")
    return parser.parse_args()


def main(from_date: str | None = None, to_date: str | None = None):
    config = load_config()

    # Validate configuration
    try:
        validate_config(config)
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        return

    # BigQuery configuration
    bq_project = config["BQ_PROJECT_ID"]
    bq_dataset = config["BQ_DATASET"]
    bq_table = config["BQ_TABLE"]
    bq_metadata_table = config["BQ_METADATA_TABLE"]
    bq_groups_table = config["BQ_GROUPS_TABLE"]

    # Fetch groups from BigQuery groups table
    tg_entities_data = get_entities_data_from_bq(bq_project, bq_dataset, bq_groups_table)
    if not tg_entities_data:
        logger.error(
            f"No Telegram groups found in {bq_project}.{bq_dataset}.{bq_groups_table}. "
            "Please ensure the table exists and contains groups with group_link values."
        )
        return

    # Telegram configuration
    telegram_config = {
        "TELEGRAM_API_ID": config["TELEGRAM_API_ID"],
        "TELEGRAM_API_HASH": config["TELEGRAM_API_HASH"],
    }

    # Date range (default: last 365 days if not provided)
    if not from_date:
        from_date = (datetime.now(timezone.utc) - timedelta(days=1095)).isoformat()
    if not to_date:
        to_date = datetime.now(timezone.utc).isoformat()

    logger.info("Starting Telegram to BigQuery ingestion...")
    if from_date:
        logger.info(f"Using date range: {from_date} to {to_date}")
    else:
        logger.info(
            f"Using metadata table {bq_project}.{bq_dataset}.{bq_metadata_table} for incremental fetching to {to_date}"
        )
    logger.info(
        "Pipeline will: Fetch new messages → Check duplicates → Insert only new messages → Update metadata"
    )

    try:
        total_inserted = ingest_telegram_to_bq(
            tg_entities_data=tg_entities_data,
            bq_project=bq_project,
            bq_dataset=bq_dataset,
            bq_table=bq_table,
            bq_metadata_table=bq_metadata_table,
            telegram_config=telegram_config,
            from_date=from_date,
            to_date=to_date,
        )
        logger.info("✅ Pipeline completed successfully!")
        logger.info(f"📊 Total messages inserted: {total_inserted}")
        logger.info("🔄 Metadata updated for next run")
        logger.info("🎉 Your pipeline is now complete and production-ready!")
    except Exception as e:
        logger.error(f"❌ Error during ingestion: {e}")
        raise


if __name__ == "__main__":
    args = parse_args()
    main(from_date=args.from_date, to_date=args.to_date)
