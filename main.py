import argparse
from config import load_config
from telegram_bq_ingest import ingest_telegram_to_bq
from bq_utils import get_group_ids_from_bq
from loguru import logger
from datetime import datetime, timedelta, timezone
import sys

def parse_args():
    parser = argparse.ArgumentParser(description="Telegram to BigQuery Pipeline with Date Range")
    parser.add_argument('--from-date', help='Start date in ISO format (optional, will use metadata if not provided)')
    parser.add_argument('--to-date', help='End date in ISO format (default: now)')
    return parser.parse_args()

def main():
    args = parse_args()
    config = load_config()
    
    # BigQuery configuration
    bq_project = config.get('BQ_PROJECT_ID', 'your-gcp-project-id')
    bq_dataset = config.get('BQ_DATASET', 'your_dataset')
    bq_table = config.get('BQ_TABLE', 'telegram_messages')
    bq_metadata_table = config.get('BQ_METADATA_TABLE', 'telegram_last_ingestion')
    
    # Fetch group IDs from BigQuery metadata table
    group_ids = get_group_ids_from_bq(bq_project, bq_dataset, bq_metadata_table)
    
    if not group_ids:
        logger.error("No group IDs found in metadata table. Please ensure the table exists and contains data.")
        return
    
    # Telegram configuration
    telegram_config = {
        'TELEGRAM_API_ID': config['TELEGRAM_API_ID'],
        'TELEGRAM_API_HASH': config['TELEGRAM_API_HASH'],
    }
    
    # Date range (default: last 7 days)
    from_date = args.from_date
    to_date = args.to_date
    
    if not from_date:
        from_date = (datetime.now(timezone.utc) - timedelta(days=365)).isoformat()
    if not to_date:
        to_date = datetime.now(timezone.utc).isoformat()
    
    logger.info(f"Starting Telegram to BigQuery ingestion for groups: {group_ids}")
    if from_date:
        logger.info(f"Using provided date range: {from_date} to {to_date}")
    else:
        logger.info(f"Using metadata table {bq_project}.{bq_dataset}.{bq_metadata_table} for incremental fetching to {to_date}")
    logger.info("Pipeline will: Fetch new messages → Check duplicates → Insert only new messages → Update metadata")
    
    try:
        total_inserted = ingest_telegram_to_bq(
            group_ids=group_ids,
            bq_project=bq_project,
            bq_dataset=bq_dataset,
            bq_table=bq_table,
            bq_metadata_table=bq_metadata_table,
            telegram_config=telegram_config,
            from_date=from_date,
            to_date=to_date
        )
        logger.info(f"✅ Pipeline completed successfully!")
        logger.info(f"📊 Total messages inserted: {total_inserted}")
        logger.info(f"🔄 Metadata updated for next run")
        logger.info(f"🎉 Your pipeline is now complete and production-ready!")
    except Exception as e:
        logger.error(f"❌ Error during ingestion: {e}")
        raise

if __name__ == "__main__":
    main() 