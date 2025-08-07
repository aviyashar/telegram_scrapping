import argparse
from config import load_config
from telegram_client import fetch_new_messages
from bq_client import load_messages_to_bq, update_last_ingestion, get_last_ingestion
from loguru import logger
import sys


def parse_args():
    parser = argparse.ArgumentParser(description="Telegram to BigQuery Pipeline")
    parser.add_argument('--groups', required=True, help='Comma-separated list of group IDs or links')
    parser.add_argument('--from', dest='from_time', required=False, help='Start ISO timestamp (optional)')
    parser.add_argument('--to', dest='to_time', required=False, help='End ISO timestamp (optional)')
    return parser.parse_args()


def main():
    args = parse_args()
    config = load_config()
    group_ids = [g.strip() for g in args.groups.split(',')]
    logger.info(f"Starting ingestion for groups: {group_ids}")
    total_fetched = 0
    for group_id in group_ids:
        try:
            last_ts = get_last_ingestion(group_id, config)
            logger.info(f"Last ingestion for {group_id}: {last_ts}")
            messages = fetch_new_messages(group_id, last_ts, args.from_time, args.to_time, config)
            logger.info(f"Fetched {len(messages)} messages from {group_id}")
            if messages:
                load_messages_to_bq(messages, config)
                update_last_ingestion(group_id, messages, config)
            total_fetched += len(messages)
        except Exception as e:
            logger.error(f"Error processing group {group_id}: {e}")
    logger.info(f"Total messages fetched: {total_fetched}")

if __name__ == "__main__":
    main() 