from google.cloud import bigquery
from loguru import logger
import os
from dotenv import load_dotenv
from datetime import datetime, timezone

def format_telegram_url(url):
    """Format Telegram URL to https://t.me/ format."""
    if not url:
        return None
    
    # Remove @ if present
    if url.startswith('@'):
        url = url[1:]
    
    # If it's already a full URL, return as is
    if url.startswith('https://t.me/') or url.startswith('http://t.me/'):
        return url
    
    # If it's just a username, add https://t.me/
    if not url.startswith('http'):
        return f"https://t.me/{url}"
    
    return url

def update_existing_metadata():
    """Update existing telegram_last_ingestion table and add new groups from telegram_url field."""
    load_dotenv()
    
    # BigQuery configuration
    bq_project = 'pwcnext-sandbox01'
    bq_dataset = 'telegram'
    bq_metadata_table = 'telegram_last_ingestion'
    bq_messages_table = 'telegram_messages'
    
    client = bigquery.Client(project=bq_project)
    
    # First, check if is_first_time column exists and add it if missing
    try:
        table = client.get_table(f"{bq_project}.{bq_dataset}.{bq_metadata_table}")
        existing_fields = {field.name for field in table.schema}
        
        if 'is_first_time' not in existing_fields:
            logger.info("Adding is_first_time column to metadata table...")
            new_schema = table.schema + [
                bigquery.SchemaField("is_first_time", "BOOLEAN", mode="REQUIRED")
            ]
            table.schema = new_schema
            client.update_table(table, ["schema"])
            logger.info("Successfully added is_first_time column")
        else:
            logger.info("is_first_time column already exists")
    except Exception as e:
        logger.error(f"Error checking/adding is_first_time column: {e}")
        return
    
    # Get all unique telegram_url values from messages table
    telegram_urls_query = f"""
    SELECT DISTINCT telegram_url
    FROM `{bq_project}.{bq_dataset}.{bq_messages_table}`
    WHERE telegram_url IS NOT NULL AND telegram_url != ''
    """
    
    try:
        urls_job = client.query(telegram_urls_query)
        telegram_urls = [row.telegram_url for row in urls_job.result()]
        logger.info(f"Found {len(telegram_urls)} unique Telegram URLs: {telegram_urls}")
        
        # Format URLs and filter valid ones
        formatted_urls = []
        for url in telegram_urls:
            formatted_url = format_telegram_url(url)
            if formatted_url and formatted_url.startswith('https://t.me/'):
                formatted_urls.append(formatted_url)
        
        logger.info(f"Formatted URLs: {formatted_urls}")
        
        # Get existing groups from metadata table
        existing_groups_query = f"""
        SELECT DISTINCT group_id
        FROM `{bq_project}.{bq_dataset}.{bq_metadata_table}`
        """
        existing_job = client.query(existing_groups_query)
        existing_groups = {row.group_id for row in existing_job.result()}
        logger.info(f"Existing groups: {existing_groups}")
        
        # Find new groups to add
        new_groups = [url for url in formatted_urls if url not in existing_groups]
        logger.info(f"New groups to add: {new_groups}")
        
        # Add new groups to metadata table
        current_time = datetime.now(timezone.utc)
        for group_id in new_groups:
            insert_query = f"""
            INSERT INTO `{bq_project}.{bq_dataset}.{bq_metadata_table}` (group_id, last_fetch_time, is_first_time)
            VALUES (@group_id, @last_fetch_time, @is_first_time)
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("group_id", "STRING", group_id),
                    bigquery.ScalarQueryParameter("last_fetch_time", "TIMESTAMP", current_time),
                    bigquery.ScalarQueryParameter("is_first_time", "BOOLEAN", True)
                ]
            )
            
            client.query(insert_query, job_config).result()
            logger.info(f"Added new group: {group_id} with is_first_time = true")
        
        # Update existing groups to set is_first_time = false
        for group_id in existing_groups:
            update_query = f"""
            UPDATE `{bq_project}.{bq_dataset}.{bq_metadata_table}`
            SET is_first_time = false
            WHERE group_id = @group_id
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("group_id", "STRING", group_id)]
            )
            
            client.query(update_query, job_config).result()
            logger.info(f"Updated existing group: {group_id} with is_first_time = false")
                
    except Exception as e:
        logger.error(f"Error updating metadata: {e}")
        raise

if __name__ == "__main__":
    update_existing_metadata()
