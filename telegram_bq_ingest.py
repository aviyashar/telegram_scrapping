import asyncio
import os
import re
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from dotenv import load_dotenv
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from telethon import TelegramClient
from telethon.errors import FloodWaitError, RPCError
from telethon.tl.types import Channel, Chat, MessageMediaDocument, MessageMediaPhoto


async def retry_on_flood(
    func: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any
) -> Any:
    while True:
        try:
            return await func(*args, **kwargs)
        except FloodWaitError as e:
            print(f"Waiting {e.seconds} seconds...")
            await asyncio.sleep(e.seconds)


async def is_eligible_for_scraping(
    client: TelegramClient, entity_id: str
) -> bool | None:
    """
    Check if the given Telegram entity should be scraped.

    Returns:
        True  -> if entity is a group, supergroup, or chat.
        False -> if entity is a user or a broadcast channel.
    """
    try:
        entity = await client.get_entity(entity_id)

        # Group chats (old style), supergroup or broadcast channel
        if isinstance(entity, Chat) or isinstance(entity, Channel):
            print(f"Entity {entity_id} is eligible for scraping.")
            return True

        # Fallback for unexpected types
        print(f"⚠ Entity {entity_id} is NOT eligible for scraping.")
        return False
    except FloodWaitError as e:
        print(f"⏳ Flood wait error: {e}")
        await asyncio.sleep(e.seconds)
        await retry_on_flood(
            func=is_eligible_for_scraping, client=client, entity_id=entity_id
        )
    except RPCError as e:
        print(f"❌ Error fetching entity {entity_id}: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False


def ensure_bq_table(client, project, dataset, table, schema):
    table_id = f"{project}.{dataset}.{table}"
    try:
        client.get_table(table_id)
    except NotFound:
        table_obj = bigquery.Table(table_id, schema=schema)
        client.create_table(table_obj)
        print(f"Created table {table_id}")


def ensure_metadata_table(client, project, dataset, table):
    table_id = f"{project}.{dataset}.{table}"
    schema = [
        bigquery.SchemaField("group_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("last_fetch_time", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("is_first_time", "BOOLEAN", mode="REQUIRED"),
    ]
    try:
        client.get_table(table_id)
    except NotFound:
        table_obj = bigquery.Table(table_id, schema=schema)
        client.create_table(table_obj)
        print(f"Created metadata table {table_id}")


def extract_urls(text):
    """Extract URLs from text using regex."""
    if not text:
        return []
    url_pattern = r"http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+"
    return re.findall(url_pattern, text)


def extract_telegram_url(text):
    """Extract Telegram URL from text."""
    if not text:
        return None
    telegram_patterns = [
        r"https?://t\.me/[a-zA-Z0-9_]{5,}",  # t.me links
        r"https?://telegram\.me/[a-zA-Z0-9_]{5,}",  # telegram.me links
        r"@[a-zA-Z0-9_]{5,}",  # Telegram usernames
    ]
    for pattern in telegram_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(0)
    return None


def get_message_type(message):
    """Determine message type based on media."""
    if message.media:
        if isinstance(message.media, MessageMediaPhoto):
            return "image"
        elif isinstance(message.media, MessageMediaDocument):
            if hasattr(message.media.document, "mime_type"):
                mime = message.media.document.mime_type
                if mime.startswith("video"):
                    return "video"
                elif mime.startswith("image"):
                    return "image"
                else:
                    return "other"
            return "other"
        else:
            return "other"
    return "text"


def normalize_message(message, group_id):
    """Normalize Telegram message to BigQuery schema."""
    return {
        "message_id": str(message.id),
        "group_id": str(group_id),
        "sender_id": str(message.sender_id) if message.sender_id else None,
        "sender_name": getattr(message.sender, "first_name", None)
        if hasattr(message, "sender")
        else None,
        "message_text": message.message,
        "message_type": get_message_type(message),
        "timestamp": message.date.isoformat(),
        "insert_date": datetime.now(timezone.utc).isoformat(),
        "source": "telegram",
        "links": extract_urls(message.message),
        "telegram_url": extract_telegram_url(message.message),
        "views": getattr(message, "views", None),
        "replies": getattr(message, "replies", None).replies
        if getattr(message, "replies", None)
        else None,
        "forwards": getattr(message, "forwards", None),
    }


def initTelegramClient(telegram_config, session_name):
    api_id = int(telegram_config["TELEGRAM_API_ID"])
    api_hash = telegram_config["TELEGRAM_API_HASH"]
    telegramClient = TelegramClient(session_name, api_id, api_hash)
    return telegramClient


async def fetch_messages_async(
    group_id: str,
    last_ts: str | None,
    from_date: str | None,
    to_date: str | None,
    telegram_config: dict[str, str] | None = None,
) -> list[dict[str, str | int | list[str] | None]]:
    """Fetch messages from Telegram group within date range or since last timestamp."""
    client = initTelegramClient(telegram_config, "fetch_session")
    await client.start()

    if not await is_eligible_for_scraping(client, group_id):
        await client.disconnect()
        return []

    # Determine offset_date based on parameters
    offset_date = None
    if from_date:
        if last_ts:
            # Use last_ts if it's after from_date, otherwise use from_date
            last_ts_dt = datetime.fromisoformat(last_ts)
            from_date_dt = datetime.fromisoformat(from_date)
            offset_date = last_ts_dt if last_ts_dt > from_date_dt else from_date_dt
        else:
            offset_date = datetime.fromisoformat(from_date)
    elif last_ts:
        offset_date = datetime.fromisoformat(last_ts)

    messages = []
    try:
        async for message in client.iter_messages(
            entity=group_id, offset_date=offset_date, reverse=True
        ):
            # Filter by to_date if provided
            if to_date and message.date > datetime.fromisoformat(to_date):
                continue
            messages.append(normalize_message(message, group_id))
    except FloodWaitError as e:
        print(f"⏳ Flood wait error while fetching messages: {e}")
        await asyncio.sleep(e.seconds)
        await retry_on_flood(
            func=fetch_messages_async,
            group_id=group_id,
            last_ts=last_ts,
            from_date=from_date,
            to_date=to_date,
            telegram_config=telegram_config,
        )
        await client.disconnect()
    await client.disconnect()
    return messages


def get_last_fetch_time(client, project, dataset, metadata_table, group_id):
    """Get last fetch time for a group from metadata table."""
    table_id = f"{project}.{dataset}.{metadata_table}"
    query = f"""
        SELECT last_fetch_time FROM `{table_id}` WHERE group_id = @group_id ORDER BY last_fetch_time DESC LIMIT 1
    """
    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("group_id", "STRING", str(group_id))
            ]
        ),
    )
    rows = list(job)
    if rows:
        return rows[0]["last_fetch_time"]
    return None


def check_duplicates(client, project, dataset, table, messages):
    """Check for existing messages and return only new ones."""
    if not messages:
        return []
    table_id = f"{project}.{dataset}.{table}"
    message_ids = [msg["message_id"] for msg in messages]
    group_ids = [msg["group_id"] for msg in messages]
    placeholders = ", ".join(
        [f"(@msg_id_{i}, @group_id_{i})" for i in range(len(messages))]
    )
    query = f"""
        SELECT message_id, group_id FROM `{table_id}`
        WHERE (message_id, group_id) IN ({placeholders})
    """
    query_params = []
    for i, (msg_id, group_id) in enumerate(zip(message_ids, group_ids)):
        query_params.extend(
            [
                bigquery.ScalarQueryParameter(f"msg_id_{i}", "STRING", msg_id),
                bigquery.ScalarQueryParameter(f"group_id_{i}", "STRING", group_id),
            ]
        )
    job = client.query(
        query, job_config=bigquery.QueryJobConfig(query_parameters=query_params)
    )
    existing = {(row["message_id"], row["group_id"]) for row in job}
    return [
        msg for msg in messages if (msg["message_id"], msg["group_id"]) not in existing
    ]


def update_metadata(
    client: bigquery.Client,
    project: str,
    dataset: str,
    metadata_table: str,
    group_id: str,
    messages: list[dict[str, str | int | list[str] | None]],
):
    """Update last fetch time for a group."""
    if not messages:
        print(f"No new messages to update for {group_id}")
        return

    last_ts = max(m["timestamp"] for m in messages)
    table_id = f"{project}.{dataset}.{metadata_table}"

    # Check if this is the first time for this group
    check_query = f"""
    SELECT COUNT(*) as count
    FROM `{table_id}`
    WHERE group_id = @group_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("group_id", "STRING", str(group_id))
        ]
    )

    try:
        result = client.query(check_query, job_config=job_config).result()
        row = list(result)[0]
        is_first_time = row.count == 0
    except Exception as e:
        print(f"Warning: Could not check if first time for {group_id}: {e}")
        is_first_time = True

    # After first run, always set is_first_time to false
    is_first_time = False

    # Use MERGE instead of DELETE + INSERT to avoid streaming buffer issues
    merge_query = f"""
    MERGE `{table_id}` AS target
    USING (SELECT @group_id as group_id, @last_fetch_time as last_fetch_time, @is_first_time as is_first_time) AS source
    ON target.group_id = source.group_id
    WHEN MATCHED THEN
      UPDATE SET last_fetch_time = source.last_fetch_time, is_first_time = source.is_first_time
    WHEN NOT MATCHED THEN
      INSERT (group_id, last_fetch_time, is_first_time) VALUES (source.group_id, source.last_fetch_time, source.is_first_time)
    """

    # Convert last_ts string to datetime object for BigQuery
    last_ts_dt = datetime.fromisoformat(last_ts)

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("group_id", "STRING", str(group_id)),
            bigquery.ScalarQueryParameter("last_fetch_time", "TIMESTAMP", last_ts_dt),
            bigquery.ScalarQueryParameter("is_first_time", "BOOL", is_first_time),
        ]
    )

    try:
        client.query(merge_query, job_config=job_config).result()
        print(
            f"Updated metadata for {group_id} to {last_ts}, is_first_time: {is_first_time}"
        )
    except Exception as e:
        raise Exception(f"Metadata update errors: {e}")


def format_telegram_url(url: str | None) -> str | None:
    """Format Telegram URL to https://t.me/ format."""
    if not url:
        return None

    # Remove @ if present
    if url.startswith("@"):
        url = url[1:]

    # If it's already a full URL, return as is
    if url.startswith("https://t.me/") or url.startswith("http://t.me/"):
        return url

    # If it's just a username, add https://t.me/
    if not url.startswith("http"):
        return f"https://t.me/{url}"

    return url


async def update_metadata_from_telegram_urls(
    client: bigquery.Client,
    project: str,
    dataset: str,
    messages_table: str,
    metadata_table: str,
    telegram_config: dict[str, str] | None = None,
):
    """Update telegram_last_ingestion table based on telegram_url values found in messages."""
    try:
        print(
            f"Scanning {project}.{dataset}.{messages_table} for telegram_url values..."
        )

        # First, ensure the is_first_time column exists
        try:
            table = client.get_table(f"{project}.{dataset}.{metadata_table}")
            existing_fields = {field.name for field in table.schema}

            if "is_first_time" not in existing_fields:
                print("Adding is_first_time column to metadata table...")
                new_schema = table.schema + [
                    bigquery.SchemaField("is_first_time", "BOOLEAN", mode="REQUIRED")
                ]
                table.schema = new_schema
                client.update_table(table, ["schema"])
                print("Successfully added is_first_time column")
            else:
                print("is_first_time column already exists")
        except Exception as e:
            print(f"Warning: Could not check/add is_first_time column: {e}")

        # Get all unique telegram_url values from messages table
        telegram_urls_query = f"""
        SELECT DISTINCT telegram_url
        FROM `{project}.{dataset}.{messages_table}`
        WHERE telegram_url IS NOT NULL AND telegram_url != ''
        """

        urls_job = client.query(telegram_urls_query)
        telegram_urls = [row.telegram_url for row in urls_job.result()]

        if not telegram_urls:
            print("No telegram_url values found in messages table")
            return

        print(f"Found {len(telegram_urls)} unique Telegram URLs:\n{telegram_urls}\n")

        # Format URLs and filter valid ones
        formatted_urls = []
        for url in telegram_urls:
            formatted_url = format_telegram_url(url)
            if formatted_url and formatted_url.startswith("https://t.me/"):
                formatted_urls.append(formatted_url)

        print(f"Formatted URLs:\n{formatted_urls}\n")

        # Get existing groups from metadata table
        existing_groups_query = f"""
        SELECT DISTINCT group_id
        FROM `{project}.{dataset}.{metadata_table}`
        """
        existing_job = client.query(existing_groups_query)
        existing_groups = {row.group_id for row in existing_job.result()}
        print(f"Existing groups in metadata table:\n{existing_groups}\n")

        # Find new groups to add
        new_groups = [url for url in formatted_urls if url not in existing_groups]
        print(
            f"New groups to add to {project}.{dataset}.{metadata_table}:\n{new_groups}\n"
        )

        # Add new groups to metadata table using MERGE
        current_time = datetime.now(timezone.utc)
        telegramClient = initTelegramClient(telegram_config, "entity_checking_session")
        await telegramClient.start()

        for group_id in new_groups:
            if not await is_eligible_for_scraping(telegramClient, group_id):
                continue

            print(
                f"Inserting into {project}.{dataset}.{metadata_table}: group_id={group_id}, last_fetch_time={current_time}, is_first_time=true"
            )

            merge_query = f"""
            MERGE `{project}.{dataset}.{metadata_table}` AS target
            USING (SELECT @group_id as group_id, @last_fetch_time as last_fetch_time) AS source
            ON target.group_id = source.group_id
            WHEN NOT MATCHED THEN
              INSERT (group_id, last_fetch_time) VALUES (source.group_id, source.last_fetch_time)
            """

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("group_id", "STRING", group_id),
                    bigquery.ScalarQueryParameter(
                        "last_fetch_time", "TIMESTAMP", current_time
                    ),
                ]
            )

            client.query(merge_query, job_config).result()
            print(
                f"✅ Successfully added new group: {group_id} with is_first_time = true\n"
            )

            # Fetch some initial messages to avoid empty groups
            messages = await fetch_messages_async(
                group_id,
                None,
                None,
                None,
                telegram_config,
            )
            print(f"Fetched {len(messages)} messages from {group_id}")
            if messages:
                handle_new_messages(
                    messages, group_id, client, project, dataset, messages_table
                )

        await telegramClient.disconnect()
        if new_groups:
            print(
                f"🎉 Successfully added {len(new_groups)} new groups to {project}.{dataset}.{metadata_table}\n"
            )
        else:
            print(
                "ℹ️ No new groups to add - all Telegram URLs already exist in metadata table\n"
            )

    except Exception as e:
        print(f"❌ Error updating metadata from telegram_urls: {e}\n")
        raise


def handle_new_messages(
    messages: list[dict[str, str | int | list[str] | None]],
    entity_id: str,
    bg_client: bigquery.Client,
    bq_project: str,
    bq_dataset: str,
    bq_table: str,
) -> int:
    """Handle insertion of new messages into BigQuery after duplicate check."""
    new_messages = check_duplicates(
        bg_client, bq_project, bq_dataset, bq_table, messages
    )
    print(f"Found {len(new_messages)} new messages (after duplicate check)")
    if new_messages:
        table_id = f"{bq_project}.{bq_dataset}.{bq_table}"
        errors = bg_client.insert_rows_json(table_id, new_messages)
        if errors:
            raise Exception(f"BigQuery insert errors: {errors}")
        print(f"✅ Inserted {len(new_messages)} messages for {entity_id}")

        return len(new_messages)
    return 0


def ingest_telegram_to_bq(
    tg_entities_data: list[dict[str, str | None]],
    bq_project: str,
    bq_dataset: str,
    bq_table: str = "telegram_messages",
    bq_metadata_table: str | None = "telegram_last_ingestion",
    telegram_config: dict[str, str | None] | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
) -> int:
    """
    Ingests messages from Telegram groups into BigQuery with duplicate checks and metadata tracking.

    Args:
        group_ids: List of Telegram group IDs or links
        bq_project: BigQuery project ID
        bq_dataset: BigQuery dataset name
        bq_table: BigQuery table name for messages
        bq_metadata_table: BigQuery table name for metadata
        telegram_config: Telegram API configuration dict
        from_date: Start date in ISO format (if None, will use metadata table)
        to_date: End date in ISO format (default: now)
    """
    load_dotenv()
    if telegram_config is None:
        api_id = os.getenv("TELEGRAM_API_ID")
        api_hash = os.getenv("TELEGRAM_API_HASH")
        if not api_id or not api_hash:
            raise ValueError(
                "TELEGRAM_API_ID and TELEGRAM_API_HASH must be set in environment variables"
            )
        telegram_config = {
            "TELEGRAM_API_ID": api_id,
            "TELEGRAM_API_HASH": api_hash,
        }

    # Set default end date if not provided
    if to_date is None:
        to_date = datetime.now(timezone.utc).isoformat()

    # print(f"Date range: {from_date or 'metadata table'} to {to_date}")

    bg_client = bigquery.Client(project=bq_project)
    schema = [
        bigquery.SchemaField("message_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("group_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("sender_id", "STRING"),
        bigquery.SchemaField("sender_name", "STRING"),
        bigquery.SchemaField("message_text", "STRING"),
        bigquery.SchemaField("message_type", "STRING"),
        bigquery.SchemaField("timestamp", "TIMESTAMP"),
        bigquery.SchemaField("insert_date", "TIMESTAMP"),
        bigquery.SchemaField("source", "STRING"),
        bigquery.SchemaField("links", "STRING", mode="REPEATED"),
        bigquery.SchemaField("telegram_url", "STRING"),
        bigquery.SchemaField("views", "INTEGER"),
        bigquery.SchemaField("replies", "INTEGER"),
        bigquery.SchemaField("forwards", "INTEGER"),
    ]
    ensure_bq_table(bg_client, bq_project, bq_dataset, bq_table, schema)
    ensure_metadata_table(bg_client, bq_project, bq_dataset, bq_metadata_table)
    total_inserted = 0
    for entity in tg_entities_data:
        print(f"\n\nProcessing entity: {entity['id']}")

        try:
            print(
                f"Fetching messages for {entity['id']} from {from_date or entity['last_fetch_time'] or 'beginning'} to {to_date}"
            )
            messages = asyncio.run(
                fetch_messages_async(
                    entity["id"],
                    entity["last_fetch_time"],
                    from_date,
                    to_date,
                    telegram_config,
                )
            )
            print(f"Fetched {len(messages)} messages from {entity['id']}")
            if messages:
                inserted = handle_new_messages(
                    messages, entity["id"], bg_client, bq_project, bq_dataset, bq_table
                )
                total_inserted += inserted

                # Update metadata regardless of whether new messages were inserted
                update_metadata(
                    bg_client,
                    bq_project,
                    bq_dataset,
                    bq_metadata_table,
                    entity["id"],
                    messages,
                )
        except Exception as e:
            print(f"Error processing entity {entity['id']}: {e}")
    print(f"Total messages inserted: {total_inserted}")

    # Update metadata table based on telegram_url values found in messages
    print("\n\nUpdating metadata table based on telegram_url values...")
    asyncio.run(
        update_metadata_from_telegram_urls(
            bg_client,
            bq_project,
            bq_dataset,
            bq_table,
            bq_metadata_table,
            telegram_config,
        )
    )

    return total_inserted
