from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
from telethon.errors import SessionPasswordNeededError
from datetime import datetime, timezone
import asyncio


def parse_iso_datetime(dt_str):
    dt = datetime.fromisoformat(dt_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def get_message_type(message):
    if message.media:
        if isinstance(message.media, MessageMediaPhoto):
            return 'image'
        elif isinstance(message.media, MessageMediaDocument):
            # Could be video, sticker, etc. Check mime type
            if hasattr(message.media.document, 'mime_type'):
                mime = message.media.document.mime_type
                if mime.startswith('video'):
                    return 'video'
                elif mime.startswith('image'):
                    return 'image'
                elif mime == 'application/x-tgsticker':
                    return 'sticker'
                else:
                    return 'other'
            return 'other'
        else:
            return 'other'
    return 'text'


def get_media_url(message):
    # For now, just return None. You can implement upload to GCS or similar if needed.
    return None


def normalize_message(message, group_id):
    return {
        'group_id': str(group_id),
        'message_id': message.id,
        'sender_id': str(message.sender_id) if message.sender_id else None,
        'sender_name': getattr(message.sender, 'first_name', None) if hasattr(message, 'sender') else None,
        'message_text': message.message,
        'message_type': get_message_type(message),
        'timestamp': message.date.isoformat(),
        'views': getattr(message, 'views', None),
        'replies': getattr(message, 'replies', None).replies if getattr(message, 'replies', None) else None,
        'forwards': getattr(message, 'forwards', None),
        'media_url': get_media_url(message),
        'ingestion_time': datetime.now(timezone.utc).isoformat(),
    }


async def fetch_messages_async(group_id, last_ts, from_time, to_time, config):
    api_id = int(config['TELEGRAM_API_ID'])
    api_hash = config['TELEGRAM_API_HASH']
    session_name = 'telegram_session'
    client = TelegramClient(session_name, api_id, api_hash)
    # This will prompt for phone number and code on first run
    await client.start()
    # Determine offset_date
    offset_date = None
    if from_time:
        offset_date = parse_iso_datetime(from_time)
    elif last_ts:
        offset_date = parse_iso_datetime(last_ts)
    # Fetch messages
    messages = []
    async for message in client.iter_messages(entity=group_id, offset_date=offset_date, reverse=True):
        if to_time and message.date > parse_iso_datetime(to_time):
            continue
        messages.append(normalize_message(message, group_id))
    await client.disconnect()
    return messages


def fetch_new_messages(group_id, last_ts, from_time, to_time, config):
    """
    Fetch new messages from a Telegram group since last_ts (or from_time if provided).
    Returns a list of normalized message dicts.
    """
    return asyncio.run(fetch_messages_async(group_id, last_ts, from_time, to_time, config)) 