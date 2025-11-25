"""Simple script to authenticate with Telegram and create a session file."""
import asyncio
from telethon import TelegramClient
from config import load_config

async def main():
    config = load_config()
    api_id = int(config['TELEGRAM_API_ID'])
    api_hash = config['TELEGRAM_API_HASH']

    client = TelegramClient('fetch_session', api_id, api_hash)

    print("Starting Telegram authentication...")
    print("You will be prompted for your phone number and verification code.")

    await client.start()

    me = await client.get_me()
    print(f"\nSuccessfully authenticated as: {me.first_name} ({me.username})")
    print("Session saved to fetch_session.session")

    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
