import os
from dotenv import load_dotenv


def load_config():
    load_dotenv()
    return {
        'TELEGRAM_API_ID': os.getenv('TELEGRAM_API_ID'),
        'TELEGRAM_API_HASH': os.getenv('TELEGRAM_API_HASH'),
        'TELEGRAM_BOT_TOKEN': os.getenv('TELEGRAM_BOT_TOKEN'),
        'GOOGLE_APPLICATION_CREDENTIALS': os.getenv('GOOGLE_APPLICATION_CREDENTIALS'),
        'BQ_PROJECT_ID': os.getenv('BQ_PROJECT_ID'),
        'BQ_DATASET': os.getenv('BQ_DATASET'),
        'BQ_TABLE': os.getenv('BQ_TABLE'),
        'BQ_METADATA_TABLE': os.getenv('BQ_METADATA_TABLE'),
    } 