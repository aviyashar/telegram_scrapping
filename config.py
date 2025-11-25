import os
from dotenv import load_dotenv


def load_config() -> dict[str, str | None]:
    """Load configuration from environment variables."""
    load_dotenv()

    return {
        # Telegram API credentials
        'TELEGRAM_API_ID': os.getenv('TELEGRAM_API_ID'),
        'TELEGRAM_API_HASH': os.getenv('TELEGRAM_API_HASH'),

        # GCP credentials (optional if using gcloud auth)
        'GOOGLE_APPLICATION_CREDENTIALS': os.getenv('GOOGLE_APPLICATION_CREDENTIALS'),

        # BigQuery configuration
        'BQ_PROJECT_ID': os.getenv('BQ_PROJECT_ID'),
        'BQ_DATASET': os.getenv('BQ_DATASET', 'telegram'),
        'BQ_TABLE': os.getenv('BQ_TABLE', 'telegram_messages'),
        'BQ_METADATA_TABLE': os.getenv('BQ_METADATA_TABLE', 'telegram_last_ingestion'),
        'BQ_GROUPS_TABLE': os.getenv('BQ_GROUPS_TABLE', 'groups'),
    }


def validate_config(config: dict[str, str | None]) -> bool:
    """Validate that required configuration values are set."""
    required_fields = ['TELEGRAM_API_ID', 'TELEGRAM_API_HASH', 'BQ_PROJECT_ID', 'BQ_DATASET']

    missing = [field for field in required_fields if not config.get(field)]

    if missing:
        raise ValueError(f"Missing required configuration: {', '.join(missing)}")

    return True 