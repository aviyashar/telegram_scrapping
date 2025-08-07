# Telegram Groups Data Pipeline to BigQuery

## Overview
This project fetches new messages from specified Telegram groups and loads them into a BigQuery table, maintaining metadata for incremental ingestion.

## Features
- Incremental fetch of new messages from Telegram groups
- Data normalization and transformation
- Append to BigQuery with schema enforcement
- Metadata table for last ingestion timestamps
- Logging and error handling
- Docker-ready for Cloud Run/Cloud Functions

## Setup
1. Clone the repo and install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Copy `.env.example` to `.env` and fill in your credentials.
3. Ensure your GCP service account has BigQuery permissions.

## Usage
Run the pipeline with:
```bash
python main.py --groups group1,group2 --from 2024-01-01T00:00:00 --to 2024-01-02T00:00:00
```
- `--groups`: Comma-separated list of group IDs or links
- `--from`/`--to`: Optional ISO timestamps for time window

## Deployment
- Use the provided Dockerfile for containerization.
- Schedule with Cloud Scheduler + Cloud Run/Functions.

## Schema
See `schema/telegram_messages.json` for BigQuery table schema.

## License
MIT