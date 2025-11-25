# Docker Setup and Usage

## Prerequisites

1. **Docker**: Make sure Docker is installed on your system
2. **Environment Variables**: Copy `.env.example` to `.env` and fill in your values:
   ```bash
   cp .env.example .env
   # Edit .env with your actual values
   ```

3. **Google Cloud Credentials**:
   - If using service account JSON file, place it as `credentials.json` in the project root
   - Or use other GCP authentication methods (see below)

## Building and Running

### Option 1: Using Docker directly

```bash
# Build the image
docker build -t telegram-scraper .

# Run the container
docker run -d \
  --name telegram-scraper \
  --env-file .env \
  -v $(pwd)/sessions:/app/sessions \
  -v $(pwd)/logs:/app/logs \
  -v $(pwd)/credentials.json:/app/credentials.json:ro \
  telegram-scraper
```

### Option 2: Using Docker Compose (Recommended)

```bash
# Build and run
docker-compose up -d

# View logs
docker-compose logs -f

# Stop
docker-compose down
```

## Running with Arguments

You can pass arguments to the main.py script:

```bash
# Using docker run
docker run --rm \
  --env-file .env \
  -v $(pwd)/sessions:/app/sessions \
  telegram-scraper \
  python main.py --from-date "2024-01-01" --to-date "2024-01-31"

# Using docker-compose
docker-compose run --rm telegram-scraper python main.py --help
```

## Authentication Options

### Google Cloud Authentication

1. **Service Account File** (Recommended for containers):
   ```bash
   # Place your service account JSON file as credentials.json
   export GOOGLE_APPLICATION_CREDENTIALS=./credentials.json
   ```

2. **Application Default Credentials** (for development):
   ```bash
   # If running on GCP (Cloud Run, GKE, etc.)
   # No additional setup needed
   ```

3. **Using gcloud auth**:
   ```bash
   # Mount your gcloud config
   docker run -v ~/.config/gcloud:/home/appuser/.config/gcloud:ro ...
   ```

### Telegram Authentication

The app will create session files in the `sessions/` directory. These files persist your Telegram authentication so you don't need to re-authenticate every time.

## Monitoring and Debugging

### View logs
```bash
docker-compose logs -f telegram-scraper
```

### Execute commands inside the container
```bash
docker-compose exec telegram-scraper bash
```

### Check container health
```bash
docker-compose ps
```

## Production Deployment

### Cloud Run Deployment

1. **Build and push to Google Container Registry**:
   ```bash
   # Set your project ID
   export PROJECT_ID="your-gcp-project"

   # Build and tag
   docker build -t gcr.io/$PROJECT_ID/telegram-scraper .

   # Push to GCR
   docker push gcr.io/$PROJECT_ID/telegram-scraper
   ```

2. **Deploy to Cloud Run**:
   ```bash
   gcloud run deploy telegram-scraper \
     --image gcr.io/$PROJECT_ID/telegram-scraper \
     --platform managed \
     --region us-central1 \
     --set-env-vars BQ_PROJECT_ID=$PROJECT_ID \
     --set-env-vars BQ_DATASET=your_dataset \
     --set-env-vars BQ_TABLE=your_table \
     --allow-unauthenticated
   ```

### Environment Variables for Production

Make sure to set these environment variables in your production environment:

- `TELEGRAM_API_ID`
- `TELEGRAM_API_HASH`
- `TELEGRAM_BOT_TOKEN`
- `BQ_PROJECT_ID`
- `BQ_DATASET`
- `BQ_TABLE`
- `BQ_METADATA_TABLE`

## Troubleshooting

### Common Issues

1. **Permission Denied**: Make sure the appuser has write access to mounted volumes
2. **Session Files**: Telegram session files need to persist between container restarts
3. **Memory Issues**: Increase memory limits in docker-compose.yml if needed
4. **Network Issues**: Ensure the container can reach both Telegram and Google APIs

### Debug Mode

Run with debug logging:
```bash
docker-compose run --rm -e LOG_LEVEL=DEBUG telegram-scraper python main.py
```
