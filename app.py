"""FastAPI wrapper for Cloud Run deployment."""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, BackgroundTasks, HTTPException
from loguru import logger

from main import main as run_pipeline


# Track if a job is currently running
job_running = False
last_run_result: dict | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    logger.info("Telegram Scraper service starting up...")
    yield
    logger.info("Telegram Scraper service shutting down...")


app = FastAPI(
    title="Telegram Scraper",
    description="Scrapes Telegram messages and ingests them to BigQuery",
    version="1.0.0",
    lifespan=lifespan,
)


def run_scraping_job():
    """Run the scraping job synchronously."""
    global job_running, last_run_result

    job_running = True
    start_time = datetime.now(timezone.utc)

    try:
        logger.info("Starting Telegram scraping job...")
        run_pipeline()

        last_run_result = {
            "status": "success",
            "started_at": start_time.isoformat(),
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }
        logger.info("Telegram scraping job completed successfully")

    except Exception as e:
        logger.error(f"Telegram scraping job failed: {e}")
        last_run_result = {
            "status": "failed",
            "started_at": start_time.isoformat(),
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "error": str(e),
        }
    finally:
        job_running = False


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "telegram-scraper",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/status")
async def get_status():
    """Get the current status of the scraper."""
    return {
        "job_running": job_running,
        "last_run": last_run_result,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.post("/run")
async def trigger_run(background_tasks: BackgroundTasks):
    """Trigger a scraping run. Called by Cloud Scheduler."""
    global job_running

    if job_running:
        raise HTTPException(
            status_code=409,
            detail="A scraping job is already running"
        )

    logger.info("Received request to start scraping job")
    background_tasks.add_task(run_scraping_job)

    return {
        "status": "started",
        "message": "Scraping job started in background",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
