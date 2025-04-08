# --- scheduler_service/config.py ---
import os
from dotenv import load_dotenv

load_dotenv()

# URL for the Configuration Service (provided by ACA environment variables)
CONFIG_SERVICE_URL = os.getenv("CONFIG_SERVICE_URL")
# URL for the Flow Orchestrator Service (provided by ACA environment variables)
ORCHESTRATOR_SERVICE_URL = os.getenv("ORCHESTRATOR_SERVICE_URL")
# How often to reload schedules from the Config Service (in minutes)
SCHEDULE_REFRESH_MINUTES = int(os.getenv("SCHEDULE_REFRESH_MINUTES", "60"))

if not CONFIG_SERVICE_URL or not ORCHESTRATOR_SERVICE_URL:
    print("Warning: CONFIG_SERVICE_URL or ORCHESTRATOR_SERVICE_URL environment variables not set.")

# --- scheduler_service/services/scheduling_service.py ---
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.memory import MemoryJobStore
import httpx
import asyncio
import logging
from config import CONFIG_SERVICE_URL, ORCHESTRATOR_SERVICE_URL, SCHEDULE_REFRESH_MINUTES

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure APScheduler
jobstores = {
    'default': MemoryJobStore()
}
scheduler = AsyncIOScheduler(jobstores=jobstores, timezone="UTC") # Use UTC for consistency

async def trigger_flow(http_client: httpx.AsyncClient, flow_id: str, group_name: str):
    """Calls the Flow Orchestrator to start a specific flow run."""
    if not ORCHESTRATOR_SERVICE_URL:
        logger.error("Orchestrator service URL not configured. Cannot trigger flow.")
        return

    url = f"{ORCHESTRATOR_SERVICE_URL}/api/v1/orchestrator/run/flow"
    payload = {"flow_id": flow_id, "group_name": group_name}
    try:
        response = await http_client.post(url, json=payload, timeout=30.0)
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        logger.info(f"Successfully triggered flow {group_name}/{flow_id}. Orchestrator response: {response.json()}")
    except httpx.RequestError as exc:
        logger.error(f"Request error calling orchestrator for flow {group_name}/{flow_id}: {exc}")
    except httpx.HTTPStatusError as exc:
        logger.error(f"Orchestrator returned error for flow {group_name}/{flow_id}: Status {exc.response.status_code}, Response: {exc.response.text}")
    except Exception as exc:
         logger.error(f"Unexpected error triggering flow {group_name}/{flow_id}: {exc}")

async def trigger_group_flows(group_name: str, flow_ids: list[str]):
    """Triggers all specified flows within a group asynchronously."""
    logger.info(f"CRON triggered for group: '{group_name}'. Triggering flows: {flow_ids}")
    async with httpx.AsyncClient() as client:
        tasks = [trigger_flow(client, flow_id, group_name) for flow_id in flow_ids]
        await asyncio.gather(*tasks) # Run flow triggers concurrently

async def load_and_schedule_jobs():
    """Fetches schedules from Config Service and adds/updates jobs in APScheduler."""
    logger.info(f"Attempting to load schedules from {CONFIG_SERVICE_URL}...")
    if not CONFIG_SERVICE_URL:
        logger.error("Configuration Service URL not set. Cannot load schedules.")
        return

    schedules = []
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(f"{CONFIG_SERVICE_URL}/api/v1/config/schedules?enabled_only=true", timeout=20.0)
            response.raise_for_status()
            schedules = response.json() # Assumes response is List[ScheduleConfig]
            logger.info(f"Successfully fetched {len(schedules)} enabled schedules.")
        except httpx.RequestError as exc:
            logger.error(f"Could not fetch schedules from Config Service: {exc}")
            return # Stop if config is unavailable
        except Exception as e:
            logger.error(f"Error processing schedules response: {e}")
            return

    # Sync schedules: Add new/updated ones, remove old ones
    current_job_ids = {job.id for job in scheduler.get_jobs()}
    fetched_job_ids = set()

    for schedule in schedules:
        job_id = schedule.get("RowKey") # Unique ID for the schedule job
        group_name = schedule.get("GroupName")
        cron_expression = schedule.get("CronExpression")
        flow_ids_str = schedule.get("FlowIDs") # e.g., "flow1,flow2"

        if not all([job_id, group_name, cron_expression, flow_ids_str]):
             logger.warning(f"Skipping schedule with missing required fields: {job_id or 'Unknown ID'}")
             continue

        fetched_job_ids.add(job_id)
        flow_ids = [f.strip() for f in flow_ids_str.split(',') if f.strip()]
        if not flow_ids:
             logger.warning(f"Skipping schedule '{job_id}' with no valid FlowIDs.")
             continue

        try:
            # Add or update the job
            scheduler.add_job(
                trigger_group_flows,
                CronTrigger.from_crontab(cron_expression, timezone="UTC"), # Ensure UTC
                args=[group_name, flow_ids],
                id=job_id,
                name=f"Group_{group_name}",
                replace_existing=True, # Update if job_id already exists
                misfire_grace_time=300 # Allow 5 minutes grace period if scheduler was down
            )
            # logger.info(f"Scheduled/Updated job '{job_id}' for group '{group_name}'")
        except ValueError as e:
            logger.error(f"Invalid CRON expression '{cron_expression}' for job '{job_id}': {e}")
        except Exception as e:
            logger.error(f"Failed to schedule job '{job_id}': {e}")

    # Remove jobs that are no longer in the fetched configuration
    jobs_to_remove = current_job_ids - fetched_job_ids
    for job_id in jobs_to_remove:
        try:
            scheduler.remove_job(job_id)
            logger.info(f"Removed obsolete scheduled job '{job_id}'.")
        except Exception as e:
            logger.error(f"Error removing job '{job_id}': {e}")

    logger.info("Schedule loading complete.")


async def schedule_reloader():
    """Periodically calls load_and_schedule_jobs based on refresh interval."""
    while True:
        await load_and_schedule_jobs()
        await asyncio.sleep(SCHEDULE_REFRESH_MINUTES * 60)

async def start_scheduler_service():
    """Starts the APScheduler and the periodic reloader."""
    if scheduler.state == 0: # 0 = STATE_STOPPED
        # Load schedules immediately on startup
        await load_and_schedule_jobs()
        # Start the scheduler itself
        scheduler.start()
        logger.info("APScheduler started.")
        # Start the background task to reload schedules periodically
        asyncio.create_task(schedule_reloader())
        logger.info(f"Scheduled periodic reload every {SCHEDULE_REFRESH_MINUTES} minutes.")
    else:
        logger.warning("Scheduler already running or starting.")

# --- scheduler_service/main.py ---
from fastapi import FastAPI
import asyncio
import logging
from services.scheduling_service import start_scheduler_service, scheduler, load_and_schedule_jobs

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="ETL Scheduler Service",
    description="Triggers ETL flows based on schedules defined in the Configuration Service.",
    version="1.0.0"
)

@app.on_event("startup")
async def startup_event():
    logger.info("Scheduler Service starting up...")
    # Run the scheduler startup logic in the background
    asyncio.create_task(start_scheduler_service())

@app.on_event("shutdown")
def shutdown_event():
    logger.info("Scheduler Service shutting down...")
    if scheduler.running:
        scheduler.shutdown()
    logger.info("Scheduler stopped.")

@app.get("/health", tags=["Health"])
def read_health():
    """Health check endpoint."""
    if scheduler.running:
        return {"status": "healthy", "scheduler_state": "running"}
    else:
        # Consider unhealthy if scheduler failed to start/run
        return {"status": "unhealthy", "scheduler_state": str(scheduler.state)}

@app.post("/reload_schedules", status_code=202, tags=["Actions"])
async def reload_schedules_endpoint():
    """Manually triggers a reload of schedules from the Configuration Service."""
    logger.info("Manual schedule reload requested via API.")
    # Run reload in background to avoid blocking the request
    asyncio.create_task(load_and_schedule_jobs())
    return {"message": "Schedule reload initiated."}

@app.get("/jobs", tags=["Debug"])
def get_scheduled_jobs():
    """Lists the currently scheduled jobs (for debugging purposes)."""
    if not scheduler.running:
        return {"error": "Scheduler not running"}

    jobs = []
    for job in scheduler.get_jobs():
        jobs.append({
            "id": job.id,
            "name": job.name,
            "trigger": str(job.trigger),
            "next_run_time": str(job.next_run_time) if job.next_run_time else None
        })
    return {"jobs": jobs}

@app.get("/", tags=["Root"])
def read_root():
    """Root endpoint providing basic service info."""
    return {"message": "Welcome to the ETL Scheduler Service"}

if __name__ == "__main__":
    import uvicorn
    # Run directly for local testing (uvicorn main:app --reload)
    # Note: Running with --reload might interfere with APScheduler persistence or cause duplicate jobs briefly.
    # Better to run without reload for scheduler testing: uvicorn main:app
    uvicorn.run(app, host="0.0.0.0", port=8001) # Use a different port locally if needed


# --- scheduler_service/requirements.txt ---
fastapi
uvicorn[standard]
python-dotenv
httpx # For making API calls to other services
APScheduler # For scheduling jobs

# --- scheduler_service/Dockerfile ---
# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set environment variables for Python
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set the working directory in the container
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Expose the port the app runs on
EXPOSE 80

# Command to run the application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]
