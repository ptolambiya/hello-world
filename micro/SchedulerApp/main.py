from fastapi import FastAPI
import asyncio
from services.scheduling_service import start_scheduler, scheduler, load_and_schedule_jobs

app = FastAPI(title="Scheduler Service")

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(start_scheduler()) # Run scheduler in background

@app.on_event("shutdown")
def shutdown_event():
    scheduler.shutdown()

@app.get("/")
def read_root():
    return {"message": "Scheduler Service Running"}

@app.post("/reload_schedules", status_code=202)
async def reload_schedules_endpoint():
    """API endpoint to manually trigger schedule reload."""
    asyncio.create_task(load_and_schedule_jobs())
    return {"message": "Schedule reload initiated."}

@app.get("/jobs")
def get_jobs():
    """List currently scheduled jobs (for debugging)."""
    jobs = []
    for job in scheduler.get_jobs():
        jobs.append({
            "id": job.id,
            "name": job.name,
            "next_run_time": str(job.next_run_time)
        })
    return {"jobs": jobs}
