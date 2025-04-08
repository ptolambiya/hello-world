from fastapi import FastAPI
import asyncio
from services.kafka_listener import load_and_start_listeners, stop_listeners

app = FastAPI(title="Kafka Listener Service")

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(load_and_start_listeners()) # Run listeners in background

@app.on_event("shutdown")
async def shutdown_event():
   await stop_listeners()

@app.get("/")
def read_root():
    return {"message": "Kafka Listener Service Running"}

@app.post("/reload_listeners", status_code=202)
async def reload_listeners_endpoint():
    """API endpoint to manually trigger listener reload."""
    asyncio.create_task(load_and_start_listeners()) # This will stop old and start new
    return {"message": "Kafka listener reload initiated."}
