# --- flow_orchestrator_service/config.py ---
import os
from dotenv import load_dotenv

load_dotenv()

# URLs for the services the orchestrator calls
CONFIG_SERVICE_URL = os.getenv("CONFIG_SERVICE_URL")
EXTRACTOR_SERVICE_URL = os.getenv("EXTRACTOR_SERVICE_URL")
PARSER_SERVICE_URL = os.getenv("PARSER_SERVICE_URL")
LOADER_SERVICE_URL = os.getenv("LOADER_SERVICE_URL")

# Basic validation
if not all([CONFIG_SERVICE_URL, EXTRACTOR_SERVICE_URL, PARSER_SERVICE_URL, LOADER_SERVICE_URL]):
    print("Warning: One or more dependent service URLs are not set.")

# Default timeouts for calling other services (in seconds)
HTTP_TIMEOUT_CONFIG = float(os.getenv("HTTP_TIMEOUT_CONFIG", "30.0"))
HTTP_TIMEOUT_EXTRACT = float(os.getenv("HTTP_TIMEOUT_EXTRACT", "300.0")) # Can take longer
HTTP_TIMEOUT_PARSE = float(os.getenv("HTTP_TIMEOUT_PARSE", "300.0")) # Can take longer
HTTP_TIMEOUT_LOAD = float(os.getenv("HTTP_TIMEOUT_LOAD", "600.0")) # Can take longer

# --- flow_orchestrator_service/models/flow_models.py ---
from pydantic import BaseModel, Field

class FlowRunRequest(BaseModel):
    flow_id: str = Field(..., description="Unique identifier for the flow configuration")
    group_name: str = Field(..., description="The group this flow belongs to (used as PartitionKey in config)")

class KafkaFlowRunRequest(FlowRunRequest):
    message_data: str = Field(..., description="The raw message content received from Kafka")

class OrchestrationResult(BaseModel):
    status: str = "initiated" # Can be initiated, running, success, failed
    message: str | None = None
    flow_id: str
    group_name: str
    raw_data_location: str | None = None
    processed_data_location: str | None = None
    load_result: dict | None = None

#routers/orchestrator_router.py
from fastapi import APIRouter, HTTPException, BackgroundTasks, status, Request
import httpx
import logging
from config import (
    CONFIG_SERVICE_URL, EXTRACTOR_SERVICE_URL, PARSER_SERVICE_URL,
    LOADER_SERVICE_URL, DEFAULT_REQUEST_TIMEOUT
)
from models.flow_models import FlowRunRequest, KafkaFlowRunRequest, OrchestrationResult

# Setup basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

router = APIRouter()

async def execute_flow_steps(result: OrchestrationResult):
    """The core logic to run a flow's steps. Updates result object."""
    flow_id = result.flow_id
    group_name = result.group_name
    kafka_message_data = result.message # Will be set if Kafka flow

    logger.info(f"[{group_name}/{flow_id}] Orchestration started.")
    result.status = "running"
    flow_config = None
    source_type = None

    try:
        async with httpx.AsyncClient(timeout=DEFAULT_REQUEST_TIMEOUT) as client:

            # 1. Get Flow Configuration
            try:
                config_url = f"{CONFIG_SERVICE_URL}/api/v1/config/flows/{group_name}/{flow_id}"
                logger.info(f"[{group_name}/{flow_id}] Requesting config from: {config_url}")
                response = await client.get(config_url)
                response.raise_for_status() # Raises HTTPStatusError for 4xx/5xx
                flow_config = response.json()
                source_type = flow_config.get("SourceType", "").upper()
                logger.info(f"[{group_name}/{flow_id}] Fetched config. SourceType: {source_type}")
            except httpx.RequestError as e:
                logger.error(f"[{group_name}/{flow_id}] Network error getting config: {e.request.url} - {e}")
                result.status = "failed"
                result.message = f"Network error getting config: {e}"
                return
            except httpx.HTTPStatusError as e:
                logger.error(f"[{group_name}/{flow_id}] Config service error ({e.response.status_code}): {e.response.text}")
                result.status = "failed"
                result.message = f"Config service error ({e.response.status_code}): {e.response.text}"
                return
            except Exception as e:
                logger.exception(f"[{group_name}/{flow_id}] Unexpected error getting config")
                result.status = "failed"
                result.message = f"Unexpected error getting config: {e}"
                return

            # Validate essential config parts
            source_config = flow_config.get("SourceConfig", {})
            mapping_config = flow_config.get("MappingConfig", {})
            destination_config = flow_config.get("DestinationConfig", {})

            if not source_type or not source_config or not destination_config:
                 logger.error(f"[{group_name}/{flow_id}] Invalid flow config received: Missing SourceType, SourceConfig or DestinationConfig")
                 result.status = "failed"
                 result.message = "Invalid flow configuration"
                 return

            # --- Step 2: Extract Data (Skip for Kafka) ---
            if source_type != "KAFKA":
                try:
                    extract_url = f"{EXTRACTOR_SERVICE_URL}/api/v1/extract"
                    payload = {
                        "flow_id": flow_id,
                        "group_name": group_name, # Pass context
                        "source_type": source_type,
                        "source_config": source_config
                    }
                    logger.info(f"[{group_name}/{flow_id}] Calling Extractor: {extract_url}")
                    response = await client.post(extract_url, json=payload)
                    response.raise_for_status()
                    extract_result = response.json()
                    result.raw_data_location = extract_result.get("data_location")

                    if extract_result.get("status") != "success":
                         raise Exception(f"Extractor service reported failure: {extract_result.get('message', 'Unknown extractor error')}")

                    if result.raw_data_location:
                         logger.info(f"[{group_name}/{flow_id}] Extractor finished. Raw data at: {result.raw_data_location}")
                    else:
                         # Handle case where extractor succeeded but produced no data (e.g., empty query result)
                         logger.info(f"[{group_name}/{flow_id}] Extractor finished successfully but no data was extracted.")
                         result.status = "success" # Flow finished, just nothing to load
                         result.message = "Extraction successful, but no data found to process."
                         # WE MIGHT WANT TO STOP HERE unless empty files need processing downstream
                         return

                except httpx.RequestError as e:
                    logger.error(f"[{group_name}/{flow_id}] Network error calling Extractor: {e.request.url} - {e}")
                    result.status = "failed"
                    result.message = f"Network error calling Extractor: {e}"
                    return
                except httpx.HTTPStatusError as e:
                    logger.error(f"[{group_name}/{flow_id}] Extractor service error ({e.response.status_code}): {e.response.text}")
                    result.status = "failed"
                    result.message = f"Extractor service error ({e.response.status_code}): {e.response.text}"
                    return
                except Exception as e:
                    logger.exception(f"[{group_name}/{flow_id}] Unexpected error during Extraction")
                    result.status = "failed"
                    result.message = f"Unexpected error during Extraction: {e}"
                    return

            # --- Step 3: Parse/Transform Data ---
            try:
                parse_url = f"{PARSER_SERVICE_URL}/api/v1/parse"
                payload = {
                    "flow_id": flow_id,
                    "group_name": group_name,
                    "source_type": source_type, # Hint for parser
                    "mapping_config": mapping_config
                }
                if source_type == "KAFKA":
                    payload["raw_data"] = kafka_message_data # Pass Kafka message directly
                else:
                    payload["data_location"] = result.raw_data_location # Pass blob path

                logger.info(f"[{group_name}/{flow_id}] Calling Parser: {parse_url}")
                response = await client.post(parse_url, json=payload)
                response.raise_for_status()
                parse_result = response.json()
                result.processed_data_location = parse_result.get("processed_data_location")

                if parse_result.get("status") != "success":
                     raise Exception(f"Parser service reported failure: {parse_result.get('message', 'Unknown parser error')}")

                if not result.processed_data_location:
                    # This might happen if parsing resulted in no valid rows after mapping/filtering
                    logger.info(f"[{group_name}/{flow_id}] Parser finished successfully but no processed data was generated (e.g., all rows filtered out).")
                    result.status = "success"
                    result.message = "Parsing successful, but no data generated after processing."
                    # Consider cleanup of raw_data_location if needed
                    return # Nothing to load

                logger.info(f"[{group_name}/{flow_id}] Parser finished. Processed data at: {result.processed_data_location}")

            except httpx.RequestError as e:
                logger.error(f"[{group_name}/{flow_id}] Network error calling Parser: {e.request.url} - {e}")
                result.status = "failed"
                result.message = f"Network error calling Parser: {e}"
                # Consider cleanup of raw_data_location
                return
            except httpx.HTTPStatusError as e:
                logger.error(f"[{group_name}/{flow_id}] Parser service error ({e.response.status_code}): {e.response.text}")
                result.status = "failed"
                result.message = f"Parser service error ({e.response.status_code}): {e.response.text}"
                # Consider cleanup of raw_data_location
                return
            except Exception as e:
                logger.exception(f"[{group_name}/{flow_id}] Unexpected error during Parsing")
                result.status = "failed"
                result.message = f"Unexpected error during Parsing: {e}"
                 # Consider cleanup of raw_data_location
                return

            # --- Step 4: Load Data ---
            try:
                load_url = f"{LOADER_SERVICE_URL}/api/v1/load"
                payload = {
                    "flow_id": flow_id,
                    "group_name": group_name,
                    "data_location": result.processed_data_location,
                    "destination_config": destination_config
                }
                logger.info(f"[{group_name}/{flow_id}] Calling Loader: {load_url}")
                response = await client.post(load_url, json=payload)
                response.raise_for_status()
                load_result_payload = response.json()
                result.load_result = load_result_payload # Store the loader's response

                if load_result_payload.get("status") != "success":
                    raise Exception(f"Loader service reported failure: {load_result_payload.get('message', 'Unknown loader error')}")

                logger.info(f"[{group_name}/{flow_id}] Loader finished. Result: {load_result_payload}")
                result.status = "success"
                result.message = f"Flow completed successfully. Rows loaded: {load_result_payload.get('rows_loaded', 'N/A')}"

            except httpx.RequestError as e:
                logger.error(f"[{group_name}/{flow_id}] Network error calling Loader: {e.request.url} - {e}")
                result.status = "failed"
                result.message = f"Network error calling Loader: {e}"
                # Consider cleanup of processed_data_location
                return
            except httpx.HTTPStatusError as e:
                logger.error(f"[{group_name}/{flow_id}] Loader service error ({e.response.status_code}): {e.response.text}")
                result.status = "failed"
                result.message = f"Loader service error ({e.response.status_code}): {e.response.text}"
                 # Consider cleanup of processed_data_location
                return
            except Exception as e:
                logger.exception(f"[{group_name}/{flow_id}] Unexpected error during Loading")
                result.status = "failed"
                result.message = f"Unexpected error during Loading: {e}"
                 # Consider cleanup of processed_data_location
                return

            # --- Step 5: Cleanup (Optional) ---
            # TODO: Implement logic to delete intermediate blobs in raw/processed containers if desired
            # logger.info(f"[{group_name}/{flow_id}] Performing cleanup...")

    except Exception as e:
         # Catch-all for unexpected issues within the orchestration logic itself
         logger.exception(f"[{group_name}/{flow_id}] Critical error during orchestration")
         result.status = "failed"
         result.message = f"Critical orchestration error: {e}"

    finally:
        logger.info(f"[{group_name}/{flow_id}] Orchestration finished with status: {result.status}. Message: {result.message}")
        # TODO: Persist final 'result' state somewhere? (e.g., log analytics, status table)


@router.post("/run/flow", status_code=status.HTTP_202_ACCEPTED)
async def run_scheduled_flow(request: FlowRunRequest, background_tasks: BackgroundTasks):
    """Endpoint triggered by Scheduler. Runs flow asynchronously."""
    logger.info(f"Received request to run scheduled flow: {request.group_name}/{request.flow_id}")
    # Prepare result object to track state
    result = OrchestrationResult(flow_id=request.flow_id, group_name=request.group_name)
    # Run the actual flow logic in the background
    background_tasks.add_task(execute_flow_steps, result)
    # Immediately return 202 Accepted
    return {"message": "Flow execution initiated", "flow_id": request.flow_id, "group_name": request.group_name}


@router.post("/run/kafka_flow", status_code=status.HTTP_202_ACCEPTED)
async def run_kafka_flow(request: KafkaFlowRunRequest, background_tasks: BackgroundTasks):
    """Endpoint triggered by Kafka Listener. Runs flow asynchronously."""
    logger.info(f"Received request to run kafka flow: {request.group_name}/{request.flow_id}")
    # Prepare result object, adding kafka message
    result = OrchestrationResult(
        flow_id=request.flow_id,
        group_name=request.group_name,
        message=request.message_data # Store kafka message here
    )
    background_tasks.add_task(execute_flow_steps, result)
    return {"message": "Kafka flow execution initiated", "flow_id": request.flow_id, "group_name": request.group_name}

# Optional: Endpoint to check status (if results are persisted)
# @router.get("/status/{group_name}/{flow_id}/{run_id}")
# async def get_flow_status(...): ...

# --- flow_orchestrator_service/main.py ---
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from routers import orchestrator_router
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Flow Orchestrator Service",
    description="Coordinates ETL flow steps (Extract, Parse, Load)",
    version="1.0.0"
)

@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    logger.debug(f"{request.method} {request.url.path} - Completed in {process_time:.4f} secs")
    return response

# Include the API router
app.include_router(orchestrator_router.router, prefix="/api/v1/orchestrator")

@app.get("/health", tags=["Health"])
async def health_check():
    """Simple health check endpoint"""
    return {"status": "healthy"}

# Basic root endpoint
@app.get("/", tags=["Root"])
def read_root():
    return {"message": "Flow Orchestrator Service Running"}

# Optional: Add global exception handler if needed
@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    logger.exception(f"Unhandled exception for request {request.url.path}")
    return JSONResponse(
        status_code=500,
        content={"message": "An internal server error occurred"},
    )

# If running directly using `python main.py` (for local testing)
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8004) # Port for orchestrator
