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
from typing import Optional

class FlowRunRequest(BaseModel):
    """Request model for flows triggered by scheduler."""
    flow_id: str = Field(..., description="Unique ID of the flow.")
    group_name: str = Field(..., description="Group the flow belongs to.")

class KafkaFlowRunRequest(FlowRunRequest):
    """Request model for flows triggered by Kafka listener."""
    message_data: str = Field(..., description="Raw message data received from Kafka.")

class FlowRunResponse(BaseModel):
    """Response model indicating initiation."""
    status: str = "initiated"
    flow_id: str
    group_name: str
    run_id: str # A unique ID for this specific execution instance

# --- flow_orchestrator_service/services/orchestration_service.py ---
import httpx
import logging
import uuid
from config import (
    CONFIG_SERVICE_URL, EXTRACTOR_SERVICE_URL, PARSER_SERVICE_URL, LOADER_SERVICE_URL,
    HTTP_TIMEOUT_CONFIG, HTTP_TIMEOUT_EXTRACT, HTTP_TIMEOUT_PARSE, HTTP_TIMEOUT_LOAD
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def execute_flow_steps(flow_id: str, group_name: str, run_id: str, kafka_message_data: Optional[str] = None):
    """The core logic to run a flow's steps: Get Config -> Extract -> Parse -> Load."""
    log_prefix = f"[RunID: {run_id} | Flow: {group_name}/{flow_id}]"
    logger.info(f"{log_prefix} Orchestration starting...")

    flow_config = None
    raw_data_location = None
    processed_data_location = None
    source_type = None
    final_status = "FAILED" # Default to failed unless successful completion

    async with httpx.AsyncClient() as client:
        try:
            # 1. Get Flow Configuration
            logger.info(f"{log_prefix} Fetching configuration...")
            if not CONFIG_SERVICE_URL: raise ValueError("Config Service URL not set.")
            config_url = f"{CONFIG_SERVICE_URL}/api/v1/config/flows/{group_name}/{flow_id}"
            try:
                response = await client.get(config_url, timeout=HTTP_TIMEOUT_CONFIG)
                response.raise_for_status()
                flow_config = response.json()
                source_type = flow_config.get("SourceType", "").upper()
                logger.info(f"{log_prefix} Configuration fetched successfully. SourceType: {source_type}")
            except Exception as e:
                logger.error(f"{log_prefix} Failed to get configuration: {e}")
                raise # Stop execution if config fails

            # Check if flow is enabled
            if not flow_config.get("IsEnabled", True):
                 logger.warning(f"{log_prefix} Flow is disabled in configuration. Skipping execution.")
                 final_status = "SKIPPED"
                 return # Stop execution

            # --- Handle based on Source Type ---
            if source_type == "KAFKA":
                if kafka_message_data is None:
                    logger.error(f"{log_prefix} Kafka flow triggered but no message data provided.")
                    raise ValueError("Missing Kafka message data")
                logger.info(f"{log_prefix} Source is KAFKA. Skipping extraction step.")
                # Data for parser is the direct message content
                parser_input_data = {"raw_data": kafka_message_data}
            else:
                # 2. Extract Data (for non-Kafka sources)
                logger.info(f"{log_prefix} Calling Extractor Service...")
                if not EXTRACTOR_SERVICE_URL: raise ValueError("Extractor Service URL not set.")
                extract_url = f"{EXTRACTOR_SERVICE_URL}/api/v1/extract"
                extract_payload = {
                    "flow_id": flow_id, # Pass flow context
                    "run_id": run_id,   # Pass run context
                    "source_type": source_type,
                    "source_config": flow_config.get("SourceConfig", {})
                }
                try:
                    response = await client.post(extract_url, json=extract_payload, timeout=HTTP_TIMEOUT_EXTRACT)
                    response.raise_for_status()
                    extract_result = response.json()
                    raw_data_location = extract_result.get("data_location")
                    if extract_result.get("status") == "success" and raw_data_location is None:
                        logger.info(f"{log_prefix} Extractor returned success but no data location (likely no data extracted). Skipping further steps.")
                        final_status = "COMPLETED_NO_DATA"
                        return # Stop flow if no data extracted
                    elif not raw_data_location:
                        raise ValueError(f"Extractor did not return a valid data location. Result: {extract_result}")
                    logger.info(f"{log_prefix} Extraction successful. Raw data at: {raw_data_location}")
                    parser_input_data = {"data_location": raw_data_location} # Data for parser is the location
                except Exception as e:
                    logger.error(f"{log_prefix} Extraction step failed: {e}")
                    raise # Stop execution

            # 3. Parse/Transform Data
            logger.info(f"{log_prefix} Calling Parser Service...")
            if not PARSER_SERVICE_URL: raise ValueError("Parser Service URL not set.")
            parse_url = f"{PARSER_SERVICE_URL}/api/v1/parse"
            parse_payload = {
                "flow_id": flow_id,
                "run_id": run_id,
                "source_type": source_type, # Hint for the parser
                "mapping_config": flow_config.get("MappingConfig", []),
                **parser_input_data # Add either raw_data or data_location
            }
            try:
                response = await client.post(parse_url, json=parse_payload, timeout=HTTP_TIMEOUT_PARSE)
                response.raise_for_status()
                parse_result = response.json()
                processed_data_location = parse_result.get("processed_data_location")
                if not processed_data_location:
                     raise ValueError(f"Parser did not return a processed data location. Result: {parse_result}")
                logger.info(f"{log_prefix} Parsing successful. Processed data at: {processed_data_location}")
            except Exception as e:
                logger.error(f"{log_prefix} Parsing step failed: {e}")
                # TODO: Consider cleanup of raw_data_location if needed
                raise # Stop execution

            # 4. Load Data
            logger.info(f"{log_prefix} Calling Loader Service...")
            if not LOADER_SERVICE_URL: raise ValueError("Loader Service URL not set.")
            load_url = f"{LOADER_SERVICE_URL}/api/v1/load"
            load_payload = {
                "flow_id": flow_id,
                "run_id": run_id,
                "data_location": processed_data_location,
                "destination_config": flow_config.get("DestinationConfig", {})
            }
            try:
                response = await client.post(load_url, json=load_payload, timeout=HTTP_TIMEOUT_LOAD)
                response.raise_for_status()
                load_result = response.json()
                # Example success result: {"status": "success", "rows_loaded": 100}
                logger.info(f"{log_prefix} Loading successful. Result: {load_result}")
                final_status = "COMPLETED_SUCCESS"
            except Exception as e:
                logger.error(f"{log_prefix} Loading step failed: {e}")
                # TODO: Consider cleanup of processed_data_location if needed
                raise # Stop execution

            # 5. Cleanup (Optional)
            # TODO: Implement logic to delete intermediate files from Blob Storage
            # if raw_data_location: delete_blob(raw_data_location)
            # if processed_data_location: delete_blob(processed_data_location)
            logger.info(f"{log_prefix} Optional cleanup step skipped.")

        except Exception as e:
            # Catch any exception during the steps
            logger.error(f"{log_prefix} Orchestration failed due to an error: {e}")
            final_status = "FAILED"
            # Don't re-raise here, just log the final status

        finally:
            # Log the final outcome of the orchestration
            logger.info(f"{log_prefix} Orchestration finished with status: {final_status}")
            # TODO: Implement persistent status tracking (e.g., write status to a database table)


# --- flow_orchestrator_service/routers/orchestrator_router.py ---
from fastapi import APIRouter, HTTPException, BackgroundTasks, Depends
import logging
import uuid
from models.flow_models import FlowRunRequest, KafkaFlowRunRequest, FlowRunResponse
from services.orchestration_service import execute_flow_steps

logger = logging.getLogger(__name__)
router = APIRouter()

# Dependency to generate a unique run ID for each request
def generate_run_id() -> str:
    return str(uuid.uuid4())

@router.post("/run/flow", status_code=202, response_model=FlowRunResponse)
async def run_scheduled_flow(
    request: FlowRunRequest,
    background_tasks: BackgroundTasks,
    run_id: str = Depends(generate_run_id) # Generate unique ID for this run
):
    """
    Endpoint triggered by the Scheduler Service to run a standard flow.
    Runs the flow execution in the background.
    """
    log_prefix = f"[RunID: {run_id} | Flow: {request.group_name}/{request.flow_id}]"
    logger.info(f"{log_prefix} Received request to run scheduled flow.")
    # Add the actual flow execution logic as a background task
    background_tasks.add_task(execute_flow_steps, request.flow_id, request.group_name, run_id)
    # Return immediately with acceptance and run ID
    return FlowRunResponse(flow_id=request.flow_id, group_name=request.group_name, run_id=run_id)


@router.post("/run/kafka_flow", status_code=202, response_model=FlowRunResponse)
async def run_kafka_flow(
    request: KafkaFlowRunRequest,
    background_tasks: BackgroundTasks,
    run_id: str = Depends(generate_run_id)
):
    """
    Endpoint triggered by the Kafka Listener Service to run a flow based on a Kafka message.
    Runs the flow execution in the background.
    """
    log_prefix = f"[RunID: {run_id} | Flow: {request.group_name}/{request.flow_id}]"
    logger.info(
