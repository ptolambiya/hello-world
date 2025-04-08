from fastapi import APIRouter, HTTPException, BackgroundTasks
import httpx
import logging
from config import CONFIG_SERVICE_URL, EXTRACTOR_SERVICE_URL, PARSER_SERVICE_URL, LOADER_SERVICE_URL
from models.flow_models import FlowRunRequest, KafkaFlowRunRequest

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()

async def execute_flow_steps(flow_id: str, group_name: str, kafka_message_data: str | None = None):
    """The core logic to run a flow's steps."""
    logger.info(f"Starting orchestration for flow: {group_name}/{flow_id}")
    raw_data_location = None
    processed_data_location = None
    flow_config = None

    async with httpx.AsyncClient(timeout=None) as client: # Use appropriate timeouts
        # 1. Get Flow Configuration
        try:
            config_url = f"{CONFIG_SERVICE_URL}/api/v1/config/flows/{group_name}/{flow_id}"
            response = await client.get(config_url)
            response.raise_for_status()
            flow_config = response.json()
            logger.info(f"Fetched config for flow {group_name}/{flow_id}")
        except Exception as e:
            logger.error(f"Failed to get config for flow {group_name}/{flow_id}: {e}")
            return # Cannot proceed

        source_type = flow_config.get("SourceType", "").upper()
        source_config = flow_config.get("SourceConfig", {})
        mapping_config = flow_config.get("MappingConfig", {}) # Mapping needed by Parser
        destination_config = flow_config.get("DestinationConfig", {}) # Destination needed by Loader

        # 2. Extract Data (Skip for Kafka)
        if source_type != "KAFKA":
            try:
                extract_url = f"{EXTRACTOR_SERVICE_URL}/api/v1/extract"
                payload = {"source_type": source_type, "source_config": source_config}
                logger.info(f"Calling Extractor for flow {group_name}/{flow_id}")
                response = await client.post(extract_url, json=payload)
                response.raise_for_status()
                extract_result = response.json()
                raw_data_location = extract_result.get("data_location")
                if not raw_data_location:
                     raise ValueError("Extractor did not return data location")
                logger.info(f"Extractor finished for flow {group_name}/{flow_id}. Data at: {raw_data_location}")
            except Exception as e:
                logger.error(f"Extraction failed for flow {group_name}/{flow_id}: {e}")
                return # Stop flow

        # 3. Parse/Transform Data
        try:
            parse_url = f"{PARSER_SERVICE_URL}/api/v1/parse"
            payload = {
                "source_type": source_type, # Parser might need to know original format
                "mapping_config": mapping_config
            }
            if source_type == "KAFKA":
                 # Send Kafka message directly
                 payload["raw_data"] = kafka_message_data
            else:
                 # Send location of extracted data
                 payload["data_location"] = raw_data_location

            logger.info(f"Calling Parser for flow {group_name}/{flow_id}")
            response = await client.post(parse_url, json=payload)
            response.raise_for_status()
            parse_result = response.json()
            processed_data_location = parse_result.get("processed_data_location")
            if not processed_data_location:
                raise ValueError("Parser did not return processed data location")
            logger.info(f"Parser finished for flow {group_name}/{flow_id}. Processed data at: {processed_data_location}")
        except Exception as e:
            logger.error(f"Parsing failed for flow {group_name}/{flow_id}: {e}")
            # Consider cleanup of raw_data_location if needed
            return # Stop flow

        # 4. Load Data
        try:
            load_url = f"{LOADER_SERVICE_URL}/api/v1/load"
            payload = {
                "data_location": processed_data_location,
                "destination_config": destination_config
            }
            logger.info(f"Calling Loader for flow {group_name}/{flow_id}")
            response = await client.post(load_url, json=payload)
            response.raise_for_status()
            load_result = response.json()
            logger.info(f"Loader finished for flow {group_name}/{flow_id}. Result: {load_result}")
            # Add final status logging/reporting
        except Exception as e:
            logger.error(f"Loading failed for flow {group_name}/{flow_id}: {e}")
            # Consider cleanup of processed_data_location
            return # Flow failed

        # 5. TODO: Cleanup intermediate data in Blob Storage? Optional.
        logger.info(f"Successfully completed orchestration for flow: {group_name}/{flow_id}")


@router.post("/run/flow", status_code=202)
async def run_scheduled_flow(request: FlowRunRequest, background_tasks: BackgroundTasks):
    """Endpoint triggered by Scheduler."""
    logger.info(f"Received request to run scheduled flow: {request.group_name}/{request.flow_id}")
    # Run the actual flow logic in the background to immediately return 202
    background_tasks.add_task(execute_flow_steps, request.flow_id, request.group_name)
    return {"message": "Flow execution initiated"}


@router.post("/run/kafka_flow", status_code=202)
async def run_kafka_flow(request: KafkaFlowRunRequest, background_tasks: BackgroundTasks):
    """Endpoint triggered by Kafka Listener."""
    logger.info(f"Received request to run kafka flow: {request.group_name}/{request.flow_id}")
    background_tasks.add_task(execute_flow_steps, request.flow_id, request.group_name, request.message_data)
    return {"message": "Kafka flow execution initiated"}
