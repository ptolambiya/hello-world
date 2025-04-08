# --- config_service/config.py ---
import os
from dotenv import load_dotenv

# Load .env file if it exists (for local development)
load_dotenv()

# Connection string for Azure Table Storage
# In ACA, this will be set as an environment variable (ideally from a secret)
AZURE_TABLE_STORAGE_CONN_STR = os.getenv("AZURE_TABLE_STORAGE_CONN_STR")

if not AZURE_TABLE_STORAGE_CONN_STR:
    print("Warning: AZURE_TABLE_STORAGE_CONN_STR environment variable not set.")
    # Provide a default for local testing if desired, but not recommended for production
    # AZURE_TABLE_STORAGE_CONN_STR = "YOUR_LOCAL_DEV_CONNECTION_STRING"

# --- config_service/models/config_models.py ---
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any

# Define models for better type hinting and potential future validation,
# although Azure Table entities are dictionaries by default.

class FlowConfig(BaseModel):
    PartitionKey: str
    RowKey: str
    SourceType: str
    SourceConfig: Dict[str, Any] = {} # Store complex config as serialized JSON in table
    DestinationType: str
    DestinationConfig: Dict[str, Any] = {} # Store complex config as serialized JSON in table
    MappingConfig: List[Dict[str, Any]] = [] # Store mapping as serialized JSON in table
    IsEnabled: bool = True

class ScheduleConfig(BaseModel):
    PartitionKey: str # e.g., 'CRON'
    RowKey: str # Unique schedule ID, e.g., 'daily_sales_load'
    GroupName: str
    CronExpression: str
    FlowIDs: str # Comma-separated list of FlowIDs in the group
    IsEnabled: bool = True

class KafkaListenerConfig(BaseModel):
    PartitionKey: str # e.g., 'KAFKA'
    RowKey: str # Unique listener ID, e.g., 'orders_topic_listener'
    GroupName: str
    FlowID: str
    TopicName: str
    IsEnabled: bool = True

# --- config_service/services/table_service.py ---
from azure.data.tables import TableServiceClient
from azure.core.exceptions import ResourceNotFoundError
from config import AZURE_TABLE_STORAGE_CONN_STR
import json
import logging
from typing import List, Dict, Any, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ConfigTableService:
    """Service to interact with Azure Table Storage for configuration."""
    _instance = None

    def __new__(cls, *args, **kwargs):
        # Singleton pattern to reuse the client
        if cls._instance is None:
            cls._instance = super(ConfigTableService, cls).__new__(cls)
            if not AZURE_TABLE_STORAGE_CONN_STR:
                raise ValueError("Azure Table Storage connection string is not configured.")
            try:
                cls._instance.service_client = TableServiceClient.from_connection_string(conn_str=AZURE_TABLE_STORAGE_CONN_STR)
                logger.info("TableServiceClient initialized successfully.")
            except Exception as e:
                logger.error(f"Failed to initialize TableServiceClient: {e}")
                raise
        return cls._instance

    def _get_table_client(self, table_name: str):
        """Gets a client for a specific table."""
        try:
            return self.service_client.get_table_client(table_name)
        except Exception as e:
            logger.error(f"Failed to get table client for '{table_name}': {e}")
            raise

    def _deserialize_entity(self, entity: Dict[str, Any]) -> Dict[str, Any]:
        """Deserializes specific JSON string properties within an entity."""
        for key in ['SourceConfig', 'DestinationConfig', 'MappingConfig']:
            if key in entity and isinstance(entity[key], str):
                try:
                    entity[key] = json.loads(entity[key])
                except json.JSONDecodeError:
                    logger.warning(f"Failed to decode JSON for key '{key}' in entity {entity.get('RowKey')}")
                    entity[key] = {} # Default to empty dict on error
            elif key in entity and entity[key] is None:
                 entity[key] = {} # Handle None case
        return entity

    def get_flow_details(self, group_name: str, flow_id: str) -> Optional[Dict[str, Any]]:
        """Fetches details for a specific flow."""
        try:
            table_client = self._get_table_client("Flows")
            entity = table_client.get_entity(partition_key=group_name, row_key=flow_id)
            return self._deserialize_entity(entity)
        except ResourceNotFoundError:
            logger.warning(f"Flow not found: Group='{group_name}', FlowID='{flow_id}'")
            return None
        except Exception as e:
            logger.error(f"Error getting flow '{group_name}/{flow_id}': {e}")
            return None # Or raise a custom exception

    def get_flows_by_group(self, group_name: str) -> List[Dict[str, Any]]:
        """Fetches all flows belonging to a specific group."""
        flows = []
        try:
            table_client = self._get_table_client("Flows")
            query_filter = f"PartitionKey eq '{group_name}'"
            entities = table_client.query_entities(query_filter=query_filter)
            for entity in entities:
                flows.append(self._deserialize_entity(entity))
            return flows
        except Exception as e:
            logger.error(f"Error getting flows for group '{group_name}': {e}")
            return [] # Return empty list on error

    def get_schedules(self, only_enabled: bool = True) -> List[Dict[str, Any]]:
        """Fetches all schedule configurations."""
        schedules = []
        try:
            table_client = self._get_table_client("Schedules")
            query_filter = "IsEnabled eq true" if only_enabled else ""
            entities = table_client.query_entities(query_filter=query_filter)
            return list(entities) # No complex deserialization expected here by default
        except Exception as e:
            logger.error(f"Error getting schedules: {e}")
            return []

    def get_kafka_listeners(self, only_enabled: bool = True) -> List[Dict[str, Any]]:
        """Fetches all Kafka listener configurations."""
        listeners = []
        try:
            table_client = self._get_table_client("KafkaListeners")
            query_filter = "IsEnabled eq true" if only_enabled else ""
            entities = table_client.query_entities(query_filter=query_filter)
            return list(entities) # No complex deserialization expected here by default
        except Exception as e:
            logger.error(f"Error getting Kafka listeners: {e}")
            return []

# Instantiate the service (singleton ensures it's created only once)
try:
    config_table_service = ConfigTableService()
except ValueError as e:
    logger.error(f"Configuration Service initialization failed: {e}")
    config_table_service = None # Indicate failure

# --- config_service/routers/config_router.py ---
from fastapi import APIRouter, HTTPException, Depends
from typing import List
# Import the singleton instance
from services.table_service import config_table_service, ConfigTableService
from models.config_models import FlowConfig, ScheduleConfig, KafkaListenerConfig # Use for response models

router = APIRouter()

# Dependency function to ensure service is available
async def get_config_service():
    if config_table_service is None:
         raise HTTPException(status_code=503, detail="Configuration service is unavailable.")
    return config_table_service

@router.get("/flows/{group_name}/{flow_id}", response_model=FlowConfig)
async def read_flow(group_name: str, flow_id: str, service: ConfigTableService = Depends(get_config_service)):
    """Gets details for a specific flow configuration."""
    flow = service.get_flow_details(group_name=group_name, flow_id=flow_id)
    if flow is None:
        raise HTTPException(status_code=404, detail=f"Flow '{flow_id}' in group '{group_name}' not found")
    return flow

@router.get("/groups/{group_name}/flows", response_model=List[FlowConfig])
async def read_group_flows(group_name: str, service: ConfigTableService = Depends(get_config_service)):
    """Gets all flow configurations for a specific group."""
    flows = service.get_flows_by_group(group_name=group_name)
    return flows

@router.get("/schedules", response_model=List[ScheduleConfig])
async def read_schedules(enabled_only: bool = True, service: ConfigTableService = Depends(get_config_service)):
    """Gets all schedule configurations."""
    schedules = service.get_schedules(only_enabled=enabled_only)
    return schedules

@router.get("/kafka_listeners", response_model=List[KafkaListenerConfig])
async def read_kafka_listeners(enabled_only: bool = True, service: ConfigTableService = Depends(get_config_service)):
    """Gets all Kafka listener configurations."""
    listeners = service.get_kafka_listeners(only_enabled=enabled_only)
    return listeners

# --- config_service/main.py ---
from fastapi import FastAPI
from routers import config_router
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="ETL Configuration Service",
    description="Provides access to ETL flow configurations stored in Azure Table Storage.",
    version="1.0.0"
)

# Include the API router
app.include_router(config_router.router, prefix="/api/v1/config", tags=["Configuration"])

@app.get("/health", tags=["Health"])
def read_health():
    """Health check endpoint."""
    # Basic health check, could be expanded to check DB connection
    # Attempt to instantiate service if it failed initially (e.g., connection string available later)
    global config_table_service
    if config_table_service is None:
        try:
            from services.table_service import ConfigTableService
            config_table_service = ConfigTableService()
            logger.info("Configuration Service re-initialized successfully during health check.")
        except Exception as e:
             logger.error(f"Failed to re-initialize Configuration Service during health check: {e}")
             return {"status": "unhealthy", "reason": "Configuration backend unavailable"}

    return {"status": "healthy"}

@app.get("/", tags=["Root"])
def read_root():
    """Root endpoint providing basic service info."""
    return {"message": "Welcome to the ETL Configuration Service API"}

if __name__ == "__main__":
    import uvicorn
    # Run directly for local testing (uvicorn main:app --reload)
    uvicorn.run(app, host="0.0.0.0", port=8000)

# --- config_service/requirements.txt ---
fastapi
uvicorn[standard] # Includes performance extras like uvloop
python-dotenv
azure-data-tables
pydantic

# --- config_service/Dockerfile ---
# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set environment variables for Python
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set the working directory in the container
WORKDIR /app

# Install dependencies
COPY requirements.txt .
# Using --no-cache-dir reduces image size
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Expose the port the app runs on (FastAPI default with uvicorn is 8000, but ACA standard is 80)
EXPOSE 80

# Command to run the application using Uvicorn
# Use 0.0.0.0 to bind to all interfaces within the container
# Use port 80 as expected by Azure Container Apps default settings
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]

