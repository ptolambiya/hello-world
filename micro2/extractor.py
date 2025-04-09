utils/blob_storage.py:

Python

from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.identity import DefaultAzureCredential # Recommended for ACA
from config import AZURE_STORAGE_ACCOUNT_URL, AZURE_STORAGE_CONN_STR, BLOB_CONTAINER_RAW
import pandas as pd
from io import BytesIO
import uuid
import logging
import os

logger = logging.getLogger(__name__)

def get_blob_service_client() -> BlobServiceClient:
    """Gets BlobServiceClient using Managed Identity or connection string."""
    if AZURE_STORAGE_ACCOUNT_URL:
        logger.debug("Authenticating to Blob Storage using DefaultAzureCredential (Managed Identity or env vars)")
        credential = DefaultAzureCredential()
        return BlobServiceClient(account_url=AZURE_STORAGE_ACCOUNT_URL, credential=credential)
    elif AZURE_STORAGE_CONN_STR:
        logger.debug("Authenticating to Blob Storage using Connection String")
        return BlobServiceClient.from_connection_string(AZURE_STORAGE_CONN_STR)
    else:
        raise ValueError("Missing Azure Storage configuration (Account URL for Managed Identity or Connection String)")

_blob_service_client = None
def get_cached_blob_service_client() -> BlobServiceClient:
    """Returns a cached BlobServiceClient instance."""
    global _blob_service_client
    if _blob_service_client is None:
        _blob_service_client = get_blob_service_client()
    return _blob_service_client

def upload_dataframe_to_blob(df: pd.DataFrame, flow_id: str, group_name: str, source_type: str, container_name: str = BLOB_CONTAINER_RAW, format: str = 'parquet') -> str:
    """
    Uploads a pandas DataFrame to Azure Blob Storage.
    Returns the full blob path (container/blob_name).
    """
    if df is None or df.empty:
        logger.warning(f"[{group_name}/{flow_id}] DataFrame is empty, skipping blob upload.")
        return None # Indicate nothing was uploaded

    # Use Parquet format by default - efficient for columnar data
    file_extension = format
    blob_name = f"{group_name}/{flow_id}/{source_type}_{uuid.uuid4()}.{file_extension}"

    try:
        blob_service_client = get_cached_blob_service_client()
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        output_buffer = BytesIO()
        if format == 'parquet':
            df.to_parquet(output_buffer, index=False, engine='pyarrow') # Specify engine
        elif format == 'csv':
             # Use UTF-8 encoding, handle potential issues
             df.to_csv(output_buffer, index=False, encoding='utf-8')
        else:
             raise ValueError(f"Unsupported upload format: {format}")

        output_buffer.seek(0) # Rewind buffer to the beginning before uploading
        blob_client.upload_blob(output_buffer, overwrite=True)
        blob_path = f"{container_name}/{blob_name}" # Return relative path
        logger.info(f"[{group_name}/{flow_id}] Successfully uploaded DataFrame ({len(df)} rows) to {blob_path}")
        return blob_path

    except Exception as e:
        logger.exception(f"[{group_name}/{flow_id}] Failed to upload DataFrame to blob {container_name}/{blob_name}")
        raise # Re-raise the exception to be handled by the caller

def download_blob_to_bytes(blob_path: str) -> BytesIO | None:
    """Downloads a blob's content into a BytesIO buffer."""
    if '/' not in blob_path:
         raise ValueError("Invalid blob_path format. Expected 'container/blob_name'.")

    container_name, blob_name = blob_path.split('/', 1)
    logger.info(f"Attempting to download blob: {container_name}/{blob_name}")
    try:
        blob_service_client = get_cached_blob_service_client()
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        if not blob_client.exists():
            logger.error(f"Blob not found: {container_name}/{blob_name}")
            return None

        downloader = blob_client.download_blob()
        buffer = BytesIO()
        downloader.readinto(buffer)
        buffer.seek(0) # Rewind for reading
        logger.info(f"Successfully downloaded blob: {container_name}/{blob_name}")
        return buffer
    except Exception as e:
        logger.exception(f"Failed to download blob {container_name}/{blob_name}")
        raise # Re-raise the exception

services/extractors/db_extractor.py:

Python

import cx_Oracle
import pandas as pd
from utils.blob_storage import upload_dataframe_to_blob
import logging

logger = logging.getLogger(__name__)

def extract_oracle(config: dict, flow_id: str, group_name: str) -> str | None:
    """Extracts data from Oracle based on config and uploads as Parquet."""
    required_keys = ['user', 'password', 'host', 'port', 'service_name', 'query']
    if not all(key in config for key in required_keys):
        raise ValueError("Oracle config missing one or more required keys: " + ", ".join(required_keys))

    db_user = config['user']
    # Get password securely from env var referenced in config or direct secret injection
    db_pass = config['password'] # TODO: Replace with secure retrieval
    db_host = config['host']
    db_port = config['port']
    db_service = config['service_name']
    query = config['query']

    dsn = cx_Oracle.makedsn(db_host, db_port, service_name=db_service)
    logger.info(f"[{group_name}/{flow_id}] Connecting to Oracle: {db_host}:{db_port}/{db_service}")

    connection = None # Ensure connection is defined for finally block
    try:
        # Consider using connection pooling for frequently executed flows (e.g., using SQLAlchemy create_engine)
        connection = cx_Oracle.connect(user=db_user, password=db_pass, dsn=dsn)
        logger.info(f"[{group_name}/{flow_id}] Connected to Oracle. Executing query...")

        # Use pandas for robust data fetching, handles various types
        # For VERY large tables, consider fetching in chunks: pd.read_sql(query, connection, chunksize=10000)
        df = pd.read_sql(query, connection)
        logger.info(f"[{group_name}/{flow_id}] Fetched {len(df)} rows from Oracle.")

        # Upload the DataFrame to blob storage (handles empty DataFrame case)
        blob_path = upload_dataframe_to_blob(df, flow_id, group_name, "oracle_db", format='parquet')
        return blob_path # Will be None if df was empty

    except cx_Oracle.DatabaseError as e:
        logger.error(f"[{group_name}/{flow_id}] Oracle Database Error: {e}")
        raise # Re-raise to be caught by the router
    except Exception as e:
        logger.exception(f"[{group_name}/{flow_id}] Unexpected error extracting from Oracle")
        raise
    finally:
         if connection:
             connection.close()
             logger.debug(f"[{group_name}/{flow_id}] Oracle connection closed.")

# --- Add similar functions for other DB types ---
# def extract_mssql(config: dict, flow_id: str, group_name: str) -> str | None: ...
# def extract_mysql(config: dict, flow_id: str, group_name: str) -> str | None: ...
# def extract_postgres(config: dict, flow_id: str, group_name: str) -> str | None: ...
services/extractors/api_extractor.py:

Python

import httpx
import pandas as pd
from utils.blob_storage import upload_dataframe_to_blob
import logging
import json

logger = logging.getLogger(__name__)

async def extract_api(config: dict, flow_id: str, group_name: str) -> str | None:
    """Extracts data from a REST API based on config and uploads as Parquet."""
    required_keys = ['url', 'method']
    if not all(key in config for key in required_keys):
         raise ValueError("API config missing one or more required keys: " + ", ".join(required_keys))

    url = config['url']
    method = config.get('method', 'GET').upper()
    params = config.get('params') # Dictionary for query parameters
    headers = config.get('headers') # Dictionary for headers (e.g., Content-Type, Authorization)
    payload = config.get('payload') # Dictionary or JSON string for POST/PUT body
    data_path = config.get('data_path') # Optional: JSON path to the list of records (e.g., 'results' or 'data.items')

    logger.info(f"[{group_name}/{flow_id}] Calling API: {method} {url}")

    async with httpx.AsyncClient(timeout=config.get("timeout", 60.0)) as client:
        try:
            response = await client.request(
                method=method,
                url=url,
                params=params,
                headers=headers,
                json=payload if isinstance(payload, dict) else None, # Send dict as JSON
                data=payload if isinstance(payload, str) else None   # Send string as raw data
            )
            response.raise_for_status() # Raise exception for 4xx/5xx status codes
            logger.info(f"[{group_name}/{flow_id}] API call successful ({response.status_code})")

            # --- Process Response ---
            content_type = response.headers.get('content-type', '').lower()

            if 'application/json' in content_type:
                 raw_data = response.json()
                 # Optional: Navigate JSON to find the list of records
                 records = raw_data
                 if data_path:
                     try:
                         # Simple dot notation path resolver (can be made more robust)
                         path_keys = data_path.split('.')
                         for key in path_keys:
                             if isinstance(records, dict):
                                 records = records.get(key)
                             elif isinstance(records, list) and key.isdigit(): # Basic list index access
                                 records = records[int(key)]
                             else:
                                 raise KeyError(f"Path '{data_path}' not found in JSON response.")
                     except Exception as e:
                         logger.error(f"[{group_name}/{flow_id}] Error accessing data_path '{data_path}': {e}")
                         raise ValueError(f"Could not find data at path '{data_path}' in API JSON response.")

                 if isinstance(records, list):
                     # Convert list of dictionaries to DataFrame
                     df = pd.DataFrame(records)
                 elif isinstance(records, dict):
                      # Wrap single dict in a list for DataFrame compatibility
                      df = pd.DataFrame([records])
                 else:
                      raise ValueError("API response (after data_path) is not a list or dictionary.")

            elif 'text/csv' in content_type or 'application/csv' in content_type:
                 from io import StringIO
                 csv_data = StringIO(response.text)
                 df = pd.read_csv(csv_data) # Add pd.read_csv options if needed (delimiter, etc.)

            # TODO: Add handling for XML, plain text etc. if required

            else:
                 raise ValueError(f"Unsupported API response Content-Type: {content_type}")

            logger.info(f"[{group_name}/{flow_id}] Parsed {len(df)} records from API response.")
            # Upload DataFrame to blob
            blob_path = upload_dataframe_to_blob(df, flow_id, group_name, "api", format='parquet')
            return blob_path

        except httpx.RequestError as e:
            logger.error(f"[{group_name}/{flow_id}] Network error calling API {url}: {e}")
            raise
        except httpx.HTTPStatusError as e:
            logger.error(f"[{group_name}/{flow_id}] API Error ({e.response.status_code}) calling {url}: {e.response.text}")
            raise
        except (json.JSONDecodeError, ValueError, KeyError) as e:
             logger.error(f"[{group_name}/{flow_id}] Error processing API response from {url}: {e}")
             raise ValueError(f"Error processing API response: {e}") # Re-raise as specific error
        except Exception as e:
            logger.exception(f"[{group_name}/{flow_id}] Unexpected error extracting from API {url}")
            raise
services/extractors/csv_extractor.py:

Python

import pandas as pd
from utils.blob_storage import download_blob_to_bytes, upload_dataframe_to_blob
import logging

logger = logging.getLogger(__name__)

def extract_csv(config: dict, flow_id: str, group_name: str) -> str | None:
    """
    Extracts data from a CSV file located in Azure Blob Storage.
    Assumes config['blob_path'] points to the source CSV.
    Uploads the data as Parquet.
    """
    required_keys = ['blob_path']
    if 'blob_path' not in config:
        raise ValueError("CSV config missing required key: 'blob_path'")

    source_blob_path = config['blob_path'] # e.g., "input-container/my_data.csv"
    # Optional pandas read_csv arguments from config
    read_csv_args = config.get('read_csv_options', {})

    logger.info(f"[{group_name}/{flow_id}] Extracting CSV from blob: {source_blob_path}")

    try:
        # Download the source CSV blob content
        csv_buffer = download_blob_to_bytes(source_blob_path)

        if csv_buffer is None:
            logger.error(f"[{group_name}/{flow_id}] Source CSV blob not found or could not be downloaded: {source_blob_path}")
            # Depending on requirements, either raise error or return None
            raise FileNotFoundError(f"Source CSV blob not found: {source_blob_path}")
            # return None

        # Read CSV content using pandas
        # Pass any specific options from config (delimiter, encoding, header row, etc.)
        logger.info(f"[{group_name}/{flow_id}] Reading CSV data from buffer using options: {read_csv_args}")
        df = pd.read_csv(csv_buffer, **read_csv_args)
        logger.info(f"[{group_name}/{flow_id}] Read {len(df)} rows from CSV {source_blob_path}")

        # Upload the resulting DataFrame as Parquet to the raw container
        blob_path = upload_dataframe_to_blob(df, flow_id, group_name, "csv", format='parquet')
        return blob_path

    except FileNotFoundError as e:
        logger.error(f"[{group_name}/{flow_id}] Error finding source CSV: {e}")
        raise # Re-raise specific error
    except pd.errors.EmptyDataError:
         logger.warning(f"[{group_name}/{flow_id}] Source CSV file is empty: {source_blob_path}")
         # Upload empty dataframe? Or return None? Let's upload empty Parquet for consistency.
         df = pd.DataFrame()
         blob_path = upload_dataframe_to_blob(df, flow_id, group_name, "csv", format='parquet')
         return blob_path
    except Exception as e:
        logger.exception(f"[{group_name}/{flow_id}] Error processing CSV from {source_blob_path}")
        raise
routers/extract_router.py:

Python

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field
import logging
# Import specific extractor functions
from services.extractors.db_extractor import extract_oracle # Add others as implemented
from services.extractors.api_extractor import extract_api
from services.extractors.csv_extractor import extract_csv

logger = logging.getLogger(__name__)
router = APIRouter()

class ExtractRequest(BaseModel):
    flow_id: str = Field(..., description="Unique ID of the flow")
    group_name: str = Field(..., description="Group name for context/logging/blob path")
    source_type: str = Field(..., description="Type of source (e.g., ORACLE_DB, API, CSV)")
    source_config: dict = Field(..., description="Configuration specific to the source type")

class ExtractResponse(BaseModel):
    status: str = "success"
    message: str | None = None
    data_location: str | None = Field(None, description="Path to the extracted raw data in blob storage (e.g., container/path/file.parquet)")

@router.post("/", response_model=ExtractResponse, status_code=status.HTTP_200_OK)
async def extract_data(request: ExtractRequest):
    """
    Receives extraction request, calls the appropriate extractor based on source_type,
    and returns the location of the raw extracted data in blob storage.
    """
    flow_id = request.flow_id
    group_name = request.group_name
    source_type_upper = request.source_type.upper()
    config = request.source_config

    logger.info(f"[{group_name}/{flow_id}] Received extraction request. Type: {source_type_upper}")
    data_location = None

    try:
        if source_type_upper == "ORACLE_DB":
            data_location = extract_oracle(config, flow_id, group_name)
        elif source_type_upper == "API":
            # API extractor is async
            data_location = await extract_api(config, flow_id, group_name)
        elif source_type_upper == "CSV":
            data_location = extract_csv(config, flow_id, group_name)
        # --- Add elif blocks for other implemented extractors ---
        # elif source_type_upper == "MSSQL_DB": data_location = extract_mssql(...)
        elif source_type_upper == "KAFKA":
             # This shouldn't be called by the orchestrator for Kafka flows
             logger.error(f"[{group_name}/{flow_id}] Invalid request: Extractor service should not be called for KAFKA source type.")
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Extractor service does not handle KAFKA sources.")
        else:
            logger.error(f"[{group_name}/{flow_id}] Unsupported source type: {request.source_type}")
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Unsupported source type: {request.source_type}")

        if data_location:
             logger.info(f"[{group_name}/{flow_id}] Extraction successful. Data location: {data_location}")
             return ExtractResponse(status="success", data_location=data_location)
        else:
             logger.info(f"[{group_name}/{flow_id}] Extraction successful, but no data was generated/uploaded.")
             # Return success but indicate no data was produced
             return ExtractResponse(status="success", data_location=None, message="Extraction successful, but no data found/generated.")

    except FileNotFoundError as e:
         logger.error(f"[{group_name}/{flow_id}] Extraction failed - File not found: {e}")
         raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except ValueError as e: # Catch config errors or data processing errors
        logger.error(f"[{group_name}/{flow_id}] Extraction failed - Bad request/config: {e}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except HTTPException as e:
        # Re-raise HTTP exceptions directly (e.g., from API calls)
        raise e
    except Exception as e:
        # Catch-all for unexpected errors (like DB connection issues, library errors)
        logger.exception(f"[{group_name}/{flow_id}] Unexpected error during extraction") # Log stack trace
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Internal server error during extraction: {type(e).__name__}")
