from azure.storage.blob import BlobServiceClient
from config import AZURE_STORAGE_CONN_STR, BLOB_CONTAINER_RAW
import uuid
import os

blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONN_STR)

def upload_to_blob(data: bytes | str, flow_id: str, source_type: str, file_extension: str) -> str:
    """Uploads data to raw container and returns blob path."""
    blob_name = f"{flow_id}/{source_type}/{uuid.uuid4()}.{file_extension}"
    blob_client = blob_service_client.get_blob_client(container=BLOB_CONTAINER_RAW, blob=blob_name)
    blob_client.upload_blob(data, overwrite=True)
    # Return path relative to container for consistency
    return f"{BLOB_CONTAINER_RAW}/{blob_name}"

def upload_dataframe_to_blob(df, flow_id: str, source_type: str, format: str = 'parquet') -> str:
     """Uploads pandas DataFrame"""
     blob_name = f"{flow_id}/{source_type}/{uuid.uuid4()}.{format}"
     blob_client = blob_service_client.get_blob_client(container=BLOB_CONTAINER_RAW, blob=blob_name)
     # Efficiently stream upload for Parquet/CSV
     if format == 'parquet':
         from io import BytesIO
         buffer = BytesIO()
         df.to_parquet(buffer, index=False)
         buffer.seek(0)
         blob_client.upload_blob(buffer, overwrite=True)
     elif format == 'csv':
         from io import StringIO
         buffer = StringIO()
         df.to_csv(buffer, index=False)
         buffer.seek(0)
         blob_client.upload_blob(buffer.getvalue(), overwrite=True)
     else:
         raise ValueError("Unsupported DataFrame format for blob upload")
     return f"{BLOB_CONTAINER_RAW}/{blob_name}"
