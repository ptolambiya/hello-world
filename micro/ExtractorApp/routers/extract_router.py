from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
import logging
# Import your extractor functions
from services.extractors.db_extractor import extract_oracle # Add others
# from services.extractors.api_extractor import extract_api
# from services.extractors.csv_extractor import extract_csv

logger = logging.getLogger(__name__)
router = APIRouter()

class ExtractRequest(BaseModel):
    flow_id: str = Field(..., description="Unique ID of the flow for context/logging")
    source_type: str # e.g., "ORACLE_DB", "API", "CSV", "MSSQL_DB" etc.
    source_config: dict # Connection details, query, URL, path etc.

@router.post("/")
async def extract_data(request: ExtractRequest):
    logger.info(f"Received extraction request for flow {request.flow_id}, type: {request.source_type}")
    data_location = None
    source_type_upper = request.source_type.upper()

    try:
        if source_type_upper == "ORACLE_DB":
            # Need flow_id for blob path naming
            data_location = extract_oracle(request.source_config, request.flow_id)
        # elif source_type_upper == "API":
        #     data_location = extract_api(request.source_config, request.flow_id)
        # elif source_type_upper == "CSV":
        #     data_location = extract_csv(request.source_config, request.flow_id)
        # --- Add other source types ---
        elif source_type_upper == "KAFKA":
             # Kafka is handled by listener, extractor shouldn't be called
             logger.warning(f"Extractor called for KAFKA source type on flow {request.flow_id}. This should not happen.")
             raise HTTPException(status_code=400, detail="Extractor service does not handle KAFKA sources.")
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported source type: {request.source_type}")

        if data_location:
             return {"status": "success", "data_location": data_location}
        else:
             # Handle cases where extraction resulted in no data but wasn't an error
             logger.info(f"Extraction for flow {request.flow_id} resulted in no data.")
             # You might want a specific response for "no data" vs "success"
             return {"status": "success", "data_location": None, "message": "No data extracted"}

    except HTTPException:
         raise # Re-raise HTTP exceptions
    except Exception as e:
        logger.exception(f"Extraction failed for flow {request.flow_id}: {e}") # Use logger.exception to include stack trace
        raise HTTPException(status_code=500, detail=f"Internal server error during extraction: {e}")
