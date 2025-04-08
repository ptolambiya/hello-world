import cx_Oracle
import pandas as pd
from services.blob_storage_service import upload_dataframe_to_blob
import logging

logger = logging.getLogger(__name__)

def extract_oracle(config: dict, flow_id: str) -> str:
    dsn = cx_Oracle.makedsn(config['host'], config['port'], service_name=config['service_name'])
    query = config['query']
    logger.info(f"Connecting to Oracle: {config['host']}/{config['service_name']}")
    try:
        # Consider using connection pooling for performance
        with cx_Oracle.connect(user=config['user'], password=config['password'], dsn=dsn) as connection:
            logger.info(f"Executing query for flow {flow_id}")
            # Fetch data into Pandas DataFrame for easier handling/upload
            # Adjust chunking for very large tables if memory is a concern
            df = pd.read_sql(query, connection)
            logger.info(f"Fetched {len(df)} rows from Oracle for flow {flow_id}")

        if not df.empty:
             # Upload as Parquet (efficient)
            blob_path = upload_dataframe_to_blob(df, flow_id, "oracle_db", format='parquet')
            logger.info(f"Uploaded Oracle data for flow {flow_id} to {blob_path}")
            return blob_path
        else:
            logger.info(f"No data returned from Oracle query for flow {flow_id}")
            # Decide how to handle empty results - return None or a specific indicator?
            return None # Or raise an exception? Or return an empty file marker?

    except cx_Oracle.DatabaseError as e:
        logger.error(f"Oracle Database Error for flow {flow_id}: {e}")
        raise # Re-raise to indicate failure
    except Exception as e:
        logger.error(f"Error extracting from Oracle for flow {flow_id}: {e}")
        raise
