import cx_Oracle
import pandas as pd
from sqlalchemy import create_engine # For easier DataFrame upload via pandas
import logging
from services.blob_storage_service import download_blob_to_dataframe # Assuming this exists

logger = logging.getLogger(__name__)

def load_to_oracle(data_location: str, dest_config: dict):
    """Loads data from blob (as DataFrame) into Oracle table."""
    target_table = dest_config['table_name']
    batch_size = dest_config.get('batch_size', 1000) # Configurable batch size

    logger.info(f"Starting load process for {data_location} into Oracle table {target_table}")

    try:
        # 1. Download data from Blob
        logger.debug(f"Downloading data from {data_location}")
        # Assuming download_blob_to_dataframe handles parquet/jsonl etc.
        df = download_blob_to_dataframe(data_location)
        if df is None or df.empty:
             logger.info(f"No data found at {data_location} to load.")
             return {"status": "success", "rows_loaded": 0, "message": "No data to load"}
        logger.info(f"Downloaded {len(df)} rows from {data_location}")

        # 2. Prepare Oracle Connection (using SQLAlchemy engine for pandas.to_sql)
        db_user = dest_config['user']
        db_pass = dest_config['password']
        db_host = dest_config['host']
        db_port = dest_config['port']
        db_service = dest_config['service_name']
        # Construct DSN or use TNS_ADMIN if configured
        dsn = cx_Oracle.makedsn(db_host, db_port, service_name=db_service)
        # sqlalchemy engine string format: oracle+cx_oracle://user:pass@host:port/service_name
        engine_str = f"oracle+cx_oracle://{db_user}:{db_pass}@{dsn}"
        engine = create_engine(engine_str)

        # 3. Load data using pandas.to_sql (check performance for huge datasets)
        logger.info(f"Loading {len(df)} rows into Oracle table {target_table}...")
        # Note: 'append' will add rows. 'replace' would drop and recreate.
        # 'if_exists'='append' is common for ETL incremental loads.
        # 'chunksize' controls rows per INSERT statement.
        # 'method=multi' might offer performance gains with cx_Oracle, test needed.
        df.to_sql(
            name=target_table,
            con=engine,
            if_exists='append', # Or 'replace' based on flow logic
            index=False,
            chunksize=batch_size,
            method='multi' # Use 'multi' for potential speedup with cx_Oracle
        )

        logger.info(f"Successfully loaded {len(df)} rows into {target_table}")
        return {"status": "success", "rows_loaded": len(df)}

    except FileNotFoundError:
        logger.error(f"Processed data file not found at {data_location}")
        raise HTTPException(status_code=404, detail=f"Processed data not found: {data_location}")
    except cx_Oracle.DatabaseError as e:
         logger.error(f"Oracle Database Error during load: {e}")
         raise HTTPException(status_code=500, detail=f"Oracle DB Error: {e}")
    except Exception as e:
        logger.exception(f"Failed to load data into Oracle: {e}") # Use exception logger
        raise HTTPException(status_code=500, detail=f"Internal server error during load: {e}")
    finally:
        # Dispose SQLAlchemy engine if created
        if 'engine' in locals() and engine:
             engine.dispose()
