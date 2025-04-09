config.py:

Python

import os
from dotenv import load_dotenv

load_dotenv()

# Azure Storage Configuration
AZURE_STORAGE_ACCOUNT_URL = os.getenv("AZURE_STORAGE_ACCOUNT_URL")
AZURE_STORAGE_CONN_STR = os.getenv("AZURE_STORAGE_CONN_STR")
BLOB_CONTAINER_PROCESSED = os.getenv("BLOB_CONTAINER_PROCESSED", "processed-data")
BLOB_CONTAINER_RAW = os.getenv("BLOB_CONTAINER_RAW", "raw-data") # Needed if downloading raw data
utils/blob_storage.py: Copy the same blob_storage.py from the Extractor service here. Ensure it has both upload and download functions. Update the default container name used in upload_dataframe_to_blob to BLOB_CONTAINER_PROCESSED if desired, or pass it explicitly.

services/mapper.py: (Can be copied from previous plan)

Python

import pandas as pd
import logging

logger = logging.getLogger(__name__)

def apply_mappings(df: pd.DataFrame, mapping_config: dict | list, flow_id: str, group_name: str) -> pd.DataFrame:
    """
    Applies column selection and renaming based on config.
    Expects mapping_config as a list of {'source': 'colA', 'destination': 'colX'} dicts.
    """
    if df is None or df.empty:
         logger.warning(f"[{group_name}/{flow_id}] DataFrame is empty, skipping mapping.")
         return df # Return empty df

    if not mapping_config:
        logger.warning(f"[{group_name}/{flow_id}] No mapping config provided, returning original DataFrame.")
        return df

    # Standardize mapping_config format (e.g., always a list of dicts)
    if isinstance(mapping_config, dict): # Assuming simple dict maps source_col: dest_col
        logger.warning(f"[{group_name}/{flow_id}] Received dict mapping_config, converting to list format.")
        mapping_list = [{"source": k, "destination": v} for k, v in mapping_config.items()]
    elif isinstance(mapping_config, list):
         # Assuming list of {'source': 'colA', 'destination': 'colX'}
         if not all(isinstance(item, dict) and 'source' in item and 'destination' in item for item in mapping_config):
              logger.error(f"[{group_name}/{flow_id}] Invalid list format in mapping_config. Expected list of {{'source': '...', 'destination': '...'}}.")
              raise ValueError("Invalid mapping configuration format in list.")
         mapping_list = mapping_config
    else:
        logger.error(f"[{group_name}/{flow_id}] Invalid mapping_config format: {type(mapping_config)}")
        raise ValueError("Invalid mapping configuration format")

    logger.info(f"[{group_name}/{flow_id}] Applying {len(mapping_list)} mappings.")
    logger.debug(f"[{group_name}/{flow_id}] Mapping list: {mapping_list}")

    rename_map = {item['source']: item['destination'] for item in mapping_list}
    final_columns = [item['destination'] for item in mapping_list]

    # Identify source columns from mapping that *actually exist* in the input DataFrame
    cols_to_select = [item['source'] for item in mapping_list if item['source'] in df.columns]

    if not cols_to_select:
         logger.error(f"[{group_name}/{flow_id}] No source columns from mapping found in DataFrame columns: {list(df.columns)}. Mapping sources: {[item['source'] for item in mapping_list]}")
         # Return empty DF with target columns to maintain schema consistency downstream?
         return pd.DataFrame(columns=final_columns)

    logger.debug(f"[{group_name}/{flow_id}] Selecting source columns: {cols_to_select}")
    processed_df = df[cols_to_select].copy() # Create copy to avoid SettingWithCopyWarning

    # Rename columns based on the full mapping (even if source wasn't selected, doesn't hurt)
    processed_df.rename(columns=rename_map, inplace=True)
    logger.debug(f"[{group_name}/{flow_id}] Renamed columns. Current columns: {list(processed_df.columns)}")

    # Ensure all *destination* columns from the mapping are present in the final DataFrame
    # Add any missing destination columns with None/NaN values.
    missing_cols = [col for col in final_columns if col not in processed_df.columns]
    if missing_cols:
         logger.warning(f"[{group_name}/{flow_id}] Adding missing destination columns: {missing_cols}")
         for col in missing_cols:
             processed_df[col] = pd.NA # Use pandas NA for better type handling

    # Reorder columns to match the order defined in the mapping_config's destination fields
    # Filter final_columns to only those actually present in processed_df after rename/add
    final_ordered_columns = [col for col in final_columns if col in processed_df.columns]
    logger.debug(f"[{group_name}/{flow_id}] Final column order: {final_ordered_columns}")

    # Select and reorder
    processed_df = processed_df[final_ordered_columns]

    logger.info(f"[{group_name}/{flow_id}] Mapping applied successfully. Resulting columns: {list(processed_df.columns)}")
    return processed_df
services/parsers/parquet_parser.py:

Python

import pandas as pd
from utils.blob_storage import download_blob_to_bytes
import logging

logger = logging.getLogger(__name__)

def parse_parquet(blob_path: str, flow_id: str, group_name: str) -> pd.DataFrame | None:
    """Downloads and parses a Parquet file from blob storage."""
    logger.info(f"[{group_name}/{flow_id}] Parsing Parquet from blob: {blob_path}")
    try:
        parquet_buffer = download_blob_to_bytes(blob_path)
        if parquet_buffer is None:
            logger.error(f"[{group_name}/{flow_id}] Parquet blob not found or download failed: {blob_path}")
            raise FileNotFoundError(f"Parquet blob not found: {blob_path}")

        df = pd.read_parquet(parquet_buffer, engine='pyarrow')
        logger.info(f"[{group_name}/{flow_id}] Parsed {len(df)} rows from Parquet file {blob_path}")
        return df
    except FileNotFoundError:
        raise # Re-raise
    except Exception as e:
        logger.exception(f"[{group_name}/{flow_id}] Failed to parse Parquet file {blob_path}")
        raise # Re-raise
services/parsers/json_parser.py:

Python

import pandas as pd
import json
import logging

logger = logging.getLogger(__name__)

def parse_json(json_data_str: str, flow_id: str, group_name: str) -> pd.DataFrame | None:
    """Parses a JSON string (expected to be a list of objects or a single object)."""
    logger.info(f"[{group_name}/{flow_id}] Parsing JSON data string.")
    try:
        # Attempt to load the JSON string
        data = json.loads(json_data_str)

        # Handle common JSON structures: list of objects or single object
        if isinstance(data, list):
             df = pd.DataFrame(data)
             logger.info(f"[{group_name}/{flow_id}] Parsed {len(df)} records from JSON list.")
        elif isinstance(data, dict):
             # Convert single object into a DataFrame with one row
             df = pd.DataFrame([data])
             logger.info(f"[{group_name}/{flow_id}] Parsed 1 record from JSON object.")
        else:
             logger.error(f"[{group_name}/{flow_id}] Unexpected JSON structure type: {type(data)}. Expected list or dict.")
             raise ValueError("Parsed JSON data is not a list or dictionary.")

        return df

    except json.JSONDecodeError as e:
        logger.error(f"[{group_name}/{flow_id}] Invalid JSON data received: {e}. Data snippet: {json_data_str[:200]}...")
        raise ValueError(f"Invalid JSON data: {e}")
    except Exception as e:
        logger.exception(f"[{group_name}/{flow_id}] Failed to parse JSON data")
        raise
services/parsers/xml_parser.py: (Example if needed)

Python

# Example: Requires 'lxml' installation
# import pandas as pd
# from lxml import etree
# import logging

# logger = logging.getLogger(__name__)

# def parse_xml(xml_data_str: str, row_xpath: str, column_mapping: dict, flow_id: str, group_name: str) -> pd.DataFrame | None:
#     """Parses XML string based on XPath for rows and column mappings."""
#     logger.info(f"[{group_name}/{flow_id}] Parsing XML data string using row XPath: {row_xpath}")
#     try:
#         root = etree.fromstring(xml_data_str.encode('utf-8')) # Ensure bytes for lxml
#         rows = root.xpath(row_xpath)
#         logger.debug(f"Found {len(rows)} elements matching row XPath.")

#         data_list = []
#         for row_element in rows:
#             record = {}
#             for dest_col, source_xpath in column_mapping.items():
#                 # Execute relative XPath from the row element
#                 value_elements
