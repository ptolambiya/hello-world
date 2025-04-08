import pandas as pd
import logging

logger = logging.getLogger(__name__)

def apply_mappings(df: pd.DataFrame, mapping_config: dict | list) -> pd.DataFrame:
    """Applies column selection and renaming based on config."""
    if not mapping_config:
        logger.warning("No mapping config provided, returning original DataFrame.")
        return df

    # Standardize mapping_config format (e.g., always a list of dicts)
    if isinstance(mapping_config, dict): # Assuming dict maps source_col: dest_col
        mapping_list = [{"source": k, "destination": v} for k, v in mapping_config.items()]
    elif isinstance(mapping_config, list):
         # Assuming list of {'source': 'colA', 'destination': 'colX'}
         mapping_list = mapping_config
    else:
        logger.error(f"Invalid mapping_config format: {type(mapping_config)}")
        raise ValueError("Invalid mapping configuration format")


    rename_map = {item['source']: item['destination'] for item in mapping_list if 'source' in item and 'destination' in item}
    final_columns = [item['destination'] for item in mapping_list if 'destination' in item]

    # Select columns that exist in the DataFrame and are in the source mapping
    cols_to_select = [item['source'] for item in mapping_list if item['source'] in df.columns]
    if not cols_to_select:
         logger.error("No source columns from mapping found in DataFrame.")
         # Return empty DF with target columns? Or raise error?
         return pd.DataFrame(columns=final_columns)

    processed_df = df[cols_to_select].copy()

    # Rename columns
    processed_df.rename(columns=rename_map, inplace=True)

    # Ensure all destination columns are present, add missing ones with None/NaN
    for col in final_columns:
        if col not in processed_df.columns:
            processed_df[col] = None # Or pd.NA

    # Return only the final desired columns in the correct order
    return processed_df[final_columns]
