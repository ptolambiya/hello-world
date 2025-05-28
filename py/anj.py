import pandas as pd

def create_sample_files():
    """Creates the sample CSV files as described in the problem."""
    master_table_content = """object_name,attribute_name,code,description
obj1,col1,0,c1c1 description
obj1,col1,1,c1c2 description
obj1,col2,0,c2c1 description
obj1,col2,1,c2c2 description
obj1,col3,X,X description
obj2,col1,A,A description
obj2,col1,B,B description
obj2,col1,C,C description
obj2,col2,P,P description
obj2,col2,Q,Q description
obj2,col2,R,R description
"""
    obj1_content = """col1,col2,col3
0,0,X
0,1,X
1,0,X
"""
    obj2_content = """col1,col2
A,P
B,P
c,Q
a,R
"""
    with open("master_table.csv", "w") as f:
        f.write(master_table_content)
    with open("obj1.csv", "w") as f:
        f.write(obj1_content)
    with open("obj2.csv", "w") as f:
        f.write(obj2_content)
    print("Sample files (master_table.csv, obj1.csv, obj2.csv) created in the current directory.")

def enrich_object_file(object_name: str,
                       master_file_path: str = 'master_table.csv',
                       columns_to_enrich_str: str = None) -> pd.DataFrame:
    """
    Loads an object's CSV file and enriches specified columns with descriptions
    from a master CSV file.

    Args:
        object_name (str): The name of the object, corresponding to its CSV file
                           (e.g., "obj1" for "obj1.csv").
        master_file_path (str): The file path for the master table CSV.
        columns_to_enrich_str (str, optional): A comma-separated string of column names
                                             that should be enriched. If None, empty, or
                                             only whitespace, all columns in the object
                                             file will be considered for enrichment.
                                             Defaults to None.

    Returns:
        pd.DataFrame: A DataFrame containing the original object data with
                      added description columns for the specified attributes.
                      Returns an empty DataFrame if critical files are not found.
    """
    try:
        master_df = pd.read_csv(master_file_path)
        master_df['code'] = master_df['code'].astype(str)
    except FileNotFoundError:
        print(f"Error: Master file '{master_file_path}' not found.")
        return pd.DataFrame()

    obj_file_path = f"{object_name}.csv"
    try:
        obj_df = pd.read_csv(obj_file_path)
    except FileNotFoundError:
        print(f"Error: Object file '{obj_file_path}' not found.")
        return pd.DataFrame()

    enriched_df = obj_df.copy()

    # Determine which columns to process for enrichment
    actual_cols_for_enrichment = []
    if columns_to_enrich_str and columns_to_enrich_str.strip():
        # User has provided specific columns
        user_specified_cols = [col.strip() for col in columns_to_enrich_str.split(',') if col.strip()]
        
        valid_cols_from_user = []
        for col_spec in user_specified_cols:
            if col_spec in enriched_df.columns: # Check against columns in the actual object file
                valid_cols_from_user.append(col_spec)
            else:
                print(f"Warning: Column '{col_spec}' specified for enrichment not found in '{obj_file_path}'. It will be ignored.")
        actual_cols_for_enrichment = valid_cols_from_user # Process only valid columns from user list
        
        if not actual_cols_for_enrichment and user_specified_cols: # User specified cols but none were valid
            print(f"Info: None of the specified columns for enrichment ('{columns_to_enrich_str}') were found or valid in '{obj_file_path}'. No enrichment will be performed beyond loading the file.")
        elif actual_cols_for_enrichment:
             print(f"Info: Attempting to enrich specified columns in '{obj_file_path}': {', '.join(actual_cols_for_enrichment)}.")

    else:
        # No specific columns provided by user, or string was empty/None
        # Default to all columns present in the object file
        actual_cols_for_enrichment = enriched_df.columns.tolist()
        if actual_cols_for_enrichment:
            print(f"Info: No specific columns provided via 'columns_to_enrich_str'. Attempting to enrich all columns found in '{obj_file_path}': {', '.join(actual_cols_for_enrichment)}.")
        else: # obj_df was empty
            print(f"Info: Object file '{obj_file_path}' has no columns to enrich.")


    # Loop through the determined columns for enrichment
    for col_name in actual_cols_for_enrichment:
        current_mapping_df = master_df[
            (master_df['object_name'] == object_name) &
            (master_df['attribute_name'] == col_name)
        ]
        
        description_map = pd.Series(
            current_mapping_df.description.values,
            index=current_mapping_df.code # 'code' is already string type
        ).to_dict()
        
        # Add the new description column. If description_map is empty (no rules or no matching codes),
        # this will result in NaNs for the description column, which is expected.
        enriched_df[f"{col_name}_description"] = enriched_df[col_name].astype(str).map(description_map)

    return enriched_df

# Main execution block
if __name__ == '__main__':
    # Step 1: Create the sample CSV files (if they don't exist or need refreshing)
    create_sample_files()

    print("\n--- Test 1: Enriching obj1.csv (all columns - default behavior) ---")
    enriched_obj1_all_df = enrich_object_file('obj1')
    if not enriched_obj1_all_df.empty:
        print(enriched_obj1_all_df.to_string())

    print("\n--- Test 2: Enriching obj1.csv (specific columns: 'col1, col3') ---")
    enriched_obj1_specific_df = enrich_object_file('obj1', columns_to_enrich_str="col1, col3")
    if not enriched_obj1_specific_df.empty:
        print(enriched_obj1_specific_df.to_string())

    print("\n--- Test 3: Enriching obj1.csv (specific columns: 'col1, non_existent_col') ---")
    enriched_obj1_mixed_df = enrich_object_file('obj1', columns_to_enrich_str="col1, non_existent_col")
    if not enriched_obj1_mixed_df.empty:
        print(enriched_obj1_mixed_df.to_string())

    print("\n--- Test 4: Enriching obj1.csv (columns_to_enrich_str is empty string) ---")
    enriched_obj1_empty_str_df = enrich_object_file('obj1', columns_to_enrich_str="   ")
    if not enriched_obj1_empty_str_df.empty:
        print(enriched_obj1_empty_str_df.to_string())

    print("\n--- Test 5: Enriching obj2.csv (specific column: 'col1') ---")
    enriched_obj2_specific_df = enrich_object_file('obj2', columns_to_enrich_str="col1")
    if not enriched_obj2_specific_df.empty:
        print(enriched_obj2_specific_df.to_string())

    print("\n--- Test 6: Enriching obj1.csv (specific columns: 'non_existent_col, another_bad_col') ---")
    enriched_obj1_none_valid_df = enrich_object_file('obj1', columns_to_enrich_str="non_existent_col, another_bad_col")
    if not enriched_obj1_none_valid_df.empty:
        print(enriched_obj1_none_valid_df.to_string())
    else:
        # This case might return the original df if all specified cols are invalid,
        # which is not empty if obj_df loaded. Let's check output.
        # The function is designed to return the copy of obj_df if actual_cols_for_enrichment is empty.
        # So, if enriched_obj1_none_valid_df is not empty, it contains the original obj1 data.
        print("Returned DataFrame contains original data as no valid columns were specified for enrichment.")
