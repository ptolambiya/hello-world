import pandas as pd
import io

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
    print("Sample files (master_table.csv, obj1.csv, obj2.csv) created.")

def enrich_object_file(object_name: str, master_file_path: str = 'master_table.csv') -> pd.DataFrame:
    """
    Loads an object's CSV file and enriches its columns with descriptions
    from a master CSV file.

    Args:
        object_name (str): The name of the object, corresponding to its CSV file
                           (e.g., "obj1" for "obj1.csv").
        master_file_path (str): The file path for the master table CSV.

    Returns:
        pd.DataFrame: A DataFrame containing the original object data with
                      added description columns. Returns an empty DataFrame
                      if the object file is not found.
    """
    try:
        # Load the master table
        master_df = pd.read_csv(master_file_path)
        # Ensure the 'code' column in master_df is string type for reliable mapping
        master_df['code'] = master_df['code'].astype(str)
    except FileNotFoundError:
        print(f"Error: Master file '{master_file_path}' not found.")
        return pd.DataFrame()

    obj_file_path = f"{object_name}.csv"
    try:
        # Load the specific object's CSV file
        obj_df = pd.read_csv(obj_file_path)
    except FileNotFoundError:
        print(f"Error: Object file '{obj_file_path}' not found.")
        return pd.DataFrame()

    enriched_df = obj_df.copy()

    # Iterate through each column in the object's dataframe
    for col_name in obj_df.columns:
        # Filter master_df for relevant mappings for the current object and column
        current_mapping_df = master_df[
            (master_df['object_name'] == object_name) &
            (master_df['attribute_name'] == col_name)
        ]

        if current_mapping_df.empty:
            # No mapping found for this column in the master table
            enriched_df[f"{col_name}_description"] = None # Or some other placeholder like pd.NA
            continue
            
        # Create a dictionary for mapping codes to descriptions
        # {code_value: description_value}
        description_map = pd.Series(
            current_mapping_df.description.values,
            index=current_mapping_df.code # Already str type
        ).to_dict()

        # Add the new description column
        # Ensure the object column is treated as string for mapping
        enriched_df[f"{col_name}_description"] = enriched_df[col_name].astype(str).map(description_map)

    return enriched_df

if __name__ == '__main__':
    # Create the sample files first
    create_sample_files()

    print("\nEnriching obj1.csv:")
    enriched_obj1_df = enrich_object_file('obj1')
    print(enriched_obj1_df)

    print("\nEnriching obj2.csv:")
    enriched_obj2_df = enrich_object_file('obj2')
    print(enriched_obj2_df)
    
    print("\nAttempting to enrich a non-existent object file (obj3.csv):")
    enriched_obj3_df = enrich_object_file('obj3')
    print(enriched_obj3_df)

    print("\nAttempting to enrich with a non-existent master file:")
    enriched_obj4_df = enrich_object_file('obj1', master_file_path='non_existent_master.csv')
    print(enriched_obj4_df)
