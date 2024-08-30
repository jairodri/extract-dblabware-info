import os
import pandas as pd


def dump_dbinfo_to_csv(service_name:str, table_dataframes: dict, output_dir: str, sep: str=','):
    """
    Saves each DataFrame in the provided dictionary to a CSV file, organizing the files within a directory 
    named after the service.

    The CSV files are named after the corresponding table names. Each DataFrame is saved to a separate CSV file 
    in the specified output directory. If the directory does not include a subdirectory named after the service, 
    one will be created.

    Parameters:
    -----------
    service_name : str
        The name of the service (typically the database service) used to name the output subdirectory.
    
    table_dataframes : dict
        A dictionary where each key is a table name and each value is a DataFrame containing the table's data.
    
    output_dir : str
        The directory where the CSV files will be saved. If this directory does not contain a subdirectory named 
        after the service, one will be created.

    sep : str, optional
        Field delimiter for the output CSV files. The default is a comma.

    Returns:
    --------
    None
    """

    # Ensure the output directory includes a subdirectory named after the service
    if not output_dir.endswith(service_name):
        output_dir = os.path.join(output_dir, service_name)

    # Create the output directory if it does not exist
    os.makedirs(output_dir, exist_ok=True)

    # Iterate over each table name and its corresponding DataFrame in the dictionary
    # for table_name, dataframe in table_dataframes.items():
    for item, item_data in table_dataframes.items():
        # Replace newline characters with spaces in all text columns
        table_name = item_data['name']
        dataframe = item_data['data']
        dataframe = dataframe.applymap(lambda x: x.replace('\n', ' ') if isinstance(x, str) else x)

        # Create the CSV file path using the table name
        file_path = os.path.join(output_dir, f"{table_name}.csv")
        
        # Save the DataFrame to a CSV file with the specified delimiter and without the index
        dataframe.to_csv(file_path, sep=sep, index=False)