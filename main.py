import os
from dotenv import load_dotenv
from getdbinfo import get_dbinfo_metadata, get_dbinfo_table, get_dbinfo_all_tables, get_dbinfo_tables_with_clob, get_dbinfo_tables
from dumpdbinfo import dump_dbinfo_to_csv, dump_dbinfo_to_excel
from comparefiles import compare_folders_and_save_diffs, generate_excel_from_diffs


def load_grouped_vars(prefix):
    """Loads all environment variables starting with the given prefix into a dictionary."""
    grouped_vars = {}
    for key, value in os.environ.items():
        if key.startswith(prefix):
            # Add the variable to the dictionary, removing the prefix of the key name
            grouped_vars[key[len(prefix):].lower()] = value
    return grouped_vars

# Load variables from the .env file
load_dotenv()

# Get the database connections params
connection_info = load_grouped_vars('DEV_PET_V8_')

# Get the directory where the output files will be saved
output_dir_metadata = os.getenv('OUTPUT_DIR_METADATA')
output_dir_data = os.getenv('OUTPUT_DIR_DATA')

# Get the table to extract 
table_name = os.getenv('TABLE_NAME')

# Get the folders to compare
folder_in1 = os.getenv('COMPARE_FOLDER_IN1')
folder_in2 = os.getenv('COMPARE_FOLDER_IN2')
folder_out = os.getenv('COMPARE_FOLDER_OUT')


if __name__ == '__main__':
    #
    # 1 - Get catalog info and dump it to csv/excel file
    # db_info_catalog = get_dbinfo_metadata(connection_info)
    # dump_dbinfo_to_csv(connection_info['service_name'], db_info_catalog, output_dir_metadata, sep='|')
    # dump_dbinfo_to_excel(connection_info['service_name'], db_info_catalog, output_dir_metadata)
    #
    # 2 - Get data from a specific table and dump it to csv/excel file
    # db_info_table = get_dbinfo_table(connection_info, table_name)
    # dump_dbinfo_to_csv(connection_info['service_name'], db_info_table, output_dir_data, sep='|')
    # dump_dbinfo_to_excel(connection_info['service_name'], db_info_table, output_dir_data, include_record_count=True, max_records_per_table=20000, file_name=table_name)
    #
    # 3 - Get data from all tables and dump them to csv/excel file
    # db_info_all_tables = get_dbinfo_all_tables(connection_info, total_records_limit=300000, max_records_per_table=10000)
    # dump_dbinfo_to_csv(connection_info['service_name'], db_info_all_tables, output_dir_data, sep='|') 
    # dump_dbinfo_to_excel(connection_info['service_name'], db_info_all_tables, output_dir_data, include_record_count=True, max_records_per_table=10000)
    # 
    # 4 - Get data from tables with clob fields and dump to csv/excel file
    # tables_with_clob = get_dbinfo_tables_with_clob(connection_info)
    # tables_with_clob = get_dbinfo_tables(tables_with_clob, connection_info, total_records_limit=100000, max_records_per_table=20000)
    # dump_dbinfo_to_csv(connection_info['service_name'], tables_with_clob, output_dir_data, sep='|')    
    # dump_dbinfo_to_excel(connection_info['service_name'], tables_with_clob, output_dir_data, include_record_count=True, max_records_per_table=20000)
    #
    # compare_folders_and_save_diffs(folder_in1, folder_in2, folder_out)
    generate_excel_from_diffs(folder_in1, folder_in2, folder_out)


