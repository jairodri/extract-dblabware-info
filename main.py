import os
from dotenv import load_dotenv
from getdbinfo import get_dbinfo_metadata, get_dbinfo_table, get_dbinfo_all_tables, get_dbinfo_tables_with_clob, get_dbinfo_tables, get_dbinfo_list_of_tables
from dumpdbinfo import dump_dbinfo_to_csv, dump_dbinfo_to_excel
from comparefiles import compare_folders_and_save_diffs, generate_excel_from_diffs, compare_files


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
connection_info = load_grouped_vars('PRE_PET_V8_')

# Get the directory where the output files will be saved
output_dir_metadata = os.getenv('OUTPUT_DIR_METADATA')
output_dir_data = os.getenv('OUTPUT_DIR_DATA')

# Get the table to extract 
table_name = os.getenv('TABLE_NAME')
sql_query = os.getenv('SQL_QUERY')
sql_filter = os.getenv('SQL_FILTER')

# Convert environment variable from string to list
tables_with_clob_to_exclude = os.getenv('TABLES_WITH_CLOB_TO_EXCLUDE', '').split(',')
tables_with_clob_to_exclude = [table.strip() for table in tables_with_clob_to_exclude]  # Remove any extra spaces
table_list = os.getenv('TABLE_LIST', '').split(',')
table_list = [table.strip() for table in table_list]  # Remove any extra spaces

# Get the folders to compare
folder_in1 = os.getenv('COMPARE_FOLDER_IN1')
folder_in2 = os.getenv('COMPARE_FOLDER_IN2')
folder_out = os.getenv('COMPARE_FOLDER_OUT')

# Get the files to compare
file_in1 = os.getenv('COMPARE_FILE_IN1')
file_in2 = os.getenv('COMPARE_FILE_IN2')
file_out = os.getenv('COMPARE_FILE_OUT')


if __name__ == '__main__':
    #
    # 1 - Get catalog info and dump it to csv/excel file
    # db_info_catalog = get_dbinfo_metadata(connection_info)
    # dump_dbinfo_to_excel(connection_info['service_name'], db_info_catalog, output_dir_metadata)
    # dump_dbinfo_to_csv(connection_info['service_name'], db_info_catalog, output_dir_metadata, sep='|')

    #
    # 2 - Get data from a specific table and dump it to csv/excel file
    # db_info_table = get_dbinfo_table(connection_info, table_name, sql_query=sql_query)
    # dump_dbinfo_to_csv(connection_info['service_name'], db_info_table, output_dir_data, sep='|')
    # dump_dbinfo_to_excel(connection_info['service_name'], db_info_table, output_dir_data, include_record_count=True, max_records_per_table=20000, file_name='SAMPLE')
    #
    # 3 - Get data from all tables and dump them to csv/excel file
    # db_info_all_tables = get_dbinfo_all_tables(connection_info, total_records_limit=300000, max_records_per_table=10000)
    # dump_dbinfo_to_csv(connection_info['service_name'], db_info_all_tables, output_dir_data, sep='|') 
    # dump_dbinfo_to_excel(connection_info['service_name'], db_info_all_tables, output_dir_data, include_record_count=True, max_records_per_table=10000)
    # 
    # 4 - Get data from tables with clob fields and dump to csv/excel file
    # tables_with_clob = get_dbinfo_tables_with_clob(connection_info, tables_with_clob_to_exclude)
    # dump_dbinfo_to_excel(connection_info['service_name'], tables_with_clob, output_dir_data, include_record_count=True, max_records_per_table=20000)
    # dump_dbinfo_to_csv(connection_info['service_name'], tables_with_clob, output_dir_data, sep='|')    
    #
    # 5 - Get data from a list of tables and dump to csv/excel file
    info_tables = get_dbinfo_list_of_tables(table_list, connection_info)
    dump_dbinfo_to_csv(connection_info['service_name'], info_tables, output_dir_data, sep='|') 
    dump_dbinfo_to_excel(connection_info['service_name'], info_tables, output_dir_data, include_record_count=True, max_records_per_table=20000)
    #
    # 6 - Compare files and generate excel with differences
    # compare_files(file_in1, file_in2, file_out)
    # compare_folders_and_save_diffs(folder_in1, folder_in2, folder_out)
    # generate_excel_from_diffs(folder_in1, folder_in2, folder_out)



