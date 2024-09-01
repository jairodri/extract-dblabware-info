import os
from dotenv import load_dotenv
from getdbinfo import get_dbinfo_metadata, get_dbinfo_table, get_dbinfo_all_tables, get_dbinfo_tables_with_clob, get_dbinfo_tables
from dumpdbinfo import dump_dbinfo_to_csv, dump_dbinfo_to_excel
from base64 import urlsafe_b64encode, urlsafe_b64decode


def decode(enc):
    return urlsafe_b64decode(enc).decode('utf-8')

# Load variables from the .env file
load_dotenv()

# Get the database connections params
host = os.getenv('HOST')
port = os.getenv('PORT')
service_name = os.getenv('SERVICE_NAME')
username = os.getenv('USER')
password = os.getenv('PASSWORD')
owner = os.getenv('OWNER')
connection_info = {
    'host': host,
    'port': port,
    'service_name': service_name,
    'username': username,
    'password': password,
    'owner': owner
}

# Get the directory where the output files will be saved
output_dir_metadata = os.getenv('OUTPUT_DIR_METADATA')
output_dir_data = os.getenv('OUTPUT_DIR_DATA')

table_name = os.getenv('TABLE_NAME')

if __name__ == '__main__':
    # db_info_catalog = get_dbinfo_metadata(connection_info)
    # dump_dbinfo_to_csv(service_name, db_info_catalog, output_dir_metadata, sep='|')
    # dump_dbinfo_to_excel(service_name, db_info_catalog, output_dir_metadata)
    # db_info_table = get_dbinfo_table(connection_info, table_name)
    # dump_dbinfo_to_csv(service_name, db_info_table, output_dir_data, sep='|')
    # dump_dbinfo_to_excel(service_name, db_info_table, output_dir_data, include_record_count=True, max_records_per_table=20000, file_name=table_name)
    db_info_all_tables = get_dbinfo_all_tables(connection_info, total_records_limit=300000, max_records_per_table=10000)
    # dump_dbinfo_to_csv(service_name, db_info_all_tables, output_dir_data, sep='|') 
    dump_dbinfo_to_excel(service_name, db_info_all_tables, output_dir_data, include_record_count=True, max_records_per_table=10000)
    # tables_with_clob = get_dbinfo_tables_with_clob(connection_info)
    # tables_with_clob = get_dbinfo_tables(tables_with_clob, connection_info, total_records_limit=100000, max_records_per_table=20000)
    # dump_dbinfo_to_csv(service_name, tables_with_clob, output_dir_data, sep='|')    
    # dump_dbinfo_to_excel(service_name, tables_with_clob, output_dir_data, include_record_count=True, max_records_per_table=20000)
