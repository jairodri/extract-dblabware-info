import os
from dotenv import load_dotenv
from getdbinfo import get_dbinfo_metadata, get_dbinfo_table, get_dbinfo_all_tables
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

# Get the directory where the output files will be saved
output_dir_metadata = os.getenv('OUTPUT_DIR_METADATA')
output_dir_data = os.getenv('OUTPUT_DIR_DATA')

table_name = os.getenv('TABLE_NAME')

if __name__ == '__main__':
    # db_info_catalog = get_dbinfo_metadata(host, port, service_name, username, password, owner)
    # dump_dbinfo_to_csv(service_name, db_info_catalog, output_dir_metadata, sep='|')
    # dump_dbinfo_to_excel(service_name, db_info_catalog, output_dir_metadata)
    # db_info_table = get_dbinfo_table(host, port, service_name, username, password, owner, table_name)
    # dump_dbinfo_to_csv(service_name, db_info_table, output_dir_data, sep='|')
    # dump_dbinfo_to_excel(service_name, db_info_table, output_dir_data, include_record_count=True, max_records_per_table=20000, file_name=table_name)
    db_info_all_tables = get_dbinfo_all_tables(host, port, service_name, username, password, owner)
    print(db_info_all_tables)
