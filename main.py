import os
from dotenv import load_dotenv
from getdbinfo import get_dbinfo_metadata
from dumpdbinfo import dump_dbinfo_to_csv
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

if __name__ == '__main__':
    db_info = get_dbinfo_metadata(host, port, service_name, username, password, owner)
    dump_dbinfo_to_csv(service_name, db_info, output_dir_metadata, sep='|')
