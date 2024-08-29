import os
from dotenv import load_dotenv
from getdbinfo import get_dbinfo_metadata
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

if __name__ == '__main__':
    db_info = get_dbinfo_metadata(host, port, service_name, username, password)
    print(db_info)