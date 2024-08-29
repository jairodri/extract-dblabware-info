from sqlalchemy import create_engine, MetaData
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd


def connect_to_oracle(host, port, service_name, username, password):
    """
    Function to connect to an Oracle database using SQLAlchemy and cx_Oracle.
    From version 8 onwards, cx_Oracle has been renamed to oracledb, though cx_Oracle is still functional in earlier versions.

    Parameters:
    - host: The address of the database server.
    - port: The port where the database server is listening.
    - service_name: The service name of the database.
    - username: The database user's name.
    - password: The password for the database user.

    Returns:
    - engine: SQLAlchemy Engine object if the connection is successful.
    - None: If an error occurs during the connection.
    """
    # Create the connection string in the format required by SQLAlchemy and cx_Oracle
    connection_string = f'oracle+cx_oracle://{username}:{password}@{host}:{port}/?service_name={service_name}'
    
    try:
        # Create the SQLAlchemy engine
        engine = create_engine(connection_string)
        return engine
    except SQLAlchemyError as e:
        print(f"Error connecting to the database: {e}")
        return None



def get_dbinfo_metadata(host, port, service_name, username, password):

    engine = connect_to_oracle(host, port, service_name, username, password)

    if engine is None:
        return None

    catalog_info = {}

    catalog_tables = {
        "tables": "ALL_TABLES",
        "views": "ALL_VIEWS",
        "indexes": "ALL_INDEXES",
        "constraints": "ALL_CONSTRAINTS",
        "procedures": "ALL_PROCEDURES",
        "synonyms": "ALL_SYNONYMS"
    }

    with engine.connect() as connection:
        for object_type, table in catalog_tables.items():
            query = f"select COLUMN_NAME, DATA_TYPE, DATA_LENGTH from SYS.ALL_TAB_COLS where TABLE_NAME = '{table}' order by COLUMN_ID"
            try:
                df = pd.read_sql(query, connection)
                catalog_info[object_type] = df
            except SQLAlchemyError as e:
                print(f"Error retrieving {object_type}: {e}")
                catalog_info[object_type] = pd.DataFrame()  # Return an empty DataFrame in case of error

    return catalog_info

