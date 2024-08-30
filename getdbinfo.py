from sqlalchemy import create_engine, MetaData
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd


def connect_to_oracle(host:str, port:int, service_name:str, username:str, password:str):
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
        engine = create_engine(connection_string)
        return engine
    except SQLAlchemyError as e:
        print(f"Error connecting to the database: {e}")
        return None



def get_dbinfo_metadata(host:str, port:int, service_name:str, username:str, password:str, owner:str):

    engine = connect_to_oracle(host, port, service_name, username, password)

    if engine is None:
        return None

    catalog_info = {}

    catalog_tables = {
        "tables": {
            "name": "ALL_TABLES",
            "order": "TABLE_NAME",
            "field_owner": "OWNER",
            "index": "TABLE_NAME",
            "fields": {}
        },
        "views": {
            "name": "ALL_VIEWS",
            "order": "VIEW_NAME",
            "field_owner": "OWNER",
            "index": "VIEW_NAME",
            "fields": {}
        },
        "indexes": {
            "name": "ALL_INDEXES",
            "order": "TABLE_NAME",
            "field_owner": "OWNER",
            "index": "INDEX_NAME",
            "fields": {}
        },
        "constraints": {
            "name": "ALL_CONSTRAINTS",
            "order": "TABLE_NAME",
            "field_owner": "OWNER",
            "index": "TABLE_NAME",
            "fields": {}
        },
        "procedures": {
            "name": "ALL_PROCEDURES",
            "order": "OBJECT_NAME",
            "field_owner": "OWNER",
            "index": "OBJECT_NAME",
            "fields": {}
        },
        "synonyms": {
            "name": "ALL_SYNONYMS",
            "order": "TABLE_NAME",
            "field_owner": "TABLE_OWNER",
            "index": "SYNONYM_NAME",
            "fields": {}
        }
    }

    with engine.connect() as connection:
        for object_type, table in catalog_tables.items():
            table_name = table['name']
            query = f"select column_name, data_type, data_length from SYS.ALL_TAB_COLS where TABLE_NAME = '{table_name}' order by COLUMN_ID"
            try:
                df = pd.read_sql(query, connection)
                fields_dict = {}
                for _, row in df.iterrows():
                    column_name = row['column_name']
                    fields_dict[column_name] = {
                        "data_type": row['data_type'],
                        "data_length": row['data_length']
                    }
                catalog_tables[object_type]["fields"] = fields_dict
            except SQLAlchemyError as e:
                print(f"Error retrieving {object_type}: {e}")
                catalog_tables[object_type]["fields"] = {}

    with engine.connect() as connection:
        for object_type, table in catalog_tables.items():
            table_name = table['name']
            order = table['order']
            field_owner = table['field_owner']
            fields = table['fields']
            fields = ', '.join(f"{fld}" for fld in list(table['fields'].keys()))
            query = f"select {fields} from sys.{table_name} where {field_owner} = '{owner}' order by {order}"
            try:
                df = pd.read_sql(query, connection)
                catalog_info[table_name] = df
            except SQLAlchemyError as e:
                print(f"Error retrieving {object_type}: {e}")
                catalog_info[table_name] = pd.DataFrame()  # Return an empty DataFrame in case of error

    return catalog_info

