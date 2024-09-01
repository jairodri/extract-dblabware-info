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
    """
    Retrieves metadata information from the Oracle database's catalog tables for the specified owner and 
    returns it as a structured dictionary.

    This function connects to an Oracle database using the provided credentials and retrieves metadata 
    information for various types of database objects (tables, views, indexes, constraints, procedures, 
    and synonyms) owned by the specified user. The metadata includes details about columns (name, data type, 
    and length) and the actual data for each catalog object type.

    Parameters:
    -----------
    host : str
        The hostname or IP address of the Oracle database server.
    port : int
        The port number on which the Oracle database server is listening.
    service_name : str
        The service name of the Oracle database.
    username : str
        The username used to connect to the Oracle database.
    password : str
        The password associated with the username for Oracle database connection.
    owner : str
        The owner of the database objects for which metadata is being retrieved.

    Returns:
    --------
    dict
        A dictionary containing metadata and data for various Oracle catalog tables. The dictionary 
        is structured as follows:

        {
            "tables": {
                "name": "ALL_TABLES",
                "order": "TABLE_NAME",
                "field_owner": "OWNER",
                "index": "TABLE_NAME",
                "fields": { ... },  # Dictionary containing column metadata
                "data": pd.DataFrame()  # DataFrame containing data from the table
            },
            "views": { ... },  # Similar structure for views
            "indexes": { ... },  # Similar structure for indexes
            "constraints": { ... },  # Similar structure for constraints
            "procedures": { ... },  # Similar structure for procedures
            "synonyms": { ... }  # Similar structure for synonyms
        }

        Each "fields" dictionary contains entries of the form:
        {
            "column_name": {
                "data_type": <DATA_TYPE>,
                "data_length": <DATA_LENGTH>
            },
            ...
        }

    Notes:
    ------
    - The function assumes that the Oracle database connection is stable and that the user has sufficient 
      privileges to query the system catalog views.
    - In case of any SQLAlchemyError during data retrieval, the function catches the exception, prints an 
      error message, and stores an empty DataFrame or dictionary in the result.
    - The function uses SQLAlchemy to handle database connections and execute SQL queries.
    """
    engine = connect_to_oracle(host, port, service_name, username, password)

    if engine is None:
        return None

    catalog_tables = {
        "tables": {
            "name": "ALL_TABLES",
            "order": "TABLE_NAME",
            "field_owner": "OWNER",
            "index": "TABLE_NAME",
            "fields": {},
            "data": pd.DataFrame()
        },
        "views": {
            "name": "ALL_VIEWS",
            "order": "VIEW_NAME",
            "field_owner": "OWNER",
            "index": "VIEW_NAME",
            "fields": {},
            "data": pd.DataFrame()
        },
        "indexes": {
            "name": "ALL_INDEXES",
            "order": "TABLE_NAME",
            "field_owner": "OWNER",
            "index": "INDEX_NAME",
            "fields": {},
            "data": pd.DataFrame()
        },
        "constraints": {
            "name": "ALL_CONSTRAINTS",
            "order": "TABLE_NAME",
            "field_owner": "OWNER",
            "index": "TABLE_NAME",
            "fields": {},
            "data": pd.DataFrame()
        },
        "procedures": {
            "name": "ALL_PROCEDURES",
            "order": "OBJECT_NAME",
            "field_owner": "OWNER",
            "index": "OBJECT_NAME",
            "fields": {},
            "data": pd.DataFrame()
        },
        "synonyms": {
            "name": "ALL_SYNONYMS",
            "order": "TABLE_NAME",
            "field_owner": "TABLE_OWNER",
            "index": "SYNONYM_NAME",
            "fields": {},
            "data": pd.DataFrame()
        }
    }

    # First, we retrieve the fields of the column name, its type and length for the main catalog tables.
    # These values will be stored in the “fields” key of the dictionary with the catalog data.
    # We will need this information later to determine if a field is CLOB.
    with engine.connect() as connection:
        for catalog_table, table_info in catalog_tables.items():
            table_name = table_info['name']
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
                catalog_tables[catalog_table]["fields"] = fields_dict
            except SQLAlchemyError as e:
                print(f"Error retrieving {catalog_table}: {e}")
                catalog_tables[catalog_table]["fields"] = {}

    # Now we use the information in the list of fields to retrieve all the information contained in the main tables of the catalog.
    # We also use the predefined information about the owner of the tables and the order of retrieval of the query.
    # The retrieved information will be stored in the “data” key of the dictionary with the catalog tables.
    with engine.connect() as connection:
        for catalog_table, table_info in catalog_tables.items():
            table_name = table_info['name']
            order = table_info['order']
            field_owner = table_info['field_owner']
            fields = ', '.join(f"{fld}" for fld in list(table_info['fields'].keys()))
            query = f"select {fields} from sys.{table_name} where {field_owner} = '{owner}' order by {order}"
            try:
                df = pd.read_sql(query, connection)
                catalog_tables[catalog_table]["data"] = df 
            except SQLAlchemyError as e:
                print(f"Error retrieving {catalog_table}: {e}")
                catalog_tables[catalog_table]["data"] = pd.DataFrame()

    generic_fields = {
        'COLUMN_NAME': {'data_type': 'VARCHAR2', 'data_length': 128},
        'DATA_TYPE': {'data_type': 'VARCHAR2', 'data_length': 128},
        'DATA_LENGTH': {'data_type': 'NUMBER', 'data_length': 22},
        'DATA_PRECISION': {'data_type': 'NUMBER', 'data_length': 22},
        'DATA_SCALE': {'data_type': 'NUMBER', 'data_length': 22},
        'NULLABLE': {'data_type': 'VARCHAR2', 'data_length': 1},
        'COLUMN_ID': {'data_type': 'NUMBER', 'data_length': 22}
    }
    # Finally, we retrieve the catalog information of all tables defined in ALL_TABLES and create an entry for each table in the catalog information dictionary.
    with engine.connect() as connection:
        all_tables_df = catalog_tables["tables"]["data"] 
        for _, row in all_tables_df.iterrows():
            table_name = row['table_name']
            query = f"""
                SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, COLUMN_ID
                FROM SYS.ALL_TAB_COLS 
                WHERE TABLE_NAME = '{table_name}' 
                ORDER BY COLUMN_NAME
            """
            try:
                df = pd.read_sql(query, connection)
                catalog_tables[table_name] = {
                    "name": table_name,
                    "order": "",
                    "field_owner": "",
                    "index": "",
                    "fields": generic_fields,  
                    "data": df
                }
            except SQLAlchemyError as e:
                print(f"Error retrieving column information for table {table_name}: {e}")


    return catalog_tables


def get_dbinfo_table(host:str, port:int, service_name:str, username:str, password:str, owner:str, table_name: str):

    engine = connect_to_oracle(host, port, service_name, username, password)

    if engine is None:
        return None

    table_dict = {}

    # First, we retrieve the fields of the column name, its type and length for table.
    # These values will be stored in the “fields” key of the dictionary with the catalog data.
    # We will need this information later to determine if a field is CLOB.
    with engine.connect() as connection:
        query = f"select column_name, data_type, data_length from SYS.ALL_TAB_COLS where TABLE_NAME = '{table_name}' order by COLUMN_ID"
        fields_dict = {}
        try:
            df = pd.read_sql(query, connection)
            for _, row in df.iterrows():
                column_name = row['column_name']
                fields_dict[column_name] = {
                    "data_type": row['data_type'],
                    "data_length": row['data_length']
                }
            table_dict[table_name] = {
                "name": table_name,
                "order": "",
                "field_owner": "",
                "index": "",
                "fields": fields_dict,  
                "data": pd.DataFrame()
            }
        except SQLAlchemyError as e:
            print(f"Error retrieving {table_name}: {e}")
            table_dict[table_name]["fields"] = {}

        fields = ', '.join(f"{fld}" for fld in list(fields_dict.keys()))
        query = f'select {fields} from {owner}.{table_name}'
        try:
            df = pd.read_sql(query, connection)
            table_dict[table_name]["data"] = df
        except SQLAlchemyError as e:
            print(f"Error retrieving {table_name}: {e}")
            table_dict[table_name]["data"] = pd.DataFrame()

    return table_dict


def get_dbinfo_all_tables(host:str, port:int, service_name:str, username:str, password:str, owner:str):

    engine = connect_to_oracle(host, port, service_name, username, password)

    if engine is None:
        return None

    all_tables = {}

    with engine.connect() as connection:
        query = f"select TABLE_NAME from SYS.ALL_TABLES where OWNER = '{owner}' order by TABLE_NAME"
        try:
            df = pd.read_sql(query, connection)
            for _, row in df.iterrows():
                table_name = row['table_name']
                all_tables[table_name] = {
                    "name": table_name,
                    "order": "",
                    "field_owner": "",
                    "index": "",
                    "fields": {},  
                    "data": pd.DataFrame()
                }
        except SQLAlchemyError as e:
            print(f"Error retrieving tables: {e}")

        for object_table, table_info in all_tables.items():
            table_name = table_info['name']
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
                all_tables[object_table]["fields"] = fields_dict
            except SQLAlchemyError as e:
                print(f"Error retrieving {object_table}: {e}")
                all_tables[object_table]["fields"] = {}

    # Now we use the information in the list of fields to retrieve all the information contained in the tables.
        for object_table, table_info in all_tables.items():
            table_name = table_info['name']
            fields = ', '.join(f"{fld}" for fld in list(table_info['fields'].keys()))
            query = f"select {fields} from {owner}.{table_name}"
            try:
                df = pd.read_sql(query, connection)
                all_tables[table_name]["data"] = df 
            except SQLAlchemyError as e:
                print(f"Error retrieving {catalog_table}: {e}")
                all_tables[table_name]["data"] = pd.DataFrame()

    return all_tables


def get_dbinfo_tables(tables: dict, connection_info: dict):

    host = connection_info['host']
    port = connection_info['port']
    service_name = connection_info['service_name']
    username = connection_info['username']
    password = connection_info['password']
    owner = connection_info['owner']
    engine = connect_to_oracle(host, port, service_name, username, password)

    if engine is None:
        return None

    with engine.connect() as connection:
        for object_table, table_info in tables.items():
            table_name = table_info['name']
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
                tables[object_table]["fields"] = fields_dict
            except SQLAlchemyError as e:
                print(f"Error retrieving {object_table}: {e}")
                tables[object_table]["fields"] = {}

    # Now we use the information in the list of fields to retrieve all the information contained in the tables.
        for object_table, table_info in tables.items():
            table_name = table_info['name']
            fields = ', '.join(f"{fld}" for fld in list(table_info['fields'].keys()))
            query = f"select {fields} from {owner}.{table_name}"
            try:
                df = pd.read_sql(query, connection)
                tables[table_name]["data"] = df 
            except SQLAlchemyError as e:
                print(f"Error retrieving {catalog_table}: {e}")
                tables[table_name]["data"] = pd.DataFrame()

    return tables


def get_dbinfo_tables_with_clob(connection_info: dict):
    """
    Retrieves information about tables containing CLOB fields, excluding specified tables.

    Parameters:
    -----------
    connection_info : dict
        Dictionary containing connection information to the Oracle database.

    Returns:
    --------
    tables_with_clob : dict
        Dictionary containing metadata for tables that have CLOB fields.
    """
 
    host = connection_info['host']
    port = connection_info['port']
    service_name = connection_info['service_name']
    username = connection_info['username']
    password = connection_info['password']
    owner = connection_info['owner']
    engine = connect_to_oracle(host, port, service_name, username, password)

    if engine is None:
        return None

    tables_to_exclude = [
        'SAMPLE',
        'TEST',
        'RESULT'
    ]
    tables_with_clob = {}

    with engine.connect() as connection:
        # Consulta para encontrar tablas con 'AUDIT' o '_LOG' en el nombre
        query = f"""
        SELECT TABLE_NAME 
        FROM SYS.ALL_TABLES 
        WHERE OWNER = '{owner}' 
        AND (TABLE_NAME LIKE '%AUDIT%' OR TABLE_NAME LIKE '%\_LOG' ESCAPE '\\')
        """
        try:
            df = pd.read_sql(query, connection)
            tables_to_exclude.extend(df['table_name'].tolist())
        except SQLAlchemyError as e:
            print(f"Error retrieving tables: {e}")

        tablas_excluded = ', '.join(f"'{table}'" for table in tables_to_exclude)
        query = f"select distinct TABLE_NAME from SYS.ALL_TAB_COLS where OWNER = '{owner}' " \
                f" and TABLE_NAME not in ({tablas_excluded}) " \
                f" and DATA_TYPE = 'CLOB'" \
                f" order by TABLE_NAME"
        try:
            df = pd.read_sql(query, connection)
            for _, row in df.iterrows():
                table_name = row['table_name']
                tables_with_clob[table_name] = {
                    "name": table_name,
                    "order": "",
                    "field_owner": "",
                    "index": "",
                    "fields": {},  
                    "data": pd.DataFrame()
                }
        except SQLAlchemyError as e:
            print(f"Error retrieving tables: {e}")

    return tables_with_clob
