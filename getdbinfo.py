from sqlalchemy import create_engine, MetaData
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import re
import time


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


def get_oracle_version(engine):
    """
    Retrieves the major version number of the connected Oracle database.

    This function queries the `v$version` view to obtain the Oracle database version.
    It extracts the major version number (e.g., 12, 11) from the version string and returns
    it as an integer for easy comparison.

    Parameters:
    -----------
    engine : sqlalchemy.engine.base.Engine
        The SQLAlchemy engine object used to connect to the Oracle database.

    Returns:
    --------
    int or None
        An integer representing the major Oracle database version if successful (e.g., 12, 11).
        Returns None if there is an error during the query execution or if the version cannot be parsed.

    Exceptions:
    -----------
    SQLAlchemyError
        If an error occurs while querying the database, the exception is caught, and an error message
        is printed. The function will return None in this case.

    Example:
    --------
    oracle_version = get_oracle_version(engine)
    if oracle_version:
        print(f"Oracle Database Major Version: {oracle_version}")
    else:
        print("Unable to retrieve Oracle version.")
    """
    
    with engine.connect() as connection:
        version_query = "SELECT banner FROM v$version WHERE banner LIKE 'Oracle%'"
        try:
            version_df = pd.read_sql(version_query, connection)
            oracle_version_str = version_df.iloc[0, 0]
            
            # Use regex to extract the first two digits (major version)
            match = re.search(r"(\d+)", oracle_version_str)
            if match:
                oracle_version = int(match.group(1))
                return oracle_version
            else:
                print(f"Unable to parse Oracle version from: {oracle_version_str}")
                return None
        except SQLAlchemyError as e:
            print(f"Error retrieving Oracle version: {e}")
            return None


def remove_illegal_chars(value):
    """
    Removes illegal or non-printable characters from a string, except for newline (\n) and carriage return (\r).

    Parameters:
    -----------
    value : str or any
        The input value to be cleaned. If it's a string, non-printable ASCII characters 
        (except newline and carriage return) are removed. If it's not a string, the value is returned as is.

    Returns:
    --------
    str or any
        The cleaned string without non-printable characters, or the original value if not a string.
    """

    if isinstance(value, str):
        return ''.join(c for c in value if c.isprintable() or c in ('\n', '\r'))
    return value


def extract_query_info(sql_query):
    """
    Extracts the table name and a list of selected fields from a given SQL query.

    The function uses regular expressions to parse the SQL query and extract the table name
    and the list of fields specified in the SELECT clause. If the table name includes a schema
    (e.g., "schema_name.table_name"), it returns only the table name, ignoring the schema part.

    Args:
        sql_query (str): The SQL query from which to extract the table name and fields.

    Returns:
        tuple: A tuple containing the table name (str) and a list of fields (list of str).
               If the query does not match the expected pattern, both values will be `None`.

    Example:
        sql_query = '''
        SELECT 
        sample_number,
        sampled_date,
        text_id,
        status
        FROM SGLOWNER.SAMPLE 
        WHERE LOGIN_DATE >= '01/01/2024'
        '''
        
        table_name, fields_list = extract_query_info(sql_query)
        # table_name -> 'SAMPLE'
        # fields_list -> ['sample_number', 'sampled_date', 'text_id', 'status']
    """
    # Regular expression to capture the fields between SELECT and FROM
    fields_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql_query, re.IGNORECASE | re.DOTALL)
    
    # Regular expression to capture the table name after the FROM clause
    table_match = re.search(r'FROM\s+([a-zA-Z0-9_\.]+)', sql_query, re.IGNORECASE)
    
    if not fields_match or not table_match:
        return None, None

    # Extract the list of fields and clean up whitespace and new lines
    fields_str = fields_match.group(1)
    fields_list = [field.strip().upper() for field in fields_str.split(',')]

    # Extract the full table name
    full_table_name = table_match.group(1).upper()
    
    # If the table name contains a dot, extract only the table name (ignore schema)
    if '.' in full_table_name:
        table_name = full_table_name.split('.')[1]
    else:
        table_name = full_table_name

    return table_name, fields_list


def get_dbinfo_metadata(connection_info: dict):
    """
    Retrieves detailed metadata and data for various Oracle catalog tables, such as tables, views, indexes, 
    constraints, procedures, and synonyms, for a specified owner in the database.

    This function connects to an Oracle database using the provided connection information, retrieves metadata 
    related to the database objects owned by the specified user, and stores the metadata in a structured dictionary.
    It includes information about columns (e.g., name, data type, length) and retrieves the actual data from each 
    catalog table.

    Parameters:
    -----------
    connection_info : dict
        A dictionary containing connection information to the Oracle database:
            - host : str
                The hostname or IP address of the Oracle database server.
            - port : int
                The port number used by the Oracle database server.
            - service_name : str
                The Oracle database service name.
            - user : str
                The username used for the database connection.
            - password : str
                The password used for the username.
            - owner : str
                The owner of the database objects whose metadata is being retrieved.

    Returns:
    --------
    dict
        A dictionary containing the metadata and data for various Oracle catalog tables. The dictionary 
        is structured as follows:

        {
            "tables": {
                "name": "ALL_TABLES",
                "order": "TABLE_NAME",
                "field_owner": "OWNER",
                "index": ["TABLE_NAME"],
                "fields": { ... },  # Column metadata for tables
                "data": pd.DataFrame()  # Data from ALL_TABLES
            },
            "views": { ... },  # Similar structure for views
            "indexes": { ... },  # Similar structure for indexes
            "constraints": { ... },  # Similar structure for constraints
            "procedures": { ... },  # Similar structure for procedures
            "synonyms": { ... }  # Similar structure for synonyms
        }

        The "fields" dictionary for each table contains entries of the form:
        {
            "column_name": {
                "data_type": <DATA_TYPE>,
                "data_length": <DATA_LENGTH>
            },
            ...
        }

    Notes:
    ------
    - This function retrieves both column metadata and data for the specified catalog tables.
    - It assumes the user has sufficient privileges to query system catalog views like SYS.ALL_TAB_COLS, 
      SYS.ALL_TABLES, and other system views.
    - In case of a SQLAlchemyError during data retrieval, the function handles the error gracefully by printing 
      an error message and populating the corresponding dictionary keys with empty values.
    - The function uses SQLAlchemy to manage database connections and execute SQL queries.
    """
    
    host = connection_info['host']
    port = connection_info['port']
    service_name = connection_info['service_name']
    username = connection_info['user']
    password = connection_info['password']
    owner = connection_info['owner']
    engine = connect_to_oracle(host, port, service_name, username, password)

    if engine is None:
        return None

    catalog_tables = {
        "tables": {
            "name": "ALL_TABLES",
            "order": "TABLE_NAME",
            "field_owner": "OWNER",
            "index": ['TABLE_NAME'],
            "fields": {},
            "data": pd.DataFrame()
        },
        "views": {
            "name": "ALL_VIEWS",
            "order": "VIEW_NAME",
            "field_owner": "OWNER",
            "index": ["VIEW_NAME"],
            "fields": {},
            "data": pd.DataFrame()
        },
        "indexes": {
            "name": "ALL_INDEXES",
            "order": "TABLE_NAME",
            "field_owner": "OWNER",
            "index": ["INDEX_NAME"],
            "fields": {},
            "data": pd.DataFrame()
        },
        "constraints": {
            "name": "ALL_CONSTRAINTS",
            "order": "TABLE_NAME",
            "field_owner": "OWNER",
            "index": ["TABLE_NAME"],
            "fields": {},
            "data": pd.DataFrame()
        },
        "procedures": {
            "name": "ALL_PROCEDURES",
            "order": "OBJECT_NAME",
            "field_owner": "OWNER",
            "index": ["OBJECT_NAME"],
            "fields": {},
            "data": pd.DataFrame()
        },
        "synonyms": {
            "name": "ALL_SYNONYMS",
            "order": "TABLE_NAME",
            "field_owner": "TABLE_OWNER",
            "index": ["SYNONYM_NAME"],
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

        # Define the generic fields that we want to extract from non-catalog tables.
        # These fields represent common metadata such as column name, data type, length, etc.
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
    # We also filter out columns that start with 'SYS_' because these are internal system fields defined by Oracle, and not part of user-defined schema.
        all_tables_df = catalog_tables["tables"]["data"] 
        for _, row in all_tables_df.iterrows():
            table_name = row['table_name']
            query = f"""
                SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, COLUMN_ID
                FROM SYS.ALL_TAB_COLS 
                WHERE TABLE_NAME = '{table_name}' 
                AND COLUMN_NAME NOT LIKE 'SYS\_%' ESCAPE '\\'
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


def get_dbinfo_table(connection_info: dict, table_name: str, sql_filter: str = None, sql_query: str = None):
    """
    Retrieve detailed information from the specified table in the Oracle database, including field names, types, 
    lengths, and index information.

    Parameters:
    -----------
    connection_info : dict
        Dictionary containing Oracle database connection information (host, port, service_name, user, password, owner).
    table_name : str
        The name of the table for which information will be retrieved.
    sql_filter : str, optional
        Optional SQL filter to be applied to the query.
    sql_query : str, optional
        Optional SQL query to execute instead of the default table information retrieval.

    Returns:
    --------
    dict
        A dictionary containing detailed information about the specified table, including fields, indexes, and data.
    """
    
    host = connection_info['host']
    port = connection_info['port']
    service_name = connection_info['service_name']
    username = connection_info['user']
    password = connection_info['password']
    owner = connection_info['owner']
    engine = connect_to_oracle(host, port, service_name, username, password)

    if engine is None:
        return None

    table_dict = {}

    # Obtain Oracle database version using the get_oracle_version function
    oracle_version = get_oracle_version(engine)
    if oracle_version is None:
        print("Error retrieving Oracle version")
        return None

    # Check if the version is 12 or higher
    is_version_12c_or_higher = oracle_version >= 12

    # If sql_query is provided, extract table name and fields from it
    if sql_query is not None:
        extracted_table_name, extracted_fields_list = extract_query_info(sql_query)
        
        # If the extracted table name or fields list is None, return None as something went wrong
        if extracted_table_name is None or extracted_fields_list is None:
            return None
        
        table_name = extracted_table_name
        fields_list = extracted_fields_list
        fields = ', '.join(fields_list)

    # Next, we retrieve the table's columns
    with engine.connect() as connection:
        if sql_query is None:
            query = f"""
                SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH
                FROM SYS.ALL_TAB_COLS 
                WHERE TABLE_NAME = '{table_name}' 
                AND COLUMN_NAME NOT LIKE 'SYS\_%' ESCAPE '\\'
                ORDER BY COLUMN_ID
            """
        else:
            # Convertimos la lista de campos a una cadena compatible con SQL ('field1', 'field2', ...)
            fields_string = ', '.join(f"'{field}'" for field in fields_list)
            query = f"""
                SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH 
                FROM SYS.ALL_TAB_COLS 
                WHERE TABLE_NAME = '{table_name}' 
                AND COLUMN_NAME IN ({fields_string})
                AND COLUMN_NAME NOT LIKE 'SYS\_%' ESCAPE '\\' 
                ORDER BY COLUMN_ID
                """

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
                "index": [],
                "fields": fields_dict,
                "data": pd.DataFrame()
            }
        except SQLAlchemyError as e:
            print(f"Error retrieving {table_name}: {e}")
            table_dict[table_name]["fields"] = {}

        if sql_query is None:
            index_list = []
            # Use the column search_condition_vc if it's Oracle 12c or higher, otherwise use search_condition
            search_condition_column = "SEARCH_CONDITION_VC" if is_version_12c_or_higher else "SEARCH_CONDITION"

            # Retrieve the unique index and its fields if it exists
            query = f"""
                SELECT {search_condition_column} 
                FROM SYS.ALL_CONSTRAINTS s 
                WHERE s.TABLE_NAME = '{table_name}' 
                AND s.CONSTRAINT_TYPE = 'C'
            """
            try:
                df = pd.read_sql(query, connection)
                if not df.empty:
                    index_list = []
                    for condition in df[search_condition_column.lower()]:
                        # Use a regular expression to capture the field name inside quotes
                        match = re.search(r'"([^"]+)"', condition)
                        if match:
                            field_name = match.group(1)  # Extract the field name without the quotes
                            index_list.append(field_name)
                    table_dict[table_name]["index"] = index_list
            except SQLAlchemyError as e:
                print(f"Error retrieving index of {table_name}: {e}")
                table_dict[table_name]["index"] = []

            fields = ', '.join(f"{fld}" for fld in list(fields_dict.keys()))
            index = ', '.join(f"{fld}" for fld in index_list)
            query = f"SELECT {fields} FROM {owner}.{table_name}"
            query2 = f" ORDER BY {index}"
            if sql_filter is not None:
                query = query + ' ' + sql_filter
            if len(index_list) > 0:
                query = query + query2
        else:
            query = sql_query

        try:
            df = pd.read_sql(query, connection)
            table_dict[table_name]["data"] = df
        except SQLAlchemyError as e:
            print(f"Error retrieving {table_name}: {e}")
            table_dict[table_name]["data"] = pd.DataFrame()

    return table_dict


def get_dbinfo_all_tables(connection_info: dict, total_records_limit: int = 500000, max_records_per_table: int = 50000):

    host = connection_info['host']
    port = connection_info['port']
    service_name = connection_info['service_name']
    username = connection_info['user']
    password = connection_info['password']
    owner = connection_info['owner']
    engine = connect_to_oracle(host, port, service_name, username, password)

    if engine is None:
        return None

    all_tables = {}
    total_records_retrieved = 0 
    tables_to_exclude = [
        'DB_FILES',
        'LIR_RPT_GENERADOS',
        'SAP_LINK',
        'TOOLBAR',
        'WORKFLOW_DETAIL'
    ]
    with engine.connect() as connection:
        # Consulta para encontrar tablas con 'AUDIT', 'CONFIG' o '_LOG' en el nombre
        query = f"""
        SELECT TABLE_NAME 
        FROM SYS.ALL_TABLES 
        WHERE OWNER = '{owner}' 
        AND (TABLE_NAME LIKE '%AUDIT%' OR TABLE_NAME LIKE '%CONFIG%' OR TABLE_NAME LIKE '%\_LOG' ESCAPE '\\')
        """
        try:
            df = pd.read_sql(query, connection)
            tables_to_exclude.extend(df['table_name'].tolist())
        except SQLAlchemyError as e:
            print(f"Error retrieving tables: {e}")

        tablas_excluded = ', '.join(f"'{table}'" for table in tables_to_exclude)
        query = f"select TABLE_NAME from SYS.ALL_TABLES where OWNER = '{owner}' and TABLE_NAME not in ({tablas_excluded})  order by TABLE_NAME"
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
            query = f"select column_name, data_type, data_length from SYS.ALL_TAB_COLS where TABLE_NAME = '{table_name}' and COLUMN_NAME <> 'AUDIT' order by COLUMN_ID"
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
            query = f"select {fields} from {owner}.{table_name} FETCH FIRST {max_records_per_table} ROWS ONLY"
            try:
                df = pd.read_sql(query, connection)
                cleaned_df = df.applymap(remove_illegal_chars)
                all_tables[table_name]["data"] = cleaned_df 
                num_rows = len(df)
                total_records_retrieved += num_rows
                if total_records_retrieved > total_records_limit:
                    print(f"Total records limit of {total_records_limit} reached working with {table_name}. Stopping further data retrieval.")
                    break
            except SQLAlchemyError as e:
                print(f"Error retrieving {catalog_table}: {e}")
                all_tables[table_name]["data"] = pd.DataFrame()

        # Una vez que se alcanza el límite, eliminar las tablas sin datos del diccionario
        if total_records_retrieved >= total_records_limit:
            for table_name in list(all_tables.keys()):
                if all_tables[table_name]["data"].empty:
                    del all_tables[table_name]

    return all_tables


def get_dbinfo_tables(tables: dict, connection_info: dict, total_records_limit: int = 500000, max_records_per_table: int = 50000, version: str = 'v8', sql_filter: str = None):
    """
    Retrieves data from specified tables with limits on the total number of records retrieved and the maximum records per table.

    Parameters:
    -----------
    tables : dict
        Dictionary containing metadata for tables including their names and fields.

    connection_info : dict
        Dictionary containing connection information to the Oracle database.

    total_records_limit : int, optional
        The maximum number of records to retrieve in total from all tables. Default is 500,000.

    max_records_per_table : int, optional
        The maximum number of records to retrieve from each table. Default is 50,000.

    Returns:
    --------
    tables : dict
        The updated dictionary containing metadata and data for each table retrieved from the database.
    """

    host = connection_info['host']
    port = connection_info['port']
    service_name = connection_info['service_name']
    username = connection_info['user']
    password = connection_info['password']
    owner = connection_info['owner']
    engine = connect_to_oracle(host, port, service_name, username, password)

    if engine is None:
        return None

    total_records_retrieved = 0  

    with engine.connect() as connection:
        for object_table, table_info in tables.items():
            table_name = table_info['name']
            query = f"select column_name, data_type, data_length from SYS.ALL_TAB_COLS where TABLE_NAME = '{table_name}' and COLUMN_NAME <> 'AUDIT' order by COLUMN_ID"
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

            # # Recuperamos el índice único y sus campos si lo tuviera
            # query = f"""
            #     SELECT s.COLUMN_NAME 
            #     FROM SYS.ALL_IND_COLUMNS s 
            #     WHERE s.INDEX_NAME = (
            #         SELECT i.INDEX_NAME 
            #         FROM SYS.ALL_INDEXES i 
            #         WHERE i.TABLE_NAME = '{table_name}' 
            #         AND i.UNIQUENESS = 'UNIQUE'
            #     ) 
            #     ORDER BY s.COLUMN_POSITION
            # """
            if version == 'v6':
                tables[object_table]["index"] = []
            else:
                # Vamos a utilizar la tabla de Constraints que nos ofrece mejores resultados para los campos clave
                query = f"""
                    select SEARCH_CONDITION_VC 
                    from SYS.ALL_CONSTRAINTS s 
                    where s.TABLE_NAME = '{table_name}' 
                    and s.CONSTRAINT_TYPE = 'C'
                """
                try:
                    df = pd.read_sql(query, connection) 
                    if not df.empty:
                        # Lista para almacenar los nombres de los campos extraídos
                        index_list = []
                        for condition in df['SEARCH_CONDITION_VC'.lower()]:
                            # Usamos una expresión regular para capturar el nombre del campo dentro de las comillas
                            match = re.search(r'"([^"]+)"', condition)
                            if match:
                                field_name = match.group(1)  # Extraemos el nombre del campo sin las comillas
                                index_list.append(field_name)

                        tables[object_table]["index"] = index_list
                except SQLAlchemyError as e:
                    print(f"Error retrieving index of {table_name}: {e}")
                    tables[object_table]["index"] = []

    # Now we use the information in the list of fields to retrieve all the information contained in the tables.
        for object_table, table_info in tables.items():
            table_name = table_info['name']
            index_list = table_info['index']
            index = ', '.join(f"{fld}" for fld in index_list)
            fields = ', '.join(f"{fld}" for fld in list(table_info['fields'].keys()))
            query = f"SELECT {fields} FROM {owner}.{table_name}"
            query2 = f" order by {index}"
            if sql_filter is not None:
                query = query + sql_filter
            if max_records_per_table > 0:
                if version == 'v8':
                    query3 = f" FETCH FIRST {max_records_per_table} ROWS ONLY"
                else:
                    query3 = f" where ROWNUM <= {max_records_per_table}"
                query = query + query3

            # if len(index_list) > 0:
            #     query = query + query2
            # if len(index_list) == 0:
            #     if version == 'v8':
            #         query = f"SELECT {fields} FROM {owner}.{table_name} FETCH FIRST {max_records_per_table} ROWS ONLY"
            #     else:
            #         query = f"SELECT {fields} FROM {owner}.{table_name} where ROWNUM <= {max_records_per_table} "
            # else:
            #     index = ', '.join(f"{fld}" for fld in index_list)
            #     query = f"SELECT {fields} FROM {owner}.{table_name} order by {index} FETCH FIRST {max_records_per_table} ROWS ONLY"
            try:
                time1 = time.time()
                df = pd.read_sql(query, connection)
                time2 = time.time()
                print(f'lectura: {time2 - time1} segundos')
                cleaned_df = df.applymap(remove_illegal_chars)
                time3 = time.time()
                print(f'apply: {time3 - time2} segundos')
                num_rows = len(df)
                print(f'registros: {num_rows}')
                if num_rows > 1000000:
                    return None
                tables[table_name]["data"] = cleaned_df 

                total_records_retrieved += num_rows
                if total_records_retrieved > total_records_limit and total_records_limit > 0:
                    print(f"Total records limit of {total_records_limit} reached working with {table_name}. Stopping further data retrieval.")
                    break
                
            except SQLAlchemyError as e:
                print(f"Error retrieving {table_name}: {e}")
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
    username = connection_info['user']
    password = connection_info['password']
    owner = connection_info['owner']
    engine = connect_to_oracle(host, port, service_name, username, password)

    if engine is None:
        return None

    tables_to_exclude = [
        'SAMPLE',
        'TEST',
        'RESULT',
        'DB_FILES',
        'LIR_RPT_GENERADOS',
        'PRODUCT_SPEC',
        'REP_ENVIO_REINTENTOS',
        'SAP_LINK',
        'TOOLBAR',
        'V_ODBC_PRODUCT_SPEC',
        'WORKFLOW_DETAIL',
        'VW_HIGHCHART_FILES',
        'VW_HTML_ELEMENT',
        'VW_HTML_PARAM'
    ]
    tables_with_clob = {}

    with engine.connect() as connection:
        # Consulta para encontrar tablas con 'AUDIT', 'CONFIG' o '_LOG' en el nombre
        query = f"""
        SELECT TABLE_NAME 
        FROM SYS.ALL_TABLES 
        WHERE OWNER = '{owner}' 
        AND (TABLE_NAME LIKE '%AUDIT%' OR TABLE_NAME LIKE '%CONFIG%' OR TABLE_NAME LIKE '%\_LOG' ESCAPE '\\')
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
                    "index": [],
                    "fields": {},  
                    "data": pd.DataFrame()
                }
        except SQLAlchemyError as e:
            print(f"Error retrieving tables: {e}")

    return tables_with_clob


def get_dbinfo_list_of_tables(tables: list, connection_info: dict, version: str = 'v8', sql_filter: str = None):

    info_tables = {}

    for table in tables:
        table_name = table
        info_tables[table_name] = {
            "name": table_name,
            "order": "",
            "field_owner": "",
            "index": [],
            "fields": {},  
            "data": pd.DataFrame()
        }

    info_tables = get_dbinfo_tables(info_tables, connection_info, total_records_limit=0, max_records_per_table=0, version=version, sql_filter=sql_filter)
    return info_tables
