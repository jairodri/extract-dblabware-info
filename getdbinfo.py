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


def get_dbinfo_table(connection_info: dict, table_name: str, sql_filter: str = None, sql_query: str = None, max_records_per_table: int = 50000):
    """
    Retrieve detailed information from the specified table in the Oracle database, including field names, types,
    lengths, indexes, and data, with an optional limit on the number of records retrieved.

    Parameters:
    -----------
    connection_info : dict
        Dictionary containing Oracle database connection information, including host, port, service name, 
        username, password, and owner.
    table_name : str
        The name of the table for which information will be retrieved.
    sql_filter : str, optional
        An optional SQL filter to be applied to the query for filtering rows from the table.
    sql_query : str, optional
        An optional custom SQL query to retrieve specific information. If provided, `table_name` and `fields_list` 
        are extracted from this query.
    max_records_per_table : int, optional, default=50000
        The maximum number of records to retrieve from the table. This limit will be applied to the result set 
        using a `FETCH FIRST N ROWS ONLY` clause.

    Returns:
    --------
    dict
        A dictionary containing detailed information about the specified table, including fields, indexes, and 
        data. Returns None if an error occurs or no data is retrieved.
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
                AND COLUMN_NAME <> 'AUDIT'
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
            # Construct the basic SQL query to select fields from the specified table
            query = f"SELECT {fields} FROM {owner}.{table_name}"
            # Create the ORDER BY clause if there are any indexed fields
            query2 = f"ORDER BY {index}" 
            # Create the clause to limit the number of rows returned by the query
            query3 = f"FETCH FIRST {max_records_per_table} ROWS ONLY"
            # If a SQL filter is provided, append it to the query
            if sql_filter is not None:
                query = query + ' ' + sql_filter
            # If there are indexed fields, append the ORDER BY clause to the query
            if len(index_list) > 0:
                query = query + ' ' + query2
            # Finally, append the row limit clause to the query
            query = query + ' ' + query3 
        else:
            query = sql_query

        try:
            df = pd.read_sql(query, connection)
            table_dict[table_name]["data"] = df
        except SQLAlchemyError as e:
            print(f"Error retrieving {table_name}: {e}")
            table_dict[table_name]["data"] = pd.DataFrame()

    return table_dict


def get_dbinfo_all_tables(connection_info: dict, tables_to_exclude: list, total_records_limit: int = 500000, max_records_per_table: int = 50000):
    """
    Retrieves metadata and data for all tables in an Oracle database, excluding specified tables and views, 
    while respecting limits on the total number of records and the maximum number of records per table.

    Parameters:
    -----------
    connection_info : dict
        Dictionary containing Oracle database connection details (host, port, service_name, user, password, owner).
    tables_to_exclude : list
        List of table names to exclude from the data retrieval process.
    total_records_limit : int, optional
        The total number of records to retrieve across all tables. Once this limit is reached, the process will stop.
        Defaults to 500,000 records.
    max_records_per_table : int, optional
        The maximum number of records to retrieve from a single table. This prevents retrieving too much data from any 
        one table. Defaults to 50,000 records per table.

    Returns:
    --------
    all_tables : dict
        A dictionary where the keys are table names and the values are metadata and data from each table that was retrieved.
        If no tables are retrieved or the connection fails, it returns None.
    
    Notes:
    ------
    - This function will exclude any tables specified in the `tables_to_exclude` parameter, as well as any tables 
      containing 'AUDIT', 'CONFIG', or '_LOG' in their names.
    - Views are excluded from the data retrieval process.
    - If the total number of retrieved records exceeds `total_records_limit`, the function will stop retrieving data 
      and return the tables that have been processed up to that point.
    - Tables without data (empty) will be removed from the returned dictionary.
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

    all_tables = {}
    total_records_retrieved = 0 

    with engine.connect() as connection:

        # Query to find tables with 'AUDIT', 'CONFIG' or '_LOG' in the name
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

        query = f"""
        select distinct TABLE_NAME 
        from SYS.ALL_TAB_COLS 
        where OWNER = '{owner}'
        and TABLE_NAME not in ({tablas_excluded})
        AND TABLE_NAME NOT IN (
            SELECT OBJECT_NAME 
            FROM SYS.ALL_OBJECTS 
            WHERE OWNER = '{owner}' 
            AND OBJECT_TYPE = 'VIEW'
        )
        order by TABLE_NAME
        """
        try:
            df = pd.read_sql(query, connection)
            for _, row in df.iterrows():
                table_name = row['table_name']

                # Call get_dbinfo_table to retrieve table information
                table_info = get_dbinfo_table(connection_info, table_name, max_records_per_table=max_records_per_table)
                
                # Store the table info in the dictionary using table_name as the key
                if table_info is not None:
                    all_tables[table_name] = table_info[table_name]

                num_rows = len(table_info[table_name]['data'])
                total_records_retrieved += num_rows
                if total_records_retrieved > total_records_limit:
                    print(f"Total records limit of {total_records_limit} reached working with {table_name}. Stopping further data retrieval.")
                    break

        except SQLAlchemyError as e:
            print(f"Error retrieving tables: {e}")

        # Una vez que se alcanza el límite, eliminar las tablas sin datos del diccionario
        if total_records_retrieved >= total_records_limit:
            for table_name in list(all_tables.keys()):
                if all_tables[table_name]["data"].empty:
                    del all_tables[table_name]

    return all_tables


def get_dbinfo_tables_with_clob(connection_info: dict, tables_to_exclude: list, max_records_per_table: int = 50000):
    """
    Retrieves information about tables containing CLOB fields, excluding a predefined list of tables. For each table 
    with a CLOB field, the function calls `get_dbinfo_table` to gather detailed metadata and data, with a limit 
    on the maximum number of records retrieved per table.

    Parameters:
    -----------
    connection_info : dict
        Dictionary containing connection details for the Oracle database, including host, port, service name, 
        username, password, and owner.
    
    tables_to_exclude : list
        A list of table names to be excluded from the query. This list can include tables loaded from the `.env`
        file under the variable `TABLES_WITH_CLOB_TO_EXCLUDE`, as well as other tables found via a query that match 
        certain patterns (e.g., tables with 'AUDIT', 'CONFIG', or '_LOG' in the name).
    
    max_records_per_table : int, optional
        The maximum number of records to retrieve from each table with CLOB fields. This helps to limit the 
        amount of data retrieved for large tables. Defaults to 50,000 records.

    Returns:
    --------
    tables_with_clob : dict
        A dictionary where the keys are table names and the values are detailed metadata information, including data 
        for each table containing CLOB fields, retrieved via the `get_dbinfo_table` function. Returns None if the 
        connection to the database fails.
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

    tables_with_clob = {}

    with engine.connect() as connection:
        # Query to find tables with 'AUDIT', 'CONFIG' or '_LOG' in the name
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

        # Create the exclusion list for the query
        tablas_excluded = ', '.join(f"'{table}'" for table in tables_to_exclude)

        # Query to retrieve tables with CLOB fields that are not in the exclusion list
        query = f"""
        select distinct TABLE_NAME 
        from SYS.ALL_TAB_COLS 
        where OWNER = '{owner}'
        and TABLE_NAME not in ({tablas_excluded})
        AND TABLE_NAME NOT IN (
            SELECT OBJECT_NAME 
            FROM SYS.ALL_OBJECTS 
            WHERE OWNER = '{owner}' 
            AND OBJECT_TYPE = 'VIEW'
        )
        and DATA_TYPE = 'CLOB'
        order by TABLE_NAME
        """
        try:
            df = pd.read_sql(query, connection)
            # For each table with CLOB fields, call `get_dbinfo_table` to get detailed information
            for _, row in df.iterrows():
                table_name = row['table_name']

                # Call get_dbinfo_table to retrieve table information
                table_info = get_dbinfo_table(connection_info, table_name, max_records_per_table=max_records_per_table)
                
                # Store the table info in the dictionary using table_name as the key
                if table_info is not None:
                    tables_with_clob[table_name] = table_info[table_name]

        except SQLAlchemyError as e:
            print(f"Error retrieving tables: {e}")

    return tables_with_clob


def get_dbinfo_list_of_tables(tables: list, connection_info: dict, max_records_per_table: int = 50000):
    """
    Retrieves detailed information for a list of tables from an Oracle database.

    For each table in the provided list, the function calls `get_dbinfo_table` to gather metadata such as column 
    names, data types, and other table properties, with a limit on the maximum number of records retrieved per table.
    The results are stored in a dictionary with the table names as keys.

    Parameters:
    -----------
    tables : list
        A list of table names for which data is to be retrieved.

    connection_info : dict
        Dictionary containing connection details for the Oracle database, including host, port, service name, 
        username, password, and owner.
    
    max_records_per_table : int, optional
        The maximum number of records to retrieve from each table. This helps to limit the amount of data 
        retrieved for large tables. Defaults to 50,000 records.

    Returns:
    --------
    info_tables : dict
        A dictionary where the keys are table names and the values are detailed metadata information 
        about each table, as returned by `get_dbinfo_table`. If a table's information cannot be retrieved, 
        it is excluded from the result.
    """
    info_tables = {}

    for table in tables:
        table_name = table
        # Call get_dbinfo_table to retrieve table information
        table_info = get_dbinfo_table(connection_info, table_name, max_records_per_table=max_records_per_table)
        
        # Store the table info in the dictionary using table_name as the key
        if table_info is not None:
            info_tables[table_name] = table_info[table_name]

    return info_tables
