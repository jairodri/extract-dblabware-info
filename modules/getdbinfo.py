"""
Oracle Database Information Extraction Module

This module provides functions to extract metadata and data from Oracle databases,
including table structures, indexes, constraints, and actual table data.

Author: Database Analysis Team
Version: 2.0
"""

import os
import re
import time
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
from sqlalchemy import create_engine, MetaData
from sqlalchemy.exc import SQLAlchemyError
import cx_Oracle


def connect_to_oracle(host: str, port: int, service_name: str, username: str, password: str):
    """
    Establish connection to Oracle database using SQLAlchemy and cx_Oracle.
    
    Note: From version 8 onwards, cx_Oracle has been renamed to oracledb, 
    though cx_Oracle is still functional in earlier versions.

    Args:
        host (str): Database server address
        port (int): Database server port
        service_name (str): Oracle service name
        username (str): Database username
        password (str): Database password

    Returns:
        sqlalchemy.engine.Engine: SQLAlchemy Engine object if successful, None otherwise
    """
    # Create connection string in SQLAlchemy/cx_Oracle format
    connection_string = f'oracle+cx_oracle://{username}:{password}@{host}:{port}/?service_name={service_name}'
    
    try:
        engine = create_engine(connection_string)
        print(f"Successfully connected to Oracle database: {service_name}")
        return engine
    except SQLAlchemyError as e:
        print(f"Error connecting to database {service_name}: {e}")
        return None


def get_oracle_version(engine) -> Optional[int]:
    """
    Retrieve the major version number of the connected Oracle database.

    This function queries the v$version view to obtain Oracle database version
    and extracts the major version number for compatibility checks.

    Args:
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine for database connection

    Returns:
        Optional[int]: Major Oracle version number (e.g., 12, 11) or None if error occurs

    Example:
        >>> oracle_version = get_oracle_version(engine)
        >>> if oracle_version and oracle_version >= 12:
        ...     print("Using Oracle 12c+ features")
    """
    try:
        with engine.connect() as connection:
            version_query = "SELECT banner FROM v$version WHERE banner LIKE 'Oracle%'"
            version_df = pd.read_sql(version_query, connection)
            
            if version_df.empty:
                print("No Oracle version information found")
                return None
                
            oracle_version_str = version_df.iloc[0, 0]
            
            # Extract major version number using regex
            match = re.search(r"(\d+)", oracle_version_str)
            if match:
                oracle_version = int(match.group(1))
                print(f"Detected Oracle version: {oracle_version}")
                return oracle_version
            else:
                print(f"Unable to parse Oracle version from: {oracle_version_str}")
                return None
                
    except SQLAlchemyError as e:
        print(f"Error retrieving Oracle version: {e}")
        return None


def remove_illegal_chars(value: Any) -> Any:
    """
    Remove illegal or non-printable characters from strings.
    
    Preserves newline (\n) and carriage return (\r) characters while removing
    other non-printable ASCII characters that could cause issues in data export.

    Args:
        value (Any): Input value to be cleaned

    Returns:
        Any: Cleaned string if input was string, otherwise original value
    """
    if isinstance(value, str):
        return ''.join(c for c in value if c.isprintable() or c in ('\n', '\r'))
    return value


def extract_query_info(sql_query: str) -> Tuple[Optional[str], Optional[List[str]]]:
    """
    Extract table name and selected fields from a SQL query.

    Uses regular expressions to parse SQL SELECT statements and extract
    the target table and field list. Handles aliases and schema prefixes.

    Args:
        sql_query (str): SQL SELECT query to parse

    Returns:
        Tuple[Optional[str], Optional[List[str]]]: Tuple containing table name 
                                                  and list of fields, or (None, None) if parsing fails

    Example:
        >>> query = '''
        ... SELECT alias.sample_number, alias.sampled_date, alias.text_id
        ... FROM SGLOWNER.SAMPLE 
        ... WHERE LOGIN_DATE >= '01/01/2024'
        ... '''
        >>> table_name, fields = extract_query_info(query)
        >>> # Returns: ('SAMPLE', ['sample_number', 'sampled_date', 'text_id'])
    """
    # Extract fields between SELECT and FROM
    fields_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql_query, re.IGNORECASE | re.DOTALL)
    
    # Extract table name after FROM
    table_match = re.search(r'FROM\s+([a-zA-Z0-9_\.]+)', sql_query, re.IGNORECASE)
    
    if not fields_match or not table_match:
        print("Unable to parse SQL query - invalid SELECT/FROM structure")
        return None, None

    # Clean and normalize field names
    fields_str = fields_match.group(1)
    fields_list = [field.strip().upper() for field in fields_str.split(',')]

    # Remove alias prefixes (text before dot) from field names
    fields_list = [field.split('.')[-1] for field in fields_list]

    # Extract table name, removing schema prefix if present
    full_table_name = table_match.group(1).upper()
    table_name = full_table_name.split('.')[-1] if '.' in full_table_name else full_table_name

    return table_name, fields_list


def print_column_types(table_dict: Dict[str, Dict[str, Any]]) -> None:
    """
    Print data types of columns for debugging purposes.

    Args:
        table_dict (Dict[str, Dict[str, Any]]): Dictionary containing table metadata and data
    """
    for table_name, table_info in table_dict.items():
        if "data" in table_info and not table_info["data"].empty:
            print(f"Table: {table_name}")
            print(table_info["data"].dtypes)
            print("\n")


def get_dbinfo_metadata(connection_info: Dict[str, str]) -> Optional[Dict[str, Any]]:
    """
    Retrieve comprehensive metadata from Oracle database catalog tables.

    Extracts metadata for tables, views, indexes, constraints, procedures, and synonyms
    for a specified owner, including both column definitions and actual catalog data.

    Args:
        connection_info (Dict[str, str]): Database connection parameters
            - host: Database server hostname/IP
            - port: Database server port  
            - service_name: Oracle service name
            - user: Database username
            - password: Database password
            - owner: Schema owner for metadata extraction

    Returns:
        Optional[Dict[str, Any]]: Dictionary containing catalog metadata and data,
                                 None if connection fails

    Structure:
        {
            "tables": {
                "name": "ALL_TABLES",
                "fields": {column_name: {data_type, data_length}, ...},
                "data": pd.DataFrame  # Actual catalog data
            },
            "views": {...},
            "indexes": {...},
            "constraints": {...},
            "procedures": {...},
            "synonyms": {...},
            "<table_name>": {  # Individual table metadata
                "fields": {...},
                "data": pd.DataFrame  # Column definitions
            }
        }
    """
    # Extract connection parameters
    host = connection_info['host']
    port = connection_info['port']
    service_name = connection_info['service_name']
    username = connection_info['user']
    password = connection_info['password']
    owner = connection_info['owner']
    
    print(f'Extracting metadata from Oracle database {service_name} for owner {owner}...')
    
    # Establish database connection
    engine = connect_to_oracle(host, port, service_name, username, password)
    if engine is None:
        return None

    # Define catalog tables to extract
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

    try:
        with engine.connect() as connection:
            # Step 1: Extract column metadata for each catalog table
            print("Extracting catalog table schemas...")
            for catalog_table, table_info in catalog_tables.items():
                table_name = table_info['name']
                query = f"""
                    SELECT column_name, data_type, data_length 
                    FROM SYS.ALL_TAB_COLS 
                    WHERE TABLE_NAME = '{table_name}' 
                    ORDER BY COLUMN_ID
                """
                
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
                    print(f"Error retrieving schema for {catalog_table}: {e}")
                    catalog_tables[catalog_table]["fields"] = {}

            # Step 2: Extract actual data from catalog tables
            print("Extracting catalog data...")
            for catalog_table, table_info in catalog_tables.items():
                table_name = table_info['name']
                order = table_info['order']
                field_owner = table_info['field_owner']
                
                # Build field list for SELECT query
                fields = ', '.join(table_info['fields'].keys())
                if not fields:  # Skip if no fields retrieved
                    continue
                    
                query = f"""
                    SELECT {fields} 
                    FROM SYS.{table_name} 
                    WHERE {field_owner} = '{owner}' 
                    ORDER BY {order}
                """

                try:
                    df = pd.read_sql(query, connection)
                    catalog_tables[catalog_table]["data"] = df
                    print(f"Retrieved {len(df)} records from {table_name}")
                    
                except SQLAlchemyError as e:
                    print(f"Error retrieving data from {catalog_table}: {e}")
                    catalog_tables[catalog_table]["data"] = pd.DataFrame()

            # Step 3: Extract column information for all user tables
            print("Extracting individual table column definitions...")
            
            # Define standard fields for table column metadata
            generic_fields = {
                'COLUMN_NAME': {'data_type': 'VARCHAR2', 'data_length': 128},
                'DATA_TYPE': {'data_type': 'VARCHAR2', 'data_length': 128},
                'DATA_LENGTH': {'data_type': 'NUMBER', 'data_length': 22},
                'DATA_PRECISION': {'data_type': 'NUMBER', 'data_length': 22},
                'DATA_SCALE': {'data_type': 'NUMBER', 'data_length': 22},
                'NULLABLE': {'data_type': 'VARCHAR2', 'data_length': 1},
                'COLUMN_ID': {'data_type': 'NUMBER', 'data_length': 22}
            }

            # Process each table found in the catalog
            all_tables_df = catalog_tables["tables"]["data"]
            for _, row in all_tables_df.iterrows():
                table_name = row['table_name']
                
                # Extract column definitions for this table
                query = f"""
                    SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, 
                           DATA_SCALE, NULLABLE, COLUMN_ID
                    FROM SYS.ALL_TAB_COLS 
                    WHERE TABLE_NAME = '{table_name}' 
                    AND OWNER = '{owner}'
                    AND COLUMN_NAME NOT LIKE 'SYS\_%' ESCAPE '\\'
                    ORDER BY COLUMN_ID
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
                    print(f"Error retrieving column info for table {table_name}: {e}")

            print(f"Metadata extraction completed for {len(catalog_tables)} catalog objects")
            return catalog_tables
            
    except Exception as e:
        print(f"Unexpected error during metadata extraction: {e}")
        return None


def get_dbinfo_table(connection_info: Dict[str, str], table_name: str, 
                    sql_filter: Optional[str] = None, sql_query: Optional[str] = None, 
                    max_records_per_table: int = 50000, 
                    engine=None) -> Optional[Dict[str, Any]]:
    """
    Retrieve detailed information and data from a specific Oracle table.

    Extracts table schema information including column names, types, lengths, 
    constraints, and retrieves actual table data with optional filtering and row limits.

    Args:
        connection_info (Dict[str, str]): Database connection parameters
        table_name (str): Name of the table to extract
        sql_filter (Optional[str]): Optional WHERE clause for data filtering
        sql_query (Optional[str]): Optional custom SQL query (overrides table_name)
        max_records_per_table (int): Maximum number of records to retrieve (default: 50000)
        engine: Optional SQLAlchemy engine to reuse existing connection

    Returns:
        Optional[Dict[str, Any]]: Dictionary containing table metadata and data,
                                 None if extraction fails

    Example:
        >>> table_info = get_dbinfo_table(conn_info, "SAMPLE", 
        ...                              sql_filter="WHERE STATUS = 'ACTIVE'",
        ...                              max_records_per_table=10000)
    """
    print(f'Extracting data from Oracle table {table_name}...')
    
    # Extract connection parameters
    host = connection_info['host']
    port = connection_info['port']
    service_name = connection_info['service_name']
    username = connection_info['user']
    password = connection_info['password']
    owner = connection_info['owner']
    
    # Use provided engine or establish new connection
    engine_provided = engine is not None
    if not engine_provided:
        engine = connect_to_oracle(host, port, service_name, username, password)
        if engine is None:
            return None

    # Get Oracle version for compatibility (only if not already cached)
    oracle_version = get_oracle_version(engine)
    if oracle_version is None:
        print("Error retrieving Oracle version")
        return None

    is_version_12c_or_higher = oracle_version >= 12
    
    # Handle custom SQL query
    if sql_query is not None:
        extracted_table_name, extracted_fields_list = extract_query_info(sql_query)
        
        if extracted_table_name is None or extracted_fields_list is None:
            print("Failed to parse custom SQL query")
            return None
        
        table_name = extracted_table_name
        fields_list = extracted_fields_list

    table_dict = {}

    try:
        with engine.connect() as connection:
            # Step 1: Extract column metadata
            if sql_query is None:
                # Standard column query for entire table
                query = f"""
                    SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH
                    FROM SYS.ALL_TAB_COLS 
                    WHERE TABLE_NAME = '{table_name}' 
                    AND OWNER = '{owner}'
                    AND COLUMN_NAME NOT LIKE 'SYS\_%' ESCAPE '\\'
                    AND COLUMN_NAME NOT LIKE 'AUDIT%'
                    ORDER BY COLUMN_ID
                """
            else:
                # Query for specific fields from custom query
                fields_string = ', '.join(f"'{field}'" for field in fields_list)
                query = f"""
                    SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH 
                    FROM SYS.ALL_TAB_COLS 
                    WHERE TABLE_NAME = '{table_name}' 
                    AND OWNER = '{owner}'
                    AND COLUMN_NAME IN ({fields_string})
                    AND COLUMN_NAME NOT LIKE 'SYS\_%' ESCAPE '\\' 
                    ORDER BY COLUMN_ID
                """

            # Execute column metadata query
            fields_dict = {}
            try:
                df = pd.read_sql(query, connection)
                for _, row in df.iterrows():
                    column_name = row['column_name']
                    fields_dict[column_name] = {
                        "data_type": row['data_type'],
                        "data_length": row['data_length']
                    }
                    
                # Initialize table dictionary
                table_dict[table_name] = {
                    "name": table_name,
                    "order": "",
                    "field_owner": "",
                    "index": [],
                    "fields": fields_dict,
                    "data": pd.DataFrame()
                }
                
            except SQLAlchemyError as e:
                print(f"Error retrieving column metadata for {table_name}: {e}")
                return None

            # Step 2: Extract constraint information (for standard queries only)
            if sql_query is None:
                index_list = []
                
                # Choose appropriate constraint column based on Oracle version
                search_condition_column = "SEARCH_CONDITION_VC" if is_version_12c_or_higher else "SEARCH_CONDITION"

                # Query for check constraints to identify indexed fields
                constraint_query = f"""
                    SELECT {search_condition_column} 
                    FROM SYS.ALL_CONSTRAINTS 
                    WHERE TABLE_NAME = '{table_name}' 
                    AND OWNER = '{owner}'
                    AND CONSTRAINT_TYPE = 'C'
                """
                
                try:
                    df = pd.read_sql(constraint_query, connection)
                    for condition in df[search_condition_column.lower()]:
                        if condition:  # Check for non-null conditions
                            # Extract field name from constraint condition
                            match = re.search(r'"([^"]+)"', str(condition))
                            if match:
                                field_name = match.group(1)
                                index_list.append(field_name)
                    
                    # Sort and store index list
                    index_list = sorted(set(index_list))  # Remove duplicates and sort
                    table_dict[table_name]["index"] = index_list
                    
                except SQLAlchemyError as e:
                    print(f"Error retrieving constraints for {table_name}: {e}")
                    table_dict[table_name]["index"] = []

                # Step 3: Build and execute data query
                fields = ', '.join(fields_dict.keys())
                
                # Base query
                data_query = f"SELECT {fields} FROM {owner}.{table_name}"
                
                # Add optional filter
                if sql_filter is not None:
                    data_query = f"{data_query} {sql_filter}"
                    print(f'Applied filter: {sql_filter}')
                
                # Add ordering if constraints exist
                if index_list:
                    order_by = ', '.join(index_list)
                    data_query = f"{data_query} ORDER BY {order_by}"
                
                # Add row limit based on Oracle version
                if is_version_12c_or_higher:
                    data_query = f"{data_query} FETCH FIRST {max_records_per_table} ROWS ONLY"
                else:
                    data_query = f"SELECT * FROM ({data_query}) WHERE ROWNUM <= {max_records_per_table}"
            else:
                # Use custom query directly
                data_query = sql_query
                print(f'Using custom query: {sql_query}')

            # Step 4: Execute data extraction query
            try:
                print(f'Executing data query for {table_name}...')
                df = pd.read_sql(data_query, connection)
                table_dict[table_name]["data"] = df
                print(f'Retrieved {len(df)} records from {table_name}')
                
            except SQLAlchemyError as e:
                print(f"Error retrieving data from {table_name}: {e}")
                table_dict[table_name]["data"] = pd.DataFrame()

    except Exception as e:
        print(f"Unexpected error processing table {table_name}: {e}")
        return None
    
    return table_dict


def get_excluded_tables(connection, owner: str, additional_exclusions: List[str]) -> List[str]:
    """
    Get list of tables to exclude from data extraction.
    
    Combines user-specified exclusions with system-identified tables containing
    AUDIT, CONFIG, or _LOG patterns.

    Args:
        connection: Database connection object
        owner (str): Database schema owner
        additional_exclusions (List[str]): User-specified tables to exclude

    Returns:
        List[str]: Combined list of tables to exclude
    """
    excluded_tables = additional_exclusions.copy()
    
    # Query for system tables to exclude
    query = f"""
        SELECT TABLE_NAME 
        FROM SYS.ALL_TABLES 
        WHERE OWNER = '{owner}' 
        AND (TABLE_NAME LIKE '%AUDIT%' 
             OR TABLE_NAME LIKE '%CONFIG%' 
             OR TABLE_NAME LIKE '%\_LOG' ESCAPE '\\')
    """
    
    try:
        df = pd.read_sql(query, connection)
        excluded_tables.extend(df['table_name'].tolist())
        print(f"Found {len(df)} additional system tables to exclude")
        
    except SQLAlchemyError as e:
        print(f"Error retrieving system tables to exclude: {e}")
    
    return list(set(excluded_tables))  # Remove duplicates


def get_dbinfo_all_tables(connection_info: Dict[str, str], tables_to_exclude: List[str], 
                         total_records_limit: int = 500000, 
                         max_records_per_table: int = 50000) -> Optional[Dict[str, Any]]:
    """
    Retrieve metadata and data from all tables in an Oracle schema.

    Extracts information from all user tables while respecting exclusion lists
    and record limits to prevent excessive data retrieval.

    Args:
        connection_info (Dict[str, str]): Database connection parameters
        tables_to_exclude (List[str]): List of table names to exclude
        total_records_limit (int): Total record limit across all tables (default: 500000)
        max_records_per_table (int): Maximum records per individual table (default: 50000)

    Returns:
        Optional[Dict[str, Any]]: Dictionary of table information,
                                 None if extraction fails

    Notes:
        - Automatically excludes views and system tables
        - Stops processing when total_records_limit is reached
        - Removes empty tables from final result
        - Reuses database connection for all tables to improve performance
    """
    host = connection_info['host']
    port = connection_info['port']
    service_name = connection_info['service_name']
    username = connection_info['user']
    password = connection_info['password']
    owner = connection_info['owner']
    
    print(f'Extracting data from all tables in Oracle database {service_name}...')

    # Establish database connection
    engine = connect_to_oracle(host, port, service_name, username, password)
    if engine is None:
        return None

    all_tables = {}
    total_records_retrieved = 0

    try:
        with engine.connect() as connection:
            # Get comprehensive exclusion list
            excluded_tables = get_excluded_tables(connection, owner, tables_to_exclude)
            excluded_tables_str = ', '.join(f"'{table}'" for table in excluded_tables)
            
            print(f"Excluding {len(excluded_tables)} tables from extraction")

            # Query for all user tables excluding views and excluded tables
            query = f"""
                SELECT DISTINCT TABLE_NAME 
                FROM SYS.ALL_TAB_COLS 
                WHERE OWNER = '{owner}'
                AND TABLE_NAME NOT IN ({excluded_tables_str})
                AND TABLE_NAME NOT IN (
                    SELECT OBJECT_NAME 
                    FROM SYS.ALL_OBJECTS 
                    WHERE OWNER = '{owner}' 
                    AND OBJECT_TYPE = 'VIEW'
                )
                ORDER BY TABLE_NAME
            """
            
            try:
                df = pd.read_sql(query, connection)
                print(f"Found {len(df)} tables to process")
                
                # Process each table
                for idx, row in df.iterrows():
                    table_name = row['table_name']
                    print(f"Processing table {idx + 1}/{len(df)}: {table_name}")

                    # Extract table information
                    table_info = get_dbinfo_table(
                        connection_info, 
                        table_name, 
                        max_records_per_table=max_records_per_table,
                        engine=engine  # Pass the shared engine
                    )
                    
                    if table_info is not None and table_name in table_info:
                        all_tables[table_name] = table_info[table_name]
                        
                        # Track total records
                        num_rows = len(table_info[table_name]['data'])
                        total_records_retrieved += num_rows
                        
                        # Check if we've reached the total limit
                        if total_records_retrieved >= total_records_limit:
                            print(f"Total records limit of {total_records_limit:,} reached at table {table_name}")
                            print("Stopping further data retrieval")
                            break
                    else:
                        print(f"Failed to extract data from table {table_name}")

            except SQLAlchemyError as e:
                print(f"Error retrieving table list: {e}")
                return None

            print(f'Total records retrieved: {total_records_retrieved:,}')
            
            # Clean up empty tables if limit was reached
            if total_records_retrieved >= total_records_limit:
                empty_tables = [name for name, info in all_tables.items() 
                              if info["data"].empty]
                for table_name in empty_tables:
                    del all_tables[table_name]
                print(f"Removed {len(empty_tables)} empty tables from results")

    except Exception as e:
        print(f"Unexpected error during all tables extraction: {e}")
        return None

    return all_tables


def get_dbinfo_tables_with_clob(connection_info: Dict[str, str], tables_to_exclude: List[str], 
                               max_records_per_table: int = 50000) -> Optional[Dict[str, Any]]:
    """
    Retrieve information from tables containing CLOB (Character Large Object) fields.

    Identifies and extracts data from tables that have CLOB columns, which often
    contain large text data that requires special handling.

    Args:
        connection_info (Dict[str, str]): Database connection parameters
        tables_to_exclude (List[str]): List of table names to exclude  
        max_records_per_table (int): Maximum records per table (default: 50000)

    Returns:
        Optional[Dict[str, Any]]: Dictionary containing CLOB table information,
                                 None if extraction fails

    Notes:
        - Automatically excludes views and system tables
        - CLOB fields may contain large amounts of text data
        - Consider memory implications when setting max_records_per_table
    """
    host = connection_info['host']
    port = connection_info['port']
    service_name = connection_info['service_name']
    username = connection_info['user']
    password = connection_info['password']
    owner = connection_info['owner']
    
    print(f'Extracting data from tables with CLOB fields in Oracle database {service_name}...')

    # Establish database connection
    engine = connect_to_oracle(host, port, service_name, username, password)
    if engine is None:
        return None

    tables_with_clob = {}

    try:
        with engine.connect() as connection:
            # Get comprehensive exclusion list
            excluded_tables = get_excluded_tables(connection, owner, tables_to_exclude)
            excluded_tables_str = ', '.join(f"'{table}'" for table in excluded_tables)
            
            print(f"Excluding {len(excluded_tables)} tables from CLOB analysis")

            # Query for tables with CLOB fields
            query = f"""
                SELECT DISTINCT TABLE_NAME 
                FROM SYS.ALL_TAB_COLS 
                WHERE OWNER = '{owner}'
                AND TABLE_NAME NOT IN ({excluded_tables_str})
                AND TABLE_NAME NOT IN (
                    SELECT OBJECT_NAME 
                    FROM SYS.ALL_OBJECTS 
                    WHERE OWNER = '{owner}' 
                    AND OBJECT_TYPE = 'VIEW'
                )
                AND DATA_TYPE = 'CLOB'
                ORDER BY TABLE_NAME
            """
            
            try:
                df = pd.read_sql(query, connection)
                print(f"Found {len(df)} tables with CLOB fields")
                
                # Process each CLOB table
                for idx, row in df.iterrows():
                    table_name = row['table_name']
                    print(f"Processing CLOB table {idx + 1}/{len(df)}: {table_name}")

                    # Extract table information using shared engine
                    table_info = get_dbinfo_table(
                        connection_info, 
                        table_name, 
                        max_records_per_table=max_records_per_table,
                        engine=engine  # Pass the shared engine
                    )
                    
                    if table_info is not None and table_name in table_info:
                        tables_with_clob[table_name] = table_info[table_name]
                        record_count = len(table_info[table_name]['data'])
                        print(f"Retrieved {record_count} records from CLOB table {table_name}")
                    else:
                        print(f"Failed to extract data from CLOB table {table_name}")

            except SQLAlchemyError as e:
                print(f"Error retrieving CLOB tables: {e}")
                return None

    except Exception as e:
        print(f"Unexpected error during CLOB tables extraction: {e}")
        return None

    print(f"Successfully processed {len(tables_with_clob)} CLOB tables")
    return tables_with_clob


def get_dbinfo_list_of_tables(tables: List[str], connection_info: Dict[str, str], 
                             max_records_per_table: int = 50000) -> Dict[str, Any]:
    """
    Retrieve detailed information for a specific list of tables.

    Processes only the tables specified in the input list, useful for targeted
    data extraction or when working with a known subset of tables.

    Args:
        tables (List[str]): List of table names to process
        connection_info (Dict[str, str]): Database connection parameters
        max_records_per_table (int): Maximum records per table (default: 50000)

    Returns:
        Dict[str, Any]: Dictionary containing information for successfully processed tables

    Notes:
        - Reuses database connection for all tables to improve performance
        - Processes tables in the order specified in the input list
        - Continues processing remaining tables even if one fails

    Example:
        >>> tables_of_interest = ['SAMPLE', 'RESULT', 'TEST']
        >>> table_info = get_dbinfo_list_of_tables(tables_of_interest, conn_info)
    """
    service_name = connection_info['service_name']
    print(f"Extracting data from specified tables in Oracle database {service_name}...")
    print(f"Tables to process: {', '.join(tables)}")
    
    # Establish database connection ONCE for all specified tables
    host = connection_info['host']
    port = connection_info['port']
    username = connection_info['user']
    password = connection_info['password']
    
    engine = connect_to_oracle(host, port, service_name, username, password)
    if engine is None:
        print("Failed to establish database connection")
        return {}
    
    info_tables = {}
    
    try:
        # Process each specified table using the shared engine
        for idx, table_name in enumerate(tables, 1):
            print(f"Processing table {idx}/{len(tables)}: {table_name}")
            
            # Extract table information using shared engine
            table_info = get_dbinfo_table(
                connection_info, 
                table_name, 
                max_records_per_table=max_records_per_table,
                engine=engine  # Pass the shared engine
            )
            
            if table_info is not None and table_name in table_info:
                info_tables[table_name] = table_info[table_name]
                record_count = len(table_info[table_name]['data'])
                print(f"Successfully retrieved {record_count} records from {table_name}")
            else:
                print(f"Warning: Failed to extract data from table {table_name}")
                
    except Exception as e:
        print(f"Unexpected error during list tables extraction: {e}")
        return info_tables  # Return partial results if available
    
    print(f"Successfully processed {len(info_tables)} out of {len(tables)} specified tables")
    return info_tables