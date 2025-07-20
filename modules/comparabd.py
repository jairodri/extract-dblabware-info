"""
Oracle Database Schema Comparison Module

This module provides functionality to compare Oracle database schemas,
identify differences between them, and generate comprehensive reports.

Author: Database Analysis Team
Version: 2.0
"""

import os
from typing import Optional, Dict
import logging
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from typing import Dict, List, Any, Tuple
import datetime

from modules.getdbinfo import connect_to_oracle
from modules.dumpdbinfo import generate_unified_schema_report


# ==============================================================================
# LOGGING CONFIGURATION
# ==============================================================================

def setup_logging(docs_dir: Optional[str] = None):
    """
    Configure logging system for schema comparison operations.
    
    Sets up file-based logging with UTF-8 encoding to handle special characters
    and provides structured log output with timestamps and severity levels.
    
    Args:
        docs_dir (str, optional): Directory where to save the log file
    """
    try:
        # Determine log file path
        if docs_dir and os.path.exists(docs_dir):
            log_file = os.path.join(docs_dir, 'schema_comparison.log')
        else:
            log_file = 'schema_comparison.log'
            
        logging.basicConfig(
            filename=log_file,
            filemode='w',  # Overwrite log file on each run
            format='%(asctime)s - %(levelname)s - %(message)s',
            level=logging.INFO,
            encoding='utf-8',
            force=True  # Force reconfiguration if already configured
        )
        logging.info("Schema comparison process started")
        print(f"✅ Logging system initialized. Check '{log_file}' for details.")
        
    except Exception as e:
        print(f"Error configuring logging: {e}")
        # Fallback to console logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            force=True
        )


# ==============================================================================
# SCHEMA COLLECTION FUNCTIONS
# ==============================================================================

def collect_all_schemas(connections: Dict[str, Dict[str, str]], 
                       docs_dir: Optional[str] = None) -> Dict[str, pd.DataFrame]:
    """
    Connect to each database and retrieve its schema structure.
    
    Extracts table and column metadata from Oracle databases including
    column names, data types, lengths, precision, scale, and nullability.

    Args:
        connections (Dict[str, Dict[str, str]]): Dictionary where keys are connection names
                                                and values are connection parameter dictionaries
        docs_dir (str, optional): Directory for log files

    Returns:
        Dict[str, pd.DataFrame]: Dictionary where keys are connection names and 
                                values are DataFrames containing schema information
    """
    # Initialize logging with docs directory
    setup_logging(docs_dir)
    
    schema_repository = {}

    for name, conn_info in connections.items():
        logging.info(f"Collecting schema from: {name}...")
        
        try:
            # Extract connection parameters
            host = conn_info['host']
            port = conn_info['port']
            service_name = conn_info['service_name']
            username = conn_info['user']
            password = conn_info['password']
            owner = conn_info['owner']
            
            # Establish database connection
            engine = connect_to_oracle(host, port, service_name, username, password)
            if engine is None:
                logging.error(f"Failed to connect to {name}")
                continue
            
            # Query to extract schema metadata
            query = f"""
                SELECT 
                    TABLE_NAME, 
                    COLUMN_NAME, 
                    DATA_TYPE, 
                    DATA_LENGTH, 
                    DATA_PRECISION, 
                    DATA_SCALE, 
                    NULLABLE,
                    COLUMN_ID 
                FROM SYS.ALL_TAB_COLS
                WHERE OWNER = '{owner.upper()}' 
                AND COLUMN_NAME NOT LIKE 'SYS\_%' ESCAPE '\\'
                ORDER BY TABLE_NAME, COLUMN_ID
            """
            
            # Execute query and load results
            with engine.connect() as connection:
                schema_df = pd.read_sql_query(query, connection)
                
                # Store DataFrame in repository
                schema_repository[name] = schema_df
                
                logging.info(f"Schema from {name} collected successfully ({len(schema_df)} columns found)")
                print(f"Schema from {name} collected successfully ({len(schema_df)} columns found)")

        except SQLAlchemyError as e:
            logging.error(f"Database error while collecting schema from {name}: {e}")
            print(f"Error collecting schema from {name}: {e}")
            continue
            
        except Exception as e:
            logging.error(f"Unexpected error while collecting schema from {name}: {e}")
            print(f"Unexpected error collecting schema from {name}: {e}")
            continue

    logging.info(f"Schema collection completed. {len(schema_repository)} schemas collected successfully")
    return schema_repository


# ==============================================================================
# SCHEMA COMPARISON FUNCTIONS
# ==============================================================================

def _normalize_schemas(schemas_dict: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """
    Normalize column names to uppercase to avoid case-sensitivity issues.
    
    Args:
        schemas_dict (Dict[str, pd.DataFrame]): Dictionary of schema DataFrames
        
    Returns:
        Dict[str, pd.DataFrame]: Dictionary with normalized DataFrames
    """
    normalized = {}
    for schema_name, df in schemas_dict.items():
        if df is None or df.empty:
            logging.warning(f"Schema {schema_name} is empty or None, skipping normalization")
            continue
            
        df_copy = df.copy()
        df_copy.columns = df_copy.columns.str.upper()
        normalized[schema_name] = df_copy
        
    return normalized


def _create_universe(schemas_dict: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    """
    Create a consolidated universe of all tables and columns across schemas.
    
    This function builds a comprehensive map of all tables and their columns
    found in any of the provided schemas, which serves as the basis for comparison.

    Args:
        schemas_dict (Dict[str, pd.DataFrame]): Dictionary of normalized schema DataFrames

    Returns:
        Dict[str, Any]: Dictionary containing:
            - all_tables: Sorted list of all unique table names
            - table_columns: Dict mapping table names to their possible columns
            - schema_names: List of schema names for reference
    """
    all_tables = set()
    table_columns = {}
    
    # Collect all unique table names
    for df in schemas_dict.values():
        if 'TABLE_NAME' in df.columns and not df.empty:
            all_tables.update(df['TABLE_NAME'].unique())
    
    # For each table, collect all possible columns from all schemas
    for table in all_tables:
        all_columns = set()
        for df in schemas_dict.values():
            if 'TABLE_NAME' in df.columns and 'COLUMN_NAME' in df.columns:
                table_data = df[df['TABLE_NAME'] == table]
                if not table_data.empty:
                    all_columns.update(table_data['COLUMN_NAME'].unique())
        table_columns[table] = sorted(list(all_columns))
    
    return {
        'all_tables': sorted(list(all_tables)),
        'table_columns': table_columns,
        'schema_names': list(schemas_dict.keys())
    }


def _find_table_differences(universe: Dict, schemas_dict: Dict[str, pd.DataFrame]) -> List[Dict]:
    """
    Identify tables that don't exist in all schemas.
    
    Args:
        universe (Dict): Universe dictionary containing all tables and schemas
        schemas_dict (Dict[str, pd.DataFrame]): Dictionary of schema DataFrames
        
    Returns:
        List[Dict]: List of difference records for missing tables
    """
    print("Searching for table differences...")
    differences = []
    
    for table in universe['all_tables']:
        schema_status = {}
        missing_schemas = []
        
        # Check which schemas contain this table
        for schema_name, df in schemas_dict.items():
            if not df.empty and 'TABLE_NAME' in df.columns and table in df['TABLE_NAME'].unique():
                schema_status[schema_name] = 'EXISTS'
            else:
                schema_status[schema_name] = 'MISSING'
                missing_schemas.append(schema_name)
        
        # If table doesn't exist in all schemas, it's a difference
        if missing_schemas:
            row = {
                'TABLE_NAME': table,
                'COLUMN_NAME': '',
                'DIFFERENCE_TYPE': 'Table Missing'
            }
            
            # Add status for each schema
            for schema_name in universe['schema_names']:
                row[schema_name] = schema_status.get(schema_name, 'N/A')
            
            differences.append(row)
    
    return differences


def _find_column_differences(universe: Dict, schemas_dict: Dict[str, pd.DataFrame]) -> List[Dict]:
    """
    Identify columns that don't exist in all instances of tables across schemas.
    
    Args:
        universe (Dict): Universe dictionary containing all tables and columns
        schemas_dict (Dict[str, pd.DataFrame]): Dictionary of schema DataFrames
        
    Returns:
        List[Dict]: List of difference records for missing columns
    """
    print("Searching for column differences...")
    differences = []
    
    for table in universe['all_tables']:
        # Identify schemas that contain this table
        schemas_with_table = []
        for schema_name, df in schemas_dict.items():
            if not df.empty and 'TABLE_NAME' in df.columns and table in df['TABLE_NAME'].unique():
                schemas_with_table.append(schema_name)
        
        # Need at least 2 schemas with the table to compare
        if len(schemas_with_table) <= 1:
            continue
        
        # Check each column for this table
        for column in universe['table_columns'][table]:
            schema_status = {}
            missing_schemas = []
            
            for schema_name in schemas_with_table:
                df = schemas_dict[schema_name]
                table_data = df[df['TABLE_NAME'] == table]
                
                if not table_data.empty and 'COLUMN_NAME' in df.columns and column in table_data['COLUMN_NAME'].unique():
                    schema_status[schema_name] = 'EXISTS'
                else:
                    schema_status[schema_name] = 'MISSING'
                    missing_schemas.append(schema_name)
            
            # If column is missing from some schemas that have the table
            if missing_schemas and len(missing_schemas) < len(schemas_with_table):
                row = {
                    'TABLE_NAME': table,
                    'COLUMN_NAME': column,
                    'DIFFERENCE_TYPE': 'Column Missing'
                }
                
                # Add status for each schema
                for schema_name in universe['schema_names']:
                    if schema_name in schema_status:
                        row[schema_name] = schema_status[schema_name]
                    else:
                        row[schema_name] = 'N/A'  # Table doesn't exist in this schema
                
                differences.append(row)
    
    return differences


def _find_attribute_differences(universe: Dict, schemas_dict: Dict[str, pd.DataFrame]) -> List[Dict]:
    """
    Identify differences in column attributes across schemas.
    
    Compares data types, lengths, precision, scale, and nullability of columns
    that exist in multiple schemas.

    Args:
        universe (Dict): Universe dictionary containing all tables and columns
        schemas_dict (Dict[str, pd.DataFrame]): Dictionary of schema DataFrames
        
    Returns:
        List[Dict]: List of difference records for attribute mismatches
    """
    print("Searching for column attribute differences...")
    differences = []
    attributes = ['DATA_TYPE', 'DATA_LENGTH', 'DATA_PRECISION', 'DATA_SCALE', 'NULLABLE']
    
    for table in universe['all_tables']:
        for column in universe['table_columns'][table]:
            # Collect column information from all schemas
            column_info = {}
            schemas_with_column = []
            
            for schema_name, df in schemas_dict.items():
                if df.empty or 'TABLE_NAME' not in df.columns or 'COLUMN_NAME' not in df.columns:
                    continue
                    
                column_data = df[(df['TABLE_NAME'] == table) & (df['COLUMN_NAME'] == column)]
                if not column_data.empty:
                    schemas_with_column.append(schema_name)
                    column_info[schema_name] = column_data.iloc[0].to_dict()
            
            # Only compare if column exists in at least 2 schemas
            if len(schemas_with_column) < 2:
                continue
            
            # Compare each attribute
            for attr in attributes:
                if attr not in df.columns:  # Skip if attribute column doesn't exist
                    continue
                    
                attr_values = {}
                unique_values = set()
                
                # Collect attribute values
                for schema_name in schemas_with_column:
                    value = column_info[schema_name].get(attr)
                    if pd.isna(value):
                        value = None
                    attr_values[schema_name] = value
                    unique_values.add(value)
                
                # If there's more than one unique value, it's a difference
                if len(unique_values) > 1:
                    row = {
                        'TABLE_NAME': table,
                        'COLUMN_NAME': column,
                        'DIFFERENCE_TYPE': f'{attr} Different'
                    }
                    
                    # Add value for each schema
                    for schema_name in universe['schema_names']:
                        if schema_name in attr_values:
                            value = attr_values[schema_name]
                            row[schema_name] = str(value) if value is not None else 'NULL'
                        else:
                            row[schema_name] = 'N/A'
                    
                    differences.append(row)
    
    return differences


def compare_all_schemas(schemas_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Compare all schemas and generate a consolidated differences table.
    
    This is the main comparison function that orchestrates the entire schema
    comparison process and returns a comprehensive DataFrame with all differences.

    Args:
        schemas_dict (Dict[str, pd.DataFrame]): Dictionary where keys are schema names
                                               and values are DataFrames with schema structure

    Returns:
        pd.DataFrame: DataFrame containing all differences found between schemas.
                     Columns include TABLE_NAME, COLUMN_NAME, DIFFERENCE_TYPE,
                     and one column per schema showing the status/value.

    Example:
        >>> schemas = {'DB1': df1, 'DB2': df2, 'DB3': df3}
        >>> differences = compare_all_schemas(schemas)
        >>> print(f"Found {len(differences)} differences")
    """
    logging.info(f"Starting comparison of {len(schemas_dict)} schemas")
    print(f"Comparing {len(schemas_dict)} schemas...")
    
    # Validate input
    if len(schemas_dict) < 2:
        logging.warning("At least 2 schemas are required for comparison")
        print("Warning: At least 2 schemas are required for comparison")
        return pd.DataFrame()
    
    # Normalize column names to avoid case sensitivity issues
    normalized_schemas = _normalize_schemas(schemas_dict)
    
    if not normalized_schemas:
        logging.error("No valid schemas found after normalization")
        print("Error: No valid schemas found")
        return pd.DataFrame()
    
    # Create universe of all tables and columns
    universe = _create_universe(normalized_schemas)
    
    logging.info(f"Universe created with {len(universe['all_tables'])} unique tables")
    print(f"Universe created with {len(universe['all_tables'])} unique tables")
    
    # Find all types of differences
    all_differences = []
    
    # 1. Table differences
    table_diffs = _find_table_differences(universe, normalized_schemas)
    all_differences.extend(table_diffs)
    logging.info(f"Found {len(table_diffs)} table differences")
    
    # 2. Column differences
    column_diffs = _find_column_differences(universe, normalized_schemas)
    all_differences.extend(column_diffs)
    logging.info(f"Found {len(column_diffs)} column differences")
    
    # 3. Attribute differences
    attr_diffs = _find_attribute_differences(universe, normalized_schemas)
    all_differences.extend(attr_diffs)
    logging.info(f"Found {len(attr_diffs)} attribute differences")
    
    # Create final DataFrame
    if all_differences:
        diff_df = pd.DataFrame(all_differences)
        # Sort by table, column, and difference type for better readability
        diff_df = diff_df.sort_values(['TABLE_NAME', 'COLUMN_NAME', 'DIFFERENCE_TYPE'])
        
        logging.info(f"Total differences found: {len(diff_df)}")
        print(f"Total differences found: {len(diff_df)}")
        
        return diff_df
    else:
        # If no differences, return empty DataFrame with correct structure
        columns = ['TABLE_NAME', 'COLUMN_NAME', 'DIFFERENCE_TYPE'] + list(schemas_dict.keys())
        empty_df = pd.DataFrame(columns=columns)
        
        logging.info("No differences found between schemas")
        print("No differences found between schemas")
        
        return empty_df


# ==============================================================================
# REPORT GENERATION FUNCTIONS
# ==============================================================================

def analyze_and_report_schemas(schemas_dict: Dict[str, pd.DataFrame], 
                             output_filename: str = "complete_schema_report.xlsx") -> Tuple[Dict[str, pd.DataFrame], pd.DataFrame]:
    """
    Perform complete schema analysis and generate unified report.
    
    This is the main entry point function that orchestrates the entire schema
    comparison process and generates a comprehensive Excel report.

    Args:
        schemas_dict (Dict[str, pd.DataFrame]): Dictionary with schemas to analyze
        output_filename (str): Name of the Excel output file

    Returns:
        Tuple[Dict[str, pd.DataFrame], pd.DataFrame]: Tuple containing:
            - Original schemas dictionary
            - DataFrame with differences found

    Example:
        >>> schemas = collect_all_schemas(connections)
        >>> original_data, differences = analyze_and_report_schemas(
        ...     schemas, "production_schema_comparison.xlsx"
        ... )
    """
    logging.info(f"Starting complete analysis of {len(schemas_dict)} schemas")
    print(f"Starting complete analysis of {len(schemas_dict)} schemas...")
    
    # Validate input
    if not schemas_dict:
        logging.error("No schemas provided for analysis")
        print("Error: No schemas provided for analysis")
        return {}, pd.DataFrame()
    
    # Generate differences analysis
    diff_df = compare_all_schemas(schemas_dict)
    
    # Generate unified report
    try:
        generate_unified_schema_report(schemas_dict, diff_df, output_filename)
        logging.info(f"Unified report generated successfully: {output_filename}")
        print(f"Unified report generated successfully: {output_filename}")
        
    except Exception as e:
        logging.error(f"Error generating unified report: {e}")
        print(f"Error generating unified report: {e}")
    
    # Log summary statistics
    if not diff_df.empty:
        diff_summary = diff_df['DIFFERENCE_TYPE'].value_counts().to_dict()
        logging.info(f"Differences summary: {diff_summary}")
        
        print("Differences summary:")
        for diff_type, count in diff_summary.items():
            print(f"  - {diff_type}: {count}")
    else:
        logging.info("All schemas are identical")
        print("All schemas are identical!")
    
    return schemas_dict, diff_df


# ==============================================================================
# UTILITY FUNCTIONS
# ==============================================================================

def get_schema_statistics(schemas_dict: Dict[str, pd.DataFrame]) -> Dict[str, Dict[str, int]]:
    """
    Generate statistics for each schema in the dictionary.
    
    Args:
        schemas_dict (Dict[str, pd.DataFrame]): Dictionary of schema DataFrames
        
    Returns:
        Dict[str, Dict[str, int]]: Statistics for each schema including
                                  table count, column count, and data types
    """
    stats = {}
    
    for schema_name, df in schemas_dict.items():
        if df.empty:
            stats[schema_name] = {
                'total_tables': 0,
                'total_columns': 0,
                'unique_data_types': 0
            }
            continue
            
        schema_stats = {
            'total_tables': df['TABLE_NAME'].nunique() if 'TABLE_NAME' in df.columns else 0,
            'total_columns': len(df),
            'unique_data_types': df['DATA_TYPE'].nunique() if 'DATA_TYPE' in df.columns else 0
        }
        
        stats[schema_name] = schema_stats
        
    return stats


def validate_schemas_dict(schemas_dict: Dict[str, pd.DataFrame]) -> List[str]:
    """
    Validate the schemas dictionary and return list of issues found.
    
    Args:
        schemas_dict (Dict[str, pd.DataFrame]): Dictionary to validate
        
    Returns:
        List[str]: List of validation issues found
    """
    issues = []
    
    if not schemas_dict:
        issues.append("Empty schemas dictionary provided")
        return issues
    
    required_columns = ['TABLE_NAME', 'COLUMN_NAME', 'DATA_TYPE']
    
    for schema_name, df in schemas_dict.items():
        if df is None:
            issues.append(f"Schema '{schema_name}' is None")
            continue
            
        if df.empty:
            issues.append(f"Schema '{schema_name}' is empty")
            continue
            
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            issues.append(f"Schema '{schema_name}' missing required columns: {missing_cols}")
    
    return issues