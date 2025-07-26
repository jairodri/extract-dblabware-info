"""
Database Information Extraction and Schema Comparison Tool

This module provides a command-line interface for extracting database metadata,
comparing schemas, and generating reports from Oracle databases.

Author: Database Analysis Team
Version: 2.0
"""

import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional
from dotenv import load_dotenv
import traceback

# Import custom modules
from modules.getdbinfo import (
    get_dbinfo_metadata, 
    get_dbinfo_table, 
    get_dbinfo_all_tables, 
    get_dbinfo_tables_with_clob, 
    get_dbinfo_list_of_tables
)
from modules.dumpdbinfo import dump_dbinfo_to_csv, dump_dbinfo_to_excel
from modules.comparefiles import (
    generate_excel_from_diffs, 
    compare_excel_dbinfo_files, 
    get_folder_files_info, 
    compare_file_info
)
from modules.comparabd import collect_all_schemas, analyze_and_report_schemas
from modules.compare_events import collect_all_events, analyze_and_report_events
from modules.logparser import HTTPLogParser 


def load_grouped_vars_by_pattern() -> Dict[str, Dict[str, str]]:
    """
    Load all environment variables that match the database connection pattern.
    
    Pattern: <ENVIRONMENT>_<COMPLEX>_<VERSION>_<VARIABLE>
    Where:
        - ENVIRONMENT: DES, PRE, PRO
        - COMPLEX: 2-3 letter complex code (COR, SIN, CAR, etc.)
        - VERSION: V6, V7, V8
        - VARIABLE: NAME, HOST, PORT, SERVICE_NAME, USER, PASSWORD, OWNER

    Returns:
        Dict[str, Dict[str, str]]: Dictionary where keys are database names and 
                                   values are connection parameters
    """
    # Define regex pattern to match database connection variables
    pattern = re.compile(
        r'^(DES|PRE|PRO)_([A-Z]{2,3})_(V[6-8])_(NAME|HOST|PORT|SERVICE_NAME|USER|PASSWORD|OWNER)$'
    )
    grouped_vars = {}

    # Parse environment variables
    for key, value in os.environ.items():
        match = pattern.match(key)
        if match:
            environment, complex_code, version, variable = match.groups()
            group_key = f"{environment}_{complex_code}_{version}"

            # Initialize group dictionary if not present
            if group_key not in grouped_vars:
                grouped_vars[group_key] = {}

            # Add variable to the group
            grouped_vars[group_key][variable.lower()] = value

    # Transform to use NAME as the main key
    result = {}
    for group_key, vars_dict in grouped_vars.items():
        if 'name' in vars_dict:
            # Use 'name' as the key and exclude it from the subdictionary
            result[vars_dict.pop('name')] = vars_dict

    return result


def load_grouped_vars(prefix: str) -> Dict[str, str]:
    """
    Load all environment variables starting with the given prefix.
    
    Args:
        prefix (str): The prefix to filter environment variables
        
    Returns:
        Dict[str, str]: Dictionary of environment variables with prefix removed
    """
    grouped_vars = {}
    for key, value in os.environ.items():
        if key.startswith(prefix):
            # Remove prefix and convert to lowercase
            grouped_vars[key[len(prefix):].lower()] = value
    return grouped_vars


def initialize_environment() -> Dict[str, any]:
    """
    Initialize environment variables and configuration from .env file.
    
    Returns:
        Dict[str, any]: Configuration dictionary with all settings
    """
    # Add base directory to Python path
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))

    # Load environment variables from .env file
    env_path = Path(__file__).parent / "resources" / ".env"
    load_dotenv(env_path)

    # Create docs directory if it doesn't exist
    docs_dir = os.getenv('DOCS_OUTPUT_DIR', 'docs')
    docs_path = Path(__file__).parent / docs_dir
    docs_path.mkdir(exist_ok=True)

    # Parse configuration values
    config = {
        'output_dir_data': os.getenv('OUTPUT_DIR_DATA'),
        'docs_output_dir': str(docs_path),  # Full path to docs directory
        'table_name': os.getenv('TABLE_NAME'),
        'sql_query': os.getenv('SQL_QUERY') or None,
        'sql_filter': os.getenv('SQL_FILTER') or None,
        'max_records_per_table': int(os.getenv('MAX_RECORDS_PER_TABLE', 1000)),
        'total_records_limit': int(os.getenv('TOTAL_RECORDS_LIMIT', 300000)),
        'csv_separator': os.getenv('CSV_SEPARATOR', '|'),
        'events_comparison_report_file': os.getenv('EVENTS_COMPARISON_REPORT_FILE'),

        # Log parser configuration
        'log_parser_input_file': os.getenv('LOG_PARSER_INPUT_FILE'),
        'log_parser_output_file': os.getenv('LOG_PARSER_OUTPUT_FILE'),
        
        # Parse comma-separated lists
        'tables_with_clob_to_exclude': [
            table.strip() for table in os.getenv('TABLES_WITH_CLOB_TO_EXCLUDE', '').split(',')
            if table.strip()
        ],
        'tables_to_exclude': [
            table.strip() for table in os.getenv('TABLES_TO_EXCLUDE', '').split(',')
            if table.strip()
        ],
        'table_list': [
            table.strip() for table in os.getenv('TABLE_LIST', '').split(',')
            if table.strip()
        ],
        
        # Comparison folders and files
        'folder_in1': os.getenv('COMPARE_FOLDER_IN1'),
        'folder_in2': os.getenv('COMPARE_FOLDER_IN2'),
        'folder_out': os.getenv('COMPARE_FOLDER_OUT'),
        'server_folder_in1': os.getenv('SERVER_FOLDER_IN1'),
        'server_folder_in2': os.getenv('SERVER_FOLDER_IN2'),
        'server_compare_out': os.getenv('COMPARE_SERVER_OUT'),
        'file_excel1': os.getenv('COMPARE_EXCEL_FILE_1'),
        'file_excel2': os.getenv('COMPARE_EXCEL_FILE_2'),
        'file_excel_out': os.getenv('COMPARE_EXCEL_FILE_OUT'),
       
        # Schema comparison configuration
        'schema_comparison_report_file': os.getenv('SCHEMA_COMPARISON_REPORT_FILE'),
        'schema_comparison_include_patterns': [
            pattern.strip() for pattern in os.getenv('SCHEMA_COMPARISON_INCLUDE_PATTERNS', '').split(',')
            if pattern.strip()
        ],
        'schema_comparison_exclude_patterns': [
            pattern.strip() for pattern in os.getenv('SCHEMA_COMPARISON_EXCLUDE_PATTERNS', '').split(',')
            if pattern.strip()
        ],
        'schema_comparison_regex_pattern': os.getenv('SCHEMA_COMPARISON_REGEX_PATTERN') or None,
    }
    
    return config


def display_main_menu() -> None:
    """Display the main menu options to the user."""
    print("\n" + "="*60)
    print("DATABASE ANALYSIS AND COMPARISON TOOL")
    print("="*60)
    print("Select an option to run:")
    print("1 - Extract catalog metadata and dump to csv/excel")
    print("2 - Extract specific table data and dump to csv/excel")
    print("3 - Extract all tables data and dump to csv/excel")
    print("4 - Extract tables with CLOB fields and dump to csv/excel")
    print("5 - Extract specific list of tables and dump to csv/excel")
    print("6 - Compare text files and generate excel with differences")
    print("7 - Compare excel files and generate excel with differences")
    print("8 - Compare files in folders and generate excel with differences")
    print("9 - Compare database schemas and generate comprehensive report")
    print("10 - Compare events between databases and generate comprehensive report") 
    print("11 - Parse HTTP status codes from web service logs") 
    print("0 - Exit")
    print("="*60)


def get_user_option() -> int:
    """
    Get and validate user menu option.
    
    Returns:
        int: Selected menu option
        
    Raises:
        ValueError: If invalid option is entered
    """
    try:
        option = int(input("\nEnter the option number: "))
        if option < 0 or option > 11:
            raise ValueError("Option must be between 0 and 11")
        return option
    except ValueError as e:
        print(f"Invalid option: {e}")
        return -1

def execute_log_parsing(config: Dict[str, any]) -> None:
    """
    Execute HTTP log parsing to extract status codes and generate CSV report.
    
    Args:
        config (Dict[str, any]): Configuration settings including file paths
    """
    print("Starting HTTP log parsing...")
    
    # Get input log file path
    default_input = config['log_parser_input_file']
    log_file_path = input(f"\nEnter log file path (or press Enter for default: {default_input}): ").strip()
    
    if not log_file_path:
        log_file_path = default_input
    
    # Check if file exists
    if not os.path.exists(log_file_path):
        print(f"Error: Log file not found: {log_file_path}")
        return
    
    # Get output CSV file path (save in docs directory)
    default_output = os.path.join(config['docs_output_dir'], config['log_parser_output_file'])
    output_file_path = input(f"\nEnter output CSV file path (or press Enter for default: {default_output}): ").strip()
    
    if not output_file_path:
        output_file_path = default_output
    
    try:
        # Create parser instance
        log_parser = HTTPLogParser(log_file_path)
        
        # Parse the log file
        print(f"Parsing log file: {log_file_path}")
        entries = log_parser.parse_log_file()
        
        if not entries:
            print("No HTTP status entries found in the log file.")
            return
        
        # Generate CSV report
        print(f"Generating CSV report: {output_file_path}")
        csv_path = log_parser.generate_csv_report(output_file_path)
        
        if csv_path:
            print("HTTP log parsing completed successfully!")
            print(f"CSV report saved to: {csv_path}")
            
            # Display summary
            print("\n" + "="*50)
            print("HTTP LOG PARSING SUMMARY:")
            print(f"Input file: {log_file_path}")
            print(f"Output file: {csv_path}")
            print(f"Total entries processed: {len(entries)}")
            
            # Show status code summary
            log_parser.print_summary()
            print("="*50)
        else:
            print("Error: Failed to generate CSV report.")
            
    except FileNotFoundError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"Error during log parsing: {e}")
        traceback.print_exc()

def select_database_connection(database_connections: Dict[str, Dict[str, str]]) -> Optional[Dict[str, str]]:
    """
    Display available database connections and let user select one.
    
    Args:
        database_connections (Dict[str, Dict[str, str]]): Available database connections
        
    Returns:
        Optional[Dict[str, str]]: Selected connection info or None if invalid selection
    """
    if not database_connections:
        print("No database connections available.")
        return None
    
    print("\nAvailable database connections:")
    connection_keys = list(database_connections.keys())
    
    for idx, key in enumerate(connection_keys, start=1):
        print(f"{idx:2d}. {key}")

    # Get user selection
    while True:
        try:
            selected_index = int(input(f"\nSelect a database connection (1-{len(connection_keys)}): "))
            if 1 <= selected_index <= len(connection_keys):
                selected_connection = connection_keys[selected_index - 1]
                connection_info = database_connections[selected_connection]
                print(f'Selected connection: {selected_connection}')
                return connection_info
            else:
                print(f"Invalid selection. Please select a number between 1 and {len(connection_keys)}.")
        except ValueError:
            print("Invalid input. Please enter a number.")


def get_output_format() -> Optional[str]:
    """
    Get and validate output format from user.
    
    Returns:
        Optional[str]: Output format ('csv' or 'excel') or None if invalid
    """
    output_format = input("\nEnter the output format (csv/excel): ").strip().lower()
    if output_format not in ['csv', 'excel']:
        print("Invalid output format. Please enter 'csv' or 'excel'.")
        return None
    return output_format


def execute_catalog_extraction(connection_info: Dict[str, str], output_format: str, config: Dict[str, any]) -> None:
    """
    Execute catalog metadata extraction (Option 1).
    
    Args:
        connection_info (Dict[str, str]): Database connection parameters
        output_format (str): Output format ('csv' or 'excel')
        config (Dict[str, any]): Configuration settings
    """
    print("Extracting catalog metadata...")
    db_info_catalog = get_dbinfo_metadata(connection_info)
    folder_name = f"{connection_info['service_name']}_catalog"
    
    if output_format == 'excel':
        dump_dbinfo_to_excel(folder_name, db_info_catalog, config['output_dir_data'])
    else:
        dump_dbinfo_to_csv(folder_name, db_info_catalog, config['output_dir_data'], 
                          sep=config['csv_separator'])
    print("Catalog extraction completed successfully.")


def execute_table_extraction(connection_info: Dict[str, str], output_format: str, config: Dict[str, any]) -> None:
    """
    Execute specific table data extraction (Option 2).
    
    Args:
        connection_info (Dict[str, str]): Database connection parameters
        output_format (str): Output format ('csv' or 'excel')
        config (Dict[str, any]): Configuration settings
    """
    print(f"Extracting data from table: {config['table_name']}")
    db_info_table = get_dbinfo_table(
        connection_info, 
        config['table_name'], 
        sql_filter=config['sql_filter'],
        sql_query=config['sql_query'], 
        max_records_per_table=config['max_records_per_table']
    )
    folder_name = f"{connection_info['service_name']}_{config['table_name'].lower()}"
    
    if output_format == 'excel':
        dump_dbinfo_to_excel(folder_name, db_info_table, config['output_dir_data'], 
                            include_record_count=True, 
                            max_records_per_table=config['max_records_per_table'], 
                            file_name=config['table_name'])
    else:
        dump_dbinfo_to_csv(folder_name, db_info_table, config['output_dir_data'], 
                          sep=config['csv_separator'], suffix=None)
    print("Table extraction completed successfully.")


def execute_all_tables_extraction(connection_info: Dict[str, str], output_format: str, config: Dict[str, any]) -> None:
    """
    Execute all tables data extraction (Option 3).
    
    Args:
        connection_info (Dict[str, str]): Database connection parameters
        output_format (str): Output format ('csv' or 'excel')
        config (Dict[str, any]): Configuration settings
    """
    print("Extracting data from all tables...")
    db_info_all_tables = get_dbinfo_all_tables(
        connection_info, 
        config['tables_to_exclude'], 
        total_records_limit=config['total_records_limit'],
        max_records_per_table=config['max_records_per_table']
    )
    folder_name = f"{connection_info['service_name']}_all_tables"
    
    if output_format == 'excel':
        dump_dbinfo_to_excel(folder_name, db_info_all_tables, config['output_dir_data'], 
                            include_record_count=True, 
                            max_records_per_table=config['max_records_per_table'])
    else:
        dump_dbinfo_to_csv(folder_name, db_info_all_tables, config['output_dir_data'], 
                          sep=config['csv_separator'])
    print("All tables extraction completed successfully.")


def execute_clob_tables_extraction(connection_info: Dict[str, str], output_format: str, config: Dict[str, any]) -> None:
    """
    Execute CLOB tables data extraction (Option 4).
    
    Args:
        connection_info (Dict[str, str]): Database connection parameters
        output_format (str): Output format ('csv' or 'excel')
        config (Dict[str, any]): Configuration settings
    """
    print("Extracting data from tables with CLOB fields...")
    tables_with_clob = get_dbinfo_tables_with_clob(connection_info, config['tables_with_clob_to_exclude'])
    folder_name = f"{connection_info['service_name']}_clobs"
    
    if output_format == 'excel':
        dump_dbinfo_to_excel(folder_name, tables_with_clob, config['output_dir_data'], 
                            include_record_count=True, 
                            max_records_per_table=config['max_records_per_table'])
    else:
        dump_dbinfo_to_csv(folder_name, tables_with_clob, config['output_dir_data'], 
                          sep=config['csv_separator'])
    print("CLOB tables extraction completed successfully.")


def execute_list_tables_extraction(connection_info: Dict[str, str], output_format: str, config: Dict[str, any]) -> None:
    """
    Execute specific list of tables data extraction (Option 5).
    
    Args:
        connection_info (Dict[str, str]): Database connection parameters
        output_format (str): Output format ('csv' or 'excel')
        config (Dict[str, any]): Configuration settings
    """
    print(f"Extracting data from specified tables: {config['table_list']}")
    info_tables = get_dbinfo_list_of_tables(config['table_list'], connection_info)
    folder_name = f"{connection_info['service_name']}_list_tables"
    
    if output_format == 'excel':
        dump_dbinfo_to_excel(folder_name, info_tables, config['output_dir_data'], 
                            include_record_count=True, 
                            max_records_per_table=config['max_records_per_table'])
    else:
        dump_dbinfo_to_csv(folder_name, info_tables, config['output_dir_data'], 
                          sep=config['csv_separator'])
    print("List tables extraction completed successfully.")

def filter_database_connections(database_connections: Dict[str, Dict[str, str]], 
                               include_patterns: List[str], 
                               exclude_patterns: List[str],
                               regex_pattern: Optional[str] = None) -> Dict[str, Dict[str, str]]:
    """
    Filter database connections based on configurable patterns.
    
    Args:
        database_connections (Dict[str, Dict[str, str]]): All available database connections
        include_patterns (List[str]): Patterns that connection names must contain (ALL must match)
        exclude_patterns (List[str]): Patterns that connection names must NOT contain (ANY match excludes)
        regex_pattern (str, optional): Advanced regex pattern for complex filtering
        
    Returns:
        Dict[str, Dict[str, str]]: Filtered database connections
        
    Examples:
        >>> # Include only production V8 connections
        >>> filtered = filter_database_connections(
        ...     connections, 
        ...     include_patterns=['PRO_', '_V8'], 
        ...     exclude_patterns=['_ATD_', '_DT_']
        ... )
        
        >>> # Use regex for complex patterns
        >>> filtered = filter_database_connections(
        ...     connections, 
        ...     include_patterns=[], 
        ...     exclude_patterns=[],
        ...     regex_pattern=r'^PRO_[A-Z]{2,3}_V8(?!.*(_ATD_|_DT_))'
        ... )
    """
    if not database_connections:
        return {}
    
    filtered_connections = {}
    
    for name, conn_info in database_connections.items():
        # If regex pattern is provided, use it exclusively
        if regex_pattern:
            try:
                if re.match(regex_pattern, name):
                    filtered_connections[name] = conn_info
                continue
            except re.error as e:
                print(f"Warning: Invalid regex pattern '{regex_pattern}': {e}")
                print("Falling back to include/exclude pattern filtering")
                # Continue with pattern-based filtering if regex fails
        
        # Pattern-based filtering
        include_match = True
        exclude_match = False
        
        # Check include patterns (ALL must match)
        if include_patterns:
            include_match = all(pattern in name for pattern in include_patterns)
        
        # Check exclude patterns (ANY match excludes)
        if exclude_patterns:
            exclude_match = any(pattern in name for pattern in exclude_patterns)
        
        # Include connection if it matches include patterns and doesn't match exclude patterns
        if include_match and not exclude_match:
            filtered_connections[name] = conn_info
    
    return filtered_connections

def execute_schema_comparison(config: Dict[str, any]) -> None:
    """
    Execute comprehensive schema comparison with configurable connection filtering.
    
    Args:
        config (Dict[str, any]): Configuration settings including filter patterns
    """
    print("Starting comprehensive schema comparison...")
    
    # Load available database connections
    database_connections = load_grouped_vars_by_pattern()
    
    if not database_connections:
        print("No database connections found.")
        return
    
    print(f"Found {len(database_connections)} total database connections")
    
    # Apply configurable filters to database connections
    filtered_connections = filter_database_connections(
        database_connections,
        include_patterns=config['schema_comparison_include_patterns'],
        exclude_patterns=config['schema_comparison_exclude_patterns'],
        regex_pattern=config['schema_comparison_regex_pattern']
    )
    
    if not filtered_connections:
        print("No database connections match the specified filter criteria.")
        print("Filter configuration:")
        if config['schema_comparison_include_patterns']:
            print(f"  - Include patterns: {config['schema_comparison_include_patterns']}")
        if config['schema_comparison_exclude_patterns']:
            print(f"  - Exclude patterns: {config['schema_comparison_exclude_patterns']}")
        if config['schema_comparison_regex_pattern']:
            print(f"  - Regex pattern: {config['schema_comparison_regex_pattern']}")
        print("\nAvailable connections:")
        for name in database_connections.keys():
            print(f"  - {name}")
        return
    
    print(f"After filtering: {len(filtered_connections)} connections selected for comparison")
    print("Selected database connections:")
    for idx, conn_name in enumerate(filtered_connections.keys(), 1):
        print(f"  {idx}. {conn_name}")

    # Collect schemas from filtered databases (pass docs directory for logging)
    print("\nCollecting schema information from selected databases...")
    all_schemas = collect_all_schemas(filtered_connections, config['docs_output_dir'])

    
    if len(all_schemas) < 2:
        print("At least 2 schemas are needed for comparison.")
        print("Try adjusting your filter criteria to include more connections.")
        return
    
    print(f"Successfully collected {len(all_schemas)} schemas.")
    
    # Generate comprehensive unified report using configured filename in docs directory
    report_filename = config['schema_comparison_report_file']
    full_report_path = os.path.join(config['docs_output_dir'], report_filename)
    
    try:
        schemas_data, differences = analyze_and_report_schemas(all_schemas, full_report_path)
        print("Schema comparison completed successfully.")
        
        # Display summary of applied filters
        print("\n" + "="*50)
        print("FILTER SUMMARY:")
        if config['schema_comparison_include_patterns']:
            print(f"Include patterns: {', '.join(config['schema_comparison_include_patterns'])}")
        if config['schema_comparison_exclude_patterns']:
            print(f"Exclude patterns: {', '.join(config['schema_comparison_exclude_patterns'])}")
        if config['schema_comparison_regex_pattern']:
            print(f"Regex pattern: {config['schema_comparison_regex_pattern']}")
        print(f"Connections analyzed: {len(all_schemas)}")
        print(f"Report generated: {report_filename}")
        print("="*50)
        
    except Exception as e:
        print(f"Error during schema comparison: {e}")
        traceback.print_exc()

def execute_events_comparison(config: Dict[str, any]) -> None:
    """
    Execute comprehensive events comparison with configurable connection filtering.
    
    Args:
        config (Dict[str, any]): Configuration settings including filter patterns
    """
    print("Starting comprehensive events comparison...")
    
    # Load available database connections
    database_connections = load_grouped_vars_by_pattern()
    
    if not database_connections:
        print("No database connections found.")
        return
    
    print(f"Found {len(database_connections)} total database connections")
    
    # Apply configurable filters to database connections
    filtered_connections = filter_database_connections(
        database_connections,
        include_patterns=config['schema_comparison_include_patterns'],
        exclude_patterns=config['schema_comparison_exclude_patterns'],
        regex_pattern=config['schema_comparison_regex_pattern']
    )
    
    if not filtered_connections:
        print("No database connections match the specified filter criteria.")
        print("Filter configuration:")
        if config['schema_comparison_include_patterns']:
            print(f"  - Include patterns: {config['schema_comparison_include_patterns']}")
        if config['schema_comparison_exclude_patterns']:
            print(f"  - Exclude patterns: {config['schema_comparison_exclude_patterns']}")
        if config['schema_comparison_regex_pattern']:
            print(f"  - Regex pattern: {config['schema_comparison_regex_pattern']}")
        print("\nAvailable connections:")
        for name in database_connections.keys():
            print(f"  - {name}")
        return
    
    print(f"After filtering: {len(filtered_connections)} connections selected for comparison")
    print("Selected database connections:")
    for idx, conn_name in enumerate(filtered_connections.keys(), 1):
        print(f"  {idx}. {conn_name}")

    # Collect events from filtered databases
    print("\nCollecting events information from selected databases...")
    all_events = collect_all_events(filtered_connections, config['docs_output_dir'])

    if len(all_events) < 2:
        print("At least 2 schemas are needed for events comparison.")
        print("Try adjusting your filter criteria to include more connections.")
        return
    
    print(f"Successfully collected events from {len(all_events)} schemas.")
    
    # Generate comprehensive unified report using configured filename in docs directory
    report_filename = config['events_comparison_report_file']
    full_report_path = os.path.join(config['docs_output_dir'], report_filename)
    
    try:
        events_data, differences = analyze_and_report_events(all_events, full_report_path)
        print("Events comparison completed successfully.")
        
        # Display summary
        print("\n" + "="*50)
        print("EVENTS COMPARISON SUMMARY:")
        if config['schema_comparison_include_patterns']:
            print(f"Include patterns: {', '.join(config['schema_comparison_include_patterns'])}")
        if config['schema_comparison_exclude_patterns']:
            print(f"Exclude patterns: {', '.join(config['schema_comparison_exclude_patterns'])}")
        if config['schema_comparison_regex_pattern']:
            print(f"Regex pattern: {config['schema_comparison_regex_pattern']}")
        print(f"Schemas analyzed: {len(all_events)}")
        print(f"Report saved to: {full_report_path}")
        print("="*50)
        
    except Exception as e:
        print(f"Error during events comparison: {e}")
        traceback.print_exc()

def main() -> None:
    """
    Main function that orchestrates the entire application flow.
    """
    try:
        # Initialize environment and configuration
        config = initialize_environment()
        
        while True:
            # Display menu and get user choice
            display_main_menu()
            option = get_user_option()
            
            if option == -1:  # Invalid input
                continue
            elif option == 0:  # Exit
                print("Exiting the program. Thank you!")
                break
            
            # Handle database extraction options (1-5)
            elif option in [1, 2, 3, 4, 5]:
                # Load database connections and let user select one
                database_connections = load_grouped_vars_by_pattern()
                connection_info = select_database_connection(database_connections)
                
                if not connection_info:
                    continue
                
                # Get output format
                output_format = get_output_format()
                if not output_format:
                    continue
                
                # Execute the selected operation
                try:
                    if option == 1:
                        execute_catalog_extraction(connection_info, output_format, config)
                    elif option == 2:
                        execute_table_extraction(connection_info, output_format, config)
                    elif option == 3:
                        execute_all_tables_extraction(connection_info, output_format, config)
                    elif option == 4:
                        execute_clob_tables_extraction(connection_info, output_format, config)
                    elif option == 5:
                        execute_list_tables_extraction(connection_info, output_format, config)
                except Exception as e:
                    print(f"Error during extraction: {e}")
                    
            # Handle file comparison options (6-8)
            elif option == 6:
                print("Comparing text files...")
                generate_excel_from_diffs(config['folder_in1'], config['folder_in2'], config['folder_out'])
                print("Text file comparison completed.")
                
            elif option == 7:
                print("Comparing Excel files...")
                compare_excel_dbinfo_files(config['file_excel1'], config['file_excel2'], config['file_excel_out'])
                print("Excel file comparison completed.")
                
            elif option == 8:
                print("Comparing files in folders...")
                df1 = get_folder_files_info(config['server_folder_in1'])
                df2 = get_folder_files_info(config['server_folder_in2'])
                compare_file_info(df1, df2, config['server_compare_out'])
                print("Folder comparison completed.")
                
            # Handle schema comparison option (9)
            elif option == 9:
                execute_schema_comparison(config)
            
            # Handle events comparison option (10)
            elif option == 10:
                execute_events_comparison(config)

            # Handle log parsing option (11)
            elif option == 11:
                execute_log_parsing(config)

            # Pause before showing menu again
            input("\nPress Enter to continue...")
            
    except KeyboardInterrupt:
        print("\n\nProgram interrupted by user. Exiting...")
    except Exception as e:
        print(f"Unexpected error: {e}")
        traceback.print_exc()


if __name__ == '__main__':
    main()