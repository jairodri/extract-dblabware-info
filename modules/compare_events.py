"""
Database Events Comparison Module

This module provides functionality to compare events from different Oracle databases,
extract formula patterns, and generate comprehensive comparison reports.

Author: Database Analysis Team
Version: 1.0
"""

import logging
import pandas as pd
import re
from sqlalchemy.exc import SQLAlchemyError
from typing import Dict, List, Any, Optional, Set
import os

from modules.getdbinfo import connect_to_oracle
from modules.dumpdbinfo import generate_events_comparison_report


# ==============================================================================
# FORMULA PATTERN EXTRACTION
# ==============================================================================

def extract_subroutine_calls(formula: str) -> List[str]:
    """
    Extract all types of subroutine calls from formula CLOB field.
    
    Extracts patterns like:
    - GOSUB <subroutine>
    - Subroutine("<subroutine>"
    - BackgroundSubroutine("<subroutine>"
    - PostSubroutine("<subroutine>"
    
    Ignores commented calls that start with apostrophe ('GOSUB).
    Case-insensitive matching for all patterns.
    
    Args:
        formula (str): Formula CLOB content
        
    Returns:
        List[str]: List of unique subroutine names called
    """
    if not formula or pd.isna(formula):
        return []
    
    subroutines = set()  # Use set to avoid duplicates
    
    try:
        formula_str = str(formula)
        
        # Pattern 1: GOSUB <subroutine> (original pattern)
        # (?<!') ensures no apostrophe before GOSUB (not commented)
        # \s+ matches one or more whitespace characters
        # ([A-Z_][A-Z0-9_]*) captures the subroutine name
        gosub_pattern = r'(?<!\')GOSUB\s+([A-Z_][A-Z0-9_]*)'
        gosub_matches = re.findall(gosub_pattern, formula_str, re.IGNORECASE | re.MULTILINE)
        subroutines.update(gosub_matches)
        
        # Pattern 2: Function-style calls with quoted parameters
        # Matches: Subroutine("<name>"), BackgroundSubroutine("<name>"), PostSubroutine("<name>")
        # Case-insensitive matching for function names
        function_patterns = [
            r'(?<!\')Subroutine\s*\(\s*["\']([^"\']+)["\']',                    # Subroutine("name")
            r'(?<!\')BackgroundSubroutine\s*\(\s*["\']([^"\']+)["\']',          # BackgroundSubroutine("name")
            r'(?<!\')PostSubroutine\s*\(\s*["\']([^"\']+)["\']'                 # PostSubroutine("name")
        ]
        
        for pattern in function_patterns:
            matches = re.findall(pattern, formula_str, re.IGNORECASE | re.MULTILINE)
            subroutines.update(matches)
        
        # Return unique subroutines, sorted for consistency
        return sorted(list(subroutines))
        
    except Exception as e:
        logging.warning(f"Error extracting subroutine calls from formula: {e}")
        return []


# Mantener la función original por compatibilidad (alias)
def extract_gosub_calls(formula: str) -> List[str]:
    """
    Alias for extract_subroutine_calls for backward compatibility.
    
    Args:
        formula (str): Formula CLOB content
        
    Returns:
        List[str]: List of unique subroutine names called
    """
    return extract_subroutine_calls(formula)

# ==============================================================================
# DATA COLLECTION FUNCTIONS
# ==============================================================================

def collect_events_data(connection_info: Dict[str, str]) -> Dict[str, pd.DataFrame]:
    """
    Collect events data from the three event tables in a database.
    
    Args:
        connection_info (Dict[str, str]): Database connection parameters
        
    Returns:
        Dict[str, pd.DataFrame]: Dictionary with DataFrames for each event table
    """
    host = connection_info['host']
    port = connection_info['port']
    service_name = connection_info['service_name']
    username = connection_info['user']
    password = connection_info['password']
    owner = connection_info['owner']
    
    # Establish database connection
    engine = connect_to_oracle(host, port, service_name, username, password)
    if engine is None:
        logging.error(f"Failed to connect to database {service_name}")
        return {}
    
    events_data = {}
    
    # Define queries for each event table
    queries = {
        'events': f"""
            SELECT e.template, e.event, e.formula 
            FROM {owner}.events e 
            WHERE e.enabled_flag = 'T' AND e.formula_flag = 'T'
            ORDER BY e.template, e.event
        """,
        'test_events': f"""
            SELECT te.template, te.event, te.formula 
            FROM {owner}.test_events te 
            WHERE te.enabled_flag = 'T' AND te.formula_flag = 'T' 
            ORDER BY te.template, te.event
        """,
        'database_events': f"""
            SELECT db.table_name, db.event_name, db.sub_name, db.or_fields, 
                   db.field_name_1, db.has_value_1, db.changed_from_1, db.changed_to_1, 
                   db.field_name_2, db.has_value_2, db.changed_from_2, db.changed_to_2 
            FROM {owner}.database_events db 
            ORDER BY db.table_name, db.event_name
        """
    }
    
    try:
        with engine.connect() as connection:
            for table_name, query in queries.items():
                try:
                    logging.info(f"Executing query for {table_name} in {service_name}")
                    df = pd.read_sql_query(query, connection)
                    
                    # Process formula fields for events and test_events
                    if table_name in ['events', 'test_events'] and 'formula' in df.columns:
                        df = process_formula_field(df)
                    
                    events_data[table_name] = df
                    logging.info(f"Retrieved {len(df)} records from {table_name}")
                    
                except SQLAlchemyError as e:
                    logging.error(f"Error querying {table_name} in {service_name}: {e}")
                    events_data[table_name] = pd.DataFrame()
                    
    except Exception as e:
        logging.error(f"Unexpected error collecting events from {service_name}: {e}")
        return {}
    
    return events_data


def process_formula_field(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process formula field to extract subroutine calls and create derived columns.
    
    Extracts all types of subroutine calls (GOSUB, Subroutine, BackgroundSubroutine, PostSubroutine)
    and creates summary columns for analysis.
    
    Args:
        df (pd.DataFrame): DataFrame with formula column
        
    Returns:
        pd.DataFrame: DataFrame with processed subroutine call information
    """
    df_processed = df.copy()
    
    # Extract all subroutine calls from formula field
    df_processed['subroutine_calls'] = df_processed['formula'].apply(extract_subroutine_calls)
    
    # Convert subroutine calls list to string for Excel compatibility and readability
    df_processed['calls_list'] = df_processed['subroutine_calls'].apply(
        lambda x: ', '.join(x) if x else ''
    )
    
    # Count the number of unique subroutine calls
    df_processed['calls_count'] = df_processed['subroutine_calls'].apply(len)
    
    # Drop the original formula column and the list version to avoid Excel export issues
    df_processed = df_processed.drop(['formula', 'subroutine_calls'], axis=1)
    
    return df_processed

def collect_all_events(connections: Dict[str, Dict[str, str]], 
                      docs_dir: Optional[str] = None) -> Dict[str, Dict[str, pd.DataFrame]]:
    """
    Collect events data from all specified database connections.
    
    Args:
        connections (Dict[str, Dict[str, str]]): Database connections
        docs_dir (str, optional): Directory for log files
        
    Returns:
        Dict[str, Dict[str, pd.DataFrame]]: Nested dictionary with events data
                                           {schema_name: {table_name: DataFrame}}
    """
    # Set up logging
    if docs_dir:
        log_file = os.path.join(docs_dir, 'events_comparison.log')
        logging.basicConfig(
            filename=log_file,
            filemode='w',
            format='%(asctime)s - %(levelname)s - %(message)s',
            level=logging.INFO,
            encoding='utf-8',
            force=True
        )
    
    logging.info("Starting events data collection")
    
    all_events = {}
    
    for schema_name, conn_info in connections.items():
        logging.info(f"Collecting events from schema: {schema_name}")
        print(f"Collecting events from schema: {schema_name}")
        
        events_data = collect_events_data(conn_info)
        
        if events_data:
            all_events[schema_name] = events_data
            
            # Log summary for this schema
            total_records = sum(len(df) for df in events_data.values())
            logging.info(f"Schema {schema_name}: {total_records} total event records")
            print(f"  - Total records: {total_records}")
            
            for table_name, df in events_data.items():
                logging.info(f"  {table_name}: {len(df)} records")
                print(f"  - {table_name}: {len(df)} records")
        else:
            logging.warning(f"No events data collected from {schema_name}")
            print(f"  - Warning: No data collected")
    
    logging.info(f"Events collection completed. {len(all_events)} schemas processed")
    print(f"\nEvents collection completed. {len(all_events)} schemas processed")
    
    return all_events


# ==============================================================================
# COMPARISON FUNCTIONS
# ==============================================================================

def create_events_universe(all_events: Dict[str, Dict[str, pd.DataFrame]]) -> Dict[str, Any]:
    """
    Create a universe of all events across all schemas and tables.
    
    Args:
        all_events (Dict[str, Dict[str, pd.DataFrame]]): All events data
        
    Returns:
        Dict[str, Any]: Universe containing all unique events and schemas
    """
    universe = {
        'schemas': list(all_events.keys()),
        'tables': ['events', 'test_events', 'database_events'],
        'all_events': {},
        'all_test_events': {},
        'all_database_events': {}
    }
    
    # Collect unique events for each table type
    for table_type in ['events', 'test_events', 'database_events']:
        all_entries = set()
        
        for schema_name, schema_data in all_events.items():
            if table_type in schema_data:
                df = schema_data[table_type]
                
                if table_type in ['events', 'test_events']:
                    # For events and test_events: use template + event as identifier
                    if 'template' in df.columns and 'event' in df.columns:
                        entries = [(row['template'], row['event']) for _, row in df.iterrows()]
                        all_entries.update(entries)
                        
                elif table_type == 'database_events':
                    # For database_events: use table_name + event_name as identifier
                    if 'table_name' in df.columns and 'event_name' in df.columns:
                        entries = [(row['table_name'], row['event_name']) for _, row in df.iterrows()]
                        all_entries.update(entries)
        
        universe[f'all_{table_type}'] = sorted(list(all_entries))
    
    return universe


def compare_events_data(all_events: Dict[str, Dict[str, pd.DataFrame]]) -> pd.DataFrame:
    """
    Compare events data across all schemas and generate differences report.
    
    Args:
        all_events (Dict[str, Dict[str, pd.DataFrame]]): All events data
        
    Returns:
        pd.DataFrame: DataFrame with comparison results
    """
    print("Comparing events data across schemas...")
    logging.info("Starting events comparison")
    
    if len(all_events) < 2:
        logging.warning("At least 2 schemas required for comparison")
        return pd.DataFrame()
    
    universe = create_events_universe(all_events)
    differences = []
    
    # Compare each table type
    for table_type in ['events', 'test_events', 'database_events']:
        logging.info(f"Comparing {table_type}")
        print(f"  Comparing {table_type}...")
        
        table_differences = compare_table_events(all_events, table_type, universe)
        differences.extend(table_differences)
    
    # Create differences DataFrame
    if differences:
        diff_df = pd.DataFrame(differences)
        diff_df = diff_df.sort_values(['table_type', 'identifier', 'difference_type'])
        logging.info(f"Found {len(diff_df)} total differences")
        print(f"Found {len(diff_df)} total differences")
        return diff_df
    else:
        # Return empty DataFrame with correct structure
        columns = ['table_type', 'identifier', 'difference_type'] + universe['schemas']
        logging.info("No differences found between schemas")
        print("No differences found between schemas")
        return pd.DataFrame(columns=columns)


def compare_table_events(all_events: Dict[str, Dict[str, pd.DataFrame]], 
                        table_type: str, universe: Dict[str, Any]) -> List[Dict]:
    """
    Compare events for a specific table type across schemas.
    Checks both existence and value consistency.
    
    Args:
        all_events (Dict[str, Dict[str, pd.DataFrame]]): All events data
        table_type (str): Type of table to compare ('events', 'test_events', 'database_events')
        universe (Dict[str, Any]): Universe data
        
    Returns:
        List[Dict]: List of difference records
    """
    differences = []
    all_entries = universe[f'all_{table_type}']
    
    logging.info(f"Comparing {len(all_entries)} unique {table_type} entries")
    
    missing_count = 0
    mismatch_count = 0
    
    for entry in all_entries:
        # Check which schemas have this entry and collect their values
        schema_status = {}
        schema_values = {}  # Store the actual values for comparison
        
        for schema_name in universe['schemas']:
            schema_data = all_events.get(schema_name, {})
            table_df = schema_data.get(table_type, pd.DataFrame())
            
            if table_type in ['events', 'test_events']:
                template, event = entry
                matching_rows = table_df[
                    (table_df['template'] == template) & (table_df['event'] == event)
                ] if not table_df.empty else pd.DataFrame()
                
                identifier = f"{template}.{event}"
                compare_field = 'calls_list'
                
            elif table_type == 'database_events':
                table_name, event_name = entry
                matching_rows = table_df[
                    (table_df['table_name'] == table_name) & (table_df['event_name'] == event_name)
                ] if not table_df.empty else pd.DataFrame()
                
                identifier = f"{table_name}.{event_name}"
                compare_field = 'sub_name'
            
            # Check existence and get value
            if not matching_rows.empty:
                schema_status[schema_name] = 'EXISTS'
                # Get the value for comparison (take first row if multiple)
                if compare_field in matching_rows.columns:
                    value = matching_rows.iloc[0][compare_field]
                    # Convert NaN to empty string for consistent comparison
                    schema_values[schema_name] = str(value) if pd.notna(value) else ''
                else:
                    schema_values[schema_name] = ''
            else:
                schema_status[schema_name] = 'MISSING'
                schema_values[schema_name] = 'N/A'
        
        # Check for existence differences
        status_values = set(schema_status.values())
        if len(status_values) > 1:  # Different existence statuses across schemas
            missing_count += 1
            
            missing_schemas = [name for name, status in schema_status.items() if status == 'MISSING']
            existing_schemas = [name for name, status in schema_status.items() if status == 'EXISTS']
            
            logging.debug(f"Missing event {identifier}: exists in {existing_schemas}, missing in {missing_schemas}")
            
            row = {
                'table_type': table_type,
                'identifier': identifier,
                'difference_type': 'Event Missing'
            }
            
            # Add status for each schema
            for schema_name in universe['schemas']:
                row[schema_name] = schema_status.get(schema_name, 'N/A')
            
            differences.append(row)
        
        # Check for value differences (only for existing events)
        existing_schemas = [name for name, status in schema_status.items() if status == 'EXISTS']
        if len(existing_schemas) > 1:  # Only compare if event exists in multiple schemas
            existing_values = [schema_values[name] for name in existing_schemas]
            unique_values = set(existing_values)
            
            if len(unique_values) > 1:  # Different values across schemas
                mismatch_count += 1
                
                # Create summary for logging
                value_groups = analyze_value_differences(schema_values, existing_schemas)
                summary = format_value_difference_summary(value_groups)
                logging.debug(f"Value mismatch for {identifier} ({compare_field}): {summary}")
                
                row = {
                    'table_type': table_type,
                    'identifier': identifier,
                    'difference_type': f'{compare_field.replace("_", " ").title()} Mismatch'
                }
                
                # Add actual values for each schema
                for schema_name in universe['schemas']:
                    if schema_name in existing_schemas:
                        row[schema_name] = schema_values[schema_name]
                    else:
                        row[schema_name] = schema_status.get(schema_name, 'N/A')
                
                differences.append(row)
    
    logging.info(f"Table {table_type} comparison: {missing_count} missing events, {mismatch_count} value mismatches")
    
    return differences

def analyze_value_differences(schema_values: Dict[str, str], existing_schemas: List[str]) -> Dict[str, List[str]]:
    """
    Analyze value differences and group schemas by their values.
    
    Args:
        schema_values (Dict[str, str]): Schema name to value mapping
        existing_schemas (List[str]): List of schemas where the event exists
        
    Returns:
        Dict[str, List[str]]: Dictionary mapping values to lists of schemas that have that value
    """
    value_groups = {}
    
    for schema_name in existing_schemas:
        value = schema_values[schema_name]
        if value not in value_groups:
            value_groups[value] = []
        value_groups[value].append(schema_name)
    
    return value_groups


def format_value_difference_summary(value_groups: Dict[str, List[str]]) -> str:
    """
    Format a summary of value differences for logging.
    
    Args:
        value_groups (Dict[str, List[str]]): Value groups from analyze_value_differences
        
    Returns:
        str: Formatted summary string
    """
    summary_parts = []
    for value, schemas in value_groups.items():
        value_display = f"'{value}'" if value else "'<empty>'"
        schemas_str = ', '.join(schemas)
        summary_parts.append(f"{value_display} in [{schemas_str}]")
    
    return " vs ".join(summary_parts)

def analyze_and_report_events(all_events: Dict[str, Dict[str, pd.DataFrame]], 
                            output_filename: str) -> tuple:
    """
    Perform complete events analysis and generate report.
    
    Args:
        all_events (Dict[str, Dict[str, pd.DataFrame]]): Events data
        output_filename (str): Output file path
        
    Returns:
        tuple: (original_data, differences_df)
    """
    logging.info("Starting complete events analysis")
    
    # Generate differences analysis
    diff_df = compare_events_data(all_events)
    
    # Generate report
    generate_events_comparison_report(all_events, diff_df, output_filename)
    
    # Log detailed summary
    if not diff_df.empty:
        # Overall summary
        diff_summary = diff_df['difference_type'].value_counts().to_dict()
        logging.info(f"Events differences summary: {diff_summary}")
        print("Events differences summary:")
        for diff_type, count in diff_summary.items():
            print(f"  - {diff_type}: {count}")
        
        # Detailed breakdown by table type
        print("\nDetailed breakdown:")
        for table_type in ['events', 'test_events', 'database_events']:
            table_diffs = diff_df[diff_df['table_type'] == table_type]
            if not table_diffs.empty:
                table_summary = table_diffs['difference_type'].value_counts().to_dict()
                print(f"  {table_type.replace('_', ' ').title()}:")
                for diff_type, count in table_summary.items():
                    print(f"    - {diff_type}: {count}")
        
        # Log critical mismatches
        mismatch_types = [dt for dt in diff_summary.keys() if 'Mismatch' in dt]
        if mismatch_types:
            logging.warning(f"Found value mismatches in: {', '.join(mismatch_types)}")
            print(f"\n⚠️  Critical: Found value mismatches in {len(mismatch_types)} categories")
            
    else:
        logging.info("All events are identical across schemas")
        print("✅ All events are identical across schemas!")
    
    return all_events, diff_df
