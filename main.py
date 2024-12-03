import os
import re
import sys
from pathlib import Path
from dotenv import load_dotenv
from modules.getdbinfo import get_dbinfo_metadata, get_dbinfo_table, get_dbinfo_all_tables, get_dbinfo_tables_with_clob, get_dbinfo_list_of_tables
from modules.dumpdbinfo import dump_dbinfo_to_csv, dump_dbinfo_to_excel
from modules.comparefiles import generate_excel_from_diffs, compare_excel_dbinfo_files, get_folder_files_info, compare_file_info


def load_grouped_vars_by_pattern():
    """
    Loads all environment variables that match the pattern 
    <ENVIRONMENT>_<COMPLEX>_<VERSION>_<VARIABLE> and groups them by ENVIRONMENT, 
    COMPLEX, and VERSION.

    Returns:
        dict: A dictionary where the key is the NAME variable, and the value 
              is a dictionary containing the remaining variables 
              (HOST, PORT, SERVICE_NAME, USER, PASSWORD, OWNER).
    """
    # Define the regex pattern to match the variables
    pattern = re.compile(r'^(DES|PRE|PRO)_([A-Z]{2,3})_(V[6-8])_(NAME|HOST|PORT|SERVICE_NAME|USER|PASSWORD|OWNER)$')
    grouped_vars = {}

    for key, value in os.environ.items():
        match = pattern.match(key)
        if match:
            # Extract the parts of the key
            environment, complex_code, version, variable = match.groups()
            group_key = f"{environment}_{complex_code}_{version}"

            # Initialize the group dictionary if not already present
            if group_key not in grouped_vars:
                grouped_vars[group_key] = {}

            # Add the variable to the dictionary
            grouped_vars[group_key][variable.lower()] = value

    # Transform grouped_vars so that the NAME variable becomes the key
    result = {}
    for group_key, vars_dict in grouped_vars.items():
        if 'name' in vars_dict:
            # Use 'name' as the key and exclude it from the subdictionary
            result[vars_dict.pop('name')] = vars_dict

    return result


def load_grouped_vars(prefix):
    """Loads all environment variables starting with the given prefix into a dictionary."""
    grouped_vars = {}
    for key, value in os.environ.items():
        if key.startswith(prefix):
            # Add the variable to the dictionary, removing the prefix of the key name
            grouped_vars[key[len(prefix):].lower()] = value
    return grouped_vars

# Agregar el directorio base al path de Python
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Ruta al archivo .env dentro de la carpeta resources
# dotenv_path = os.path.join(os.path.dirname(__file__), 'resources', '.env')
env_path = Path(__file__).parent / "resources" / ".env"

# Load variables from the .env file
load_dotenv(env_path)

# Get the database connections params
connection_info = load_grouped_vars('DES_COR_V8_')

# Get the directory where the output files will be saved
output_dir_metadata = os.getenv('OUTPUT_DIR_METADATA')
output_dir_data = os.getenv('OUTPUT_DIR_DATA')

# Get the table to extract 
table_name = os.getenv('TABLE_NAME')

sql_query = os.getenv('SQL_QUERY')
# if sql_query is empty, set it to None
sql_query = sql_query if sql_query else None

sql_filter = os.getenv('SQL_FILTER')
# if sql_filter is empty, set it to None
sql_filter = sql_filter if sql_filter else None

# Get the limits for the number of records to extract per table
max_records_per_table = int(os.getenv('MAX_RECORDS_PER_TABLE', 1000))

# Get the limits for the number total of records to extract
total_records_limit = int(os.getenv('TOTAL_RECORDS_LIMIT', 300000))

# Get the separator for the csv files
csv_separator = os.getenv('CSV_SEPARATOR', '|')

# Convert environment variable from string to list
tables_with_clob_to_exclude = os.getenv('TABLES_WITH_CLOB_TO_EXCLUDE', '').split(',')
tables_with_clob_to_exclude = [table.strip() for table in tables_with_clob_to_exclude]  # Remove any extra spaces

tables_to_exclude = os.getenv('TABLES_TO_EXCLUDE', '').split(',')
tables_to_exclude = [table.strip() for table in tables_to_exclude]  # Remove any extra spaces

table_list = os.getenv('TABLE_LIST', '').split(',')
table_list = [table.strip() for table in table_list]  # Remove any extra spaces

# Get the folders to compare
folder_in1 = os.getenv('COMPARE_FOLDER_IN1')
folder_in2 = os.getenv('COMPARE_FOLDER_IN2')
folder_out = os.getenv('COMPARE_FOLDER_OUT')
server_folder_in1 = os.getenv('SERVER_FOLDER_IN1')
server_folder_in2 = os.getenv('SERVER_FOLDER_IN2')
server_compare_out = os.getenv('COMPARE_SERVER_OUT')

# Get the files to compare
file_in1 = os.getenv('COMPARE_FILE_IN1')
file_in2 = os.getenv('COMPARE_FILE_IN2')
file_out = os.getenv('COMPARE_FILE_OUT')
file_excel1 = os.getenv('COMPARE_EXCEL_FILE_1')
file_excel2 = os.getenv('COMPARE_EXCEL_FILE_2')
file_excel_out = os.getenv('COMPARE_EXCEL_FILE_OUT')


def main():
    # Mostrar el menú principal
    print("\nSelect an option to run:")
    print("1 - Get catalog info and dump it to csv/excel file")
    print("2 - Get data from a specific table and dump it to csv/excel file")
    print("3 - Get data from all tables and dump them to csv/excel file")
    print("4 - Get data from tables with clob fields and dump to csv/excel file")
    print("5 - Get data from a list of tables and dump to csv/excel file")
    print("6 - Compare files and generate excel with differences")
    
    try:
        option = int(input("\nEnter the option number: "))
    except ValueError:
        print("Invalid option. Exiting.")
        return

    # Si se selecciona una opción de la 1 a la 5, pedir conexión a base de datos
    if option in [1, 2, 3, 4, 5]:
        # Cargar conexiones disponibles
        database_connections = load_grouped_vars_by_pattern()

        # Mostrar las opciones de conexión disponibles
        print("\nAvailable database connections:")
        connection_keys = list(database_connections.keys())
        for idx, key in enumerate(connection_keys, start=1):
            print(f"{idx}. {key}")

        # Pedir al usuario que seleccione una conexión
        while True:
            try:
                selected_index = int(input("\nSelect a database connection by number: "))
                if 1 <= selected_index <= len(connection_keys):
                    selected_connection = connection_keys[selected_index - 1]
                    break
                else:
                    print("Invalid selection. Please select a valid number.")
            except ValueError:
                print("Invalid input. Please enter a number.")

        # Crear el diccionario connection_info basado en la selección del usuario
        connection_info = database_connections[selected_connection]

        # Pedir el formato de salida
        output_format = input("\nEnter the output format (csv/excel): ").strip().lower()
        if output_format not in ['csv', 'excel']:
            print("Invalid output format selected.")
            return

        # Ejecutar la opción seleccionada
        if option == 1:
            db_info_catalog = get_dbinfo_metadata(connection_info)
            if output_format == 'excel':
                dump_dbinfo_to_excel(connection_info['service_name'], db_info_catalog, output_dir_metadata)
            else:
                dump_dbinfo_to_csv(connection_info['service_name'], db_info_catalog, output_dir_metadata, sep=csv_separator)
        elif option == 2:
            db_info_table = get_dbinfo_table(connection_info, table_name, sql_filter=sql_filter, sql_query=sql_query, max_records_per_table=max_records_per_table)
            if output_format == 'excel':
                dump_dbinfo_to_excel(connection_info['service_name'], db_info_table, output_dir_data, include_record_count=True, max_records_per_table=max_records_per_table, file_name=table_name)
            else:
                dump_dbinfo_to_csv(connection_info['service_name'], db_info_table, output_dir_data, sep=csv_separator, suffix=None)
        elif option == 3:
            db_info_all_tables = get_dbinfo_all_tables(connection_info, tables_to_exclude, total_records_limit=total_records_limit, max_records_per_table=max_records_per_table)
            if output_format == 'excel':
                dump_dbinfo_to_excel(connection_info['service_name'], db_info_all_tables, output_dir_data, include_record_count=True, max_records_per_table=max_records_per_table)
            else:
                dump_dbinfo_to_csv(connection_info['service_name'], db_info_all_tables, output_dir_data, sep=csv_separator) 
        elif option == 4:
            tables_with_clob = get_dbinfo_tables_with_clob(connection_info, tables_with_clob_to_exclude)
            if output_format == 'excel':
                dump_dbinfo_to_excel(connection_info['service_name'], tables_with_clob, output_dir_data, include_record_count=True, max_records_per_table=max_records_per_table)
            else:
                dump_dbinfo_to_csv(connection_info['service_name'], tables_with_clob, output_dir_data, sep=csv_separator)    
        elif option == 5:
            info_tables = get_dbinfo_list_of_tables(table_list, connection_info)
            if output_format == 'excel':
                dump_dbinfo_to_excel(connection_info['service_name'], info_tables, output_dir_data, include_record_count=True, max_records_per_table=max_records_per_table)
            else:
                dump_dbinfo_to_csv(connection_info['service_name'], info_tables, output_dir_data, sep=csv_separator) 
    elif option == 6:
        # Opción 6 no requiere conexión a base de datos
        generate_excel_from_diffs(folder_in1, folder_in2, folder_out)
    else:
        print("Invalid option selected.")


if __name__ == '__main__':
    main()


#     # differences = compare_excel_dbinfo_files(file_excel1, file_excel2, file_excel_out)
#     # df1 = get_folder_files_info(server_folder_in1, 'sll')
#     # df2 = get_folder_files_info(server_folder_in2, 'sll')
#     # compare_file_info(df1, df2, server_compare_out)
    #


