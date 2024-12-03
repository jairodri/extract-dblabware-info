# extract-dblabware-info

Extract metadata information and obtain data from Labware Oracle database to be managed through Pandas dataframes.

## Operations

The script provides the following operations that can be selected by the user:

1. **Get catalog info and dump it to csv/excel file**
   - Retrieves metadata information from the database and dumps it into a CSV or Excel file.

2. **Get data from a specific table and dump it to csv/excel file**
   - Retrieves data from a specified table in the database and dumps it into a CSV or Excel file.

3. **Get data from all tables and dump them to csv/excel file**
   - Retrieves data from all tables in the database and dumps it into CSV or Excel files.

4. **Get data from tables with clob fields and dump to csv/excel file**
   - Retrieves data from tables that contain CLOB fields and dumps it into CSV or Excel files.

5. **Get data from a list of tables and dump to csv/excel file**
   - Retrieves data from a specified list of tables in the database and dumps it into CSV or Excel files.

6. **Compare files and generate excel with differences**
   - Compares two Excel files and generates a new Excel file highlighting the differences.

## Usage

1. **Set Up Environment Variables**:
   - Create a `.env` file in the `resources` directory (`\LWutils\_internal\resources\.env` if you have the executable version) with the necessary environment variables. 
   Example:

     ```env
     OUTPUT_DIR_METADATA=./output/metadata
     OUTPUT_DIR_DATA=./output/data
     TABLE_NAME=your_table_name
     SQL_QUERY=your_sql_query
     SQL_FILTER=your_sql_filter
     MAX_RECORDS_PER_TABLE=1000
     TOTAL_RECORDS_LIMIT=300000
     CSV_SEPARATOR=|
     TABLES_WITH_CLOB_TO_EXCLUDE=table1,table2
     TABLES_TO_EXCLUDE=table3,table4
     ```


2. **Run the Script**:
   - Execute the script and follow the prompts to select the desired operation and output format (CSV or Excel).
     ```sh
     python main.py
     or
     LWutils.exe (if you have the executable version) 
     ```


