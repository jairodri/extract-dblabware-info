# Database Analysis and Comparison Tool

A comprehensive Python tool for extracting, analyzing, and comparing Oracle database schemas, events, and generating detailed reports.

## 🚀 Features

- **Database Metadata Extraction**: Extract catalog information, table data, and CLOB fields
- **Schema Comparison**: Compare database schemas across multiple environments
- **Events Analysis**: Compare events, test events, and database events between schemas
- **Log Analysis**: Parse HTTP status codes from web service logs
- **File Comparison**: Compare text files, Excel files, and folder contents
- **Multiple Output Formats**: Support for CSV and Excel outputs
- **Configurable Filtering**: Advanced pattern matching for database selection
- **Comprehensive Reporting**: Detailed Excel reports with multiple sheets and summaries

## 📋 Table of Contents

- [Installation](#installation)
- [Configuration](#configuration)
- [Menu Options](#menu-options)
- [Usage Examples](#usage-examples)
- [Output Structure](#output-structure)
- [Advanced Features](#advanced-features)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)

## 🛠️ Installation

### Prerequisites

- Python 3.8+
- Oracle Client libraries
- Required Python packages (see `requirements.txt`)

### Setup

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd extract-dblabware-info
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment:**
   ```bash
   cp resources/.env.template resources/.env
   # Edit resources/.env with your database connections and settings
   ```

4. **Run the application:**
   ```bash
   python main.py
   ```

## ⚙️ Configuration

Copy `resources/.env.template` to `resources/.env` and configure your settings:

```properties
# Output directories
OUTPUT_DIR_DATA=C:/path/to/output
DOCS_OUTPUT_DIR=docs

# Database connections (example)
PRO_NYC_V8_NAME=Production_NewYork_V8
PRO_NYC_V8_HOST=prod-db.company.com
PRO_NYC_V8_PORT=1521
PRO_NYC_V8_SERVICE_NAME=PRODNY
PRO_NYC_V8_USER=app_user
PRO_NYC_V8_PASSWORD=secure_password
PRO_NYC_V8_OWNER=SCHEMA_OWNER
```

See [`README_SETUP.md`](README_SETUP.md) for detailed configuration instructions.

## 📖 Menu Options

### 🗄️ Database Extraction (Options 1-5)

#### **1. Extract Catalog Metadata**
- **Purpose**: Extract complete database schema metadata
- **Output**: Table structures, columns, indexes, constraints, triggers
- **Use Case**: Schema documentation and analysis
- **Formats**: CSV or Excel

#### **2. Extract Specific Table Data**
- **Purpose**: Extract data from a single specified table
- **Configuration**: Set `TABLE_NAME` in `.env`
- **Features**: Custom SQL queries, filtering, record limits
- **Use Case**: Detailed analysis of specific table contents

#### **3. Extract All Tables Data**
- **Purpose**: Extract data from all tables in the database
- **Features**: Configurable exclusion lists, performance limits
- **Configuration**: `TABLES_TO_EXCLUDE`, `MAX_RECORDS_PER_TABLE`
- **Use Case**: Complete database backup or migration analysis

#### **4. Extract Tables with CLOB Fields**
- **Purpose**: Extract tables containing CLOB (large text) fields
- **Features**: Handles large text data, configurable exclusions
- **Configuration**: `TABLES_WITH_CLOB_TO_EXCLUDE`
- **Use Case**: Document management system analysis

#### **5. Extract Specific List of Tables**
- **Purpose**: Extract data from a predefined list of tables
- **Configuration**: Set `TABLE_LIST` in `.env`
- **Use Case**: Targeted data extraction for specific business processes

### 📊 File and Data Comparison (Options 6-8)

#### **6. Compare Text Files**
- **Purpose**: Compare two text files and generate difference report
- **Output**: Excel file with highlighted differences
- **Use Case**: Configuration file comparison, script analysis

#### **7. Compare Excel Files**
- **Purpose**: Compare two Excel files sheet by sheet
- **Features**: Cell-by-cell comparison, difference highlighting
- **Use Case**: Data validation, report comparison

#### **8. Compare Files in Folders**
- **Purpose**: Compare file listings between two folders
- **Features**: File size, modification date, existence comparison
- **Use Case**: Deployment verification, backup validation

### 🏗️ Advanced Database Analysis (Options 9-10)

#### **9. Compare Database Schemas**
- **Purpose**: Comprehensive comparison of database schemas across multiple environments
- **Features**: 
  - Table structure comparison
  - Column differences analysis
  - Index and constraint comparison
  - Configurable filtering (include/exclude patterns, regex)
- **Output**: Detailed Excel report with per-schema sheets and differences summary
- **Use Case**: Environment synchronization, deployment validation

#### **10. Compare Events Between Databases**
- **Purpose**: Compare events, test events, and database events across schemas
- **Features**:
  - Event existence comparison
  - Subroutine call analysis (GOSUB, Subroutine, BackgroundSubroutine, PostSubroutine)
  - Formula pattern extraction
  - Value mismatch detection
- **Output**: Comprehensive Excel report with event analysis
- **Use Case**: Application logic comparison, event synchronization

### 📝 Log Analysis (Option 11)

#### **11. Parse HTTP Status Codes from Web Service Logs**
- **Purpose**: Extract and analyze HTTP status codes from integration logs
- **Features**:
  - Pattern recognition for HTTP headers and status codes
  - Timestamp extraction
  - Statistical summary
- **Input Format**: 
  ```
  ==={Received HTTP Header 18/07/2025 07:19:17}====
  HTTP/1.1 100 Continue
  HTTP/1.1 500 Internal Server Error
  ```
- **Output**: CSV report with status codes, timestamps, and statistics
- **Use Case**: API monitoring, integration troubleshooting

## 💡 Usage Examples

### Basic Database Extraction
```bash
# Run the application
python main.py

# Select option 1 (Extract catalog metadata)
# Choose your database connection
# Select output format (csv/excel)
```

### Schema Comparison with Filtering
```properties
# In .env file
SCHEMA_COMPARISON_INCLUDE_PATTERNS=PRO_,_V8
SCHEMA_COMPARISON_EXCLUDE_PATTERNS=_TEST_,_TEMP_
```

### Events Comparison
```bash
# Run option 10
# Uses same filtering as schema comparison
# Automatically analyzes events, test_events, and database_events tables
```

### Log Analysis
```bash
# Run option 11
# Specify log file path
# Choose output CSV location
# View statistical summary
```

## 📁 Output Structure

```
project/
├── docs/                           # Generated reports and logs
│   ├── schema_comparison.xlsx      # Schema comparison report
│   ├── events_comparison.xlsx      # Events comparison report
│   ├── http_status_report.csv      # HTTP log analysis
│   └── *.log                       # Process logs
├── output/                         # Data extraction outputs
│   ├── DATABASE_catalog/           # Catalog metadata
│   ├── DATABASE_all_tables/        # All tables data
│   └── DATABASE_table_name/        # Specific table data
└── resources/
    ├── .env                        # Your configuration (not in git)
    └── .env.template              # Configuration template
```

## 🔧 Advanced Features

### Database Connection Filtering

**Include Patterns** (ALL must match):
```properties
SCHEMA_COMPARISON_INCLUDE_PATTERNS=PRO_,_V8
# Matches: PRO_NYC_V8, PRO_LON_V8
# Excludes: DES_NYC_V8, PRO_NYC_V7
```

**Exclude Patterns** (ANY match excludes):
```properties
SCHEMA_COMPARISON_EXCLUDE_PATTERNS=_TEST_,_TEMP_
# Excludes: PRO_TEST_V8, PRO_TEMP_V8
```

**Regex Patterns** (advanced filtering):
```properties
SCHEMA_COMPARISON_REGEX_PATTERN=^PRO_[A-Z]{3}_V8$
# Matches: PRO_NYC_V8, PRO_LON_V8
# Excludes: PRO_NEWYORK_V8, PRO_NYC_V7
```

### Event Analysis Patterns

The tool extracts various subroutine call patterns:
- `GOSUB SUBROUTINE_NAME`
- `Subroutine("SUBROUTINE_NAME")`
- `BackgroundSubroutine("SUBROUTINE_NAME")`
- `PostSubroutine("SUBROUTINE_NAME")`

Commented calls (starting with `'`) are automatically ignored.

### Performance Configuration

```properties
MAX_RECORDS_PER_TABLE=40000      # Limit per table
TOTAL_RECORDS_LIMIT=400000       # Overall limit for all tables
CSV_SEPARATOR=|                  # CSV field separator
```

## 🐛 Troubleshooting

### Common Issues

**Database Connection Failed**
```
- Check Oracle client installation
- Verify network connectivity
- Confirm credentials in .env file
- Check firewall settings
```

**Memory Issues with Large Tables**
```
- Reduce MAX_RECORDS_PER_TABLE
- Use table filtering options
- Exclude large tables from extraction
```

**Permission Errors**
```
- Verify user has SELECT permissions
- Check schema ownership settings
- Confirm password hasn't expired
```

**Excel Export Errors**
```
- Check output directory permissions
- Ensure sufficient disk space
- Verify no conflicting Excel processes
```

### Log Files

Check `docs/*.log` files for detailed error information:
- `schema_comparison.log` - Schema analysis logs
- `events_comparison.log` - Events analysis logs
- Application logs for general issues

## 📈 Performance Tips

1. **Use filtering** to limit the number of databases analyzed
2. **Set appropriate record limits** for large databases
3. **Exclude unnecessary tables** from extraction
4. **Use specific table extraction** for targeted analysis
5. **Monitor log files** for performance bottlenecks

## 🔒 Security Considerations

- **Never commit `.env` files** to version control
- **Use read-only database users** when possible
- **Limit network access** to database servers
- **Regularly rotate passwords** for database accounts
- **Review output files** before sharing (may contain sensitive data)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Update documentation
6. Submit a pull request

### Development Setup

```bash
# Clone your fork
git clone <your-fork-url>
cd extract-dblabware-info

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate     # Windows

# Install development dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt  # if exists
```

## 📄 License

The source code license is [MIT](https://opensource.org/license/mit/), as described in the LICENSE file.

## 🆘 Support

For support:
1. Check the troubleshooting section
2. Review log files in `docs/`
3. Verify configuration in `.env`
4. Create an issue with detailed error information

## 📚 Additional Resources

- [Setup Guide](README_SETUP.md) - Detailed configuration instructions
- [API Documentation](docs/api.md) - Module documentation (if available)
- [Examples](examples/) - Usage examples and scripts (if available)

---

**Version**: 2.0  
**Last Updated**: December 2024  
**Python Compatibility**: 3.8+  
**Database Support**: Oracle 11g+