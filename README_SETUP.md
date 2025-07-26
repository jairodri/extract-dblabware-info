# Database Analysis Tool - Setup Guide

## Environment Configuration

### Quick Start

1. **Copy the environment template:**
   ```bash
   cp resources/.env.template resources/.env
   ```

2. **Edit the configuration file:**
   Open `resources/.env` and configure your settings.

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Run the application:**
   ```bash
   python main.py
   ```

## Configuration Sections

### 📁 Output Directories
```properties
OUTPUT_DIR_DATA=C:/your/output/path
DOCS_OUTPUT_DIR=docs
```

### 🔧 Extraction Settings
```properties
MAX_RECORDS_PER_TABLE=40000
TOTAL_RECORDS_LIMIT=400000
CSV_SEPARATOR=|
```

### 🗄️ Database Connections
Follow the naming pattern:
```
<ENVIRONMENT>_<LOCATION>_<VERSION>_<PARAMETER>
```

**Environments:**
- `DES` - Development
- `PRE` - Pre-production  
- `PRO` - Production

**Parameters:**
- `NAME` - Descriptive connection name
- `HOST` - Database server hostname
- `PORT` - Database port number
- `SERVICE_NAME` - Oracle service name
- `USER` - Database username
- `PASSWORD` - Database password
- `OWNER` - Schema owner

**Example:**
```properties
PRO_NYC_V8_NAME=Production_NewYork_V8
PRO_NYC_V8_HOST=prod-db.company.com
PRO_NYC_V8_PORT=1521
PRO_NYC_V8_SERVICE_NAME=PRODNY
PRO_NYC_V8_USER=app_user
PRO_NYC_V8_PASSWORD=secure_password
PRO_NYC_V8_OWNER=SCHEMA_OWNER
```

### 🔍 Schema Comparison Filters

**Include Patterns** (ALL must match):
```properties
SCHEMA_COMPARISON_INCLUDE_PATTERNS=PRO_,_V8
```

**Exclude Patterns** (NONE can match):
```properties
SCHEMA_COMPARISON_EXCLUDE_PATTERNS=_TEST_,_TEMP_
```

**Regex Pattern** (alternative to include/exclude):
```properties
SCHEMA_COMPARISON_REGEX_PATTERN=^PRO_[A-Z]{3}_V8$
```

### 📊 Log Parser
```properties
LOG_PARSER_INPUT_FILE=C:/path/to/integration.log
LOG_PARSER_OUTPUT_FILE=http_status_report.csv
```

## Security Notes

⚠️ **NEVER commit the `.env` file to version control!**

The `.env` file contains sensitive information like:
- Database passwords
- Server hostnames
- Internal network paths

Always use the `.env.template` file for documentation and sharing.

## File Structure

```
extract-dblabware-info/
├── resources/
│   ├── .env.template    # ✅ Safe to commit
│   └── .env            # ❌ Never commit (contains secrets)
├── modules/
├── main.py
└── README_SETUP.md
```

## Testing Your Configuration

1. **Test database connections:**
   - Run option 1 (Extract catalog metadata)
   - Verify connection to one of your databases

2. **Test output directories:**
   - Check that files are created in `OUTPUT_DIR_DATA`
   - Verify reports are generated in `docs/`

3. **Test comparisons:**
   - Run option 9 (Schema comparison)
   - Check filter patterns work correctly

## Troubleshooting

### Connection Issues
- Verify database server is accessible
- Check firewall settings
- Confirm Oracle client is installed

### Permission Issues
- Ensure user has read access to required tables
- Check schema ownership settings
- Verify password hasn't expired

### Output Issues
- Confirm output directories exist and are writable
- Check disk space availability
- Verify path separators for your OS

## Support

For setup assistance:
1. Check the application logs in `docs/`
2. Verify your `.env` configuration
3. Test individual database connections first