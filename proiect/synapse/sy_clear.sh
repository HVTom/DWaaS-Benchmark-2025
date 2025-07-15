#!/bin/bash

# Usage: ./sy_clear.sh [raw] [processed]

SYNAPSE_WORKSPACE="dwbenchmarkworkspace"
SQL_POOL="dwbenchmarkdedicatedpool"
SERVER_NAME="${SYNAPSE_WORKSPACE}.sql.azuresynapse.net"
DATABASE="$SQL_POOL"
SQL_USER="sqladminuser"
SQL_PASS="j7A+2,;}xVm=p&5"

SCHEMA_RAW=$1
SCHEMA_PROCESSED=$2



cleanup_schema() {
    local schema=$1
    echo "Cleaning all tables from schema: $schema"

    # list all the tables inside schema
    tables=$(sqlcmd -I -S "$SERVER_NAME" -d "$DATABASE" -U "$SQL_USER" -P "$SQL_PASS" -h -1 -W -Q "
SELECT name
FROM sys.tables
WHERE schema_id = SCHEMA_ID('$schema')" | tr -d ' ' | grep -v '^$')

    if [ -z "$tables" ]; then
        echo "Nothing to delete in schema $schema"
        return 0
    fi

    for table in $tables; do
        if [ -n "$table" ]; then
            echo "Truncating table: $schema.$table"
            sqlcmd -I -S "$SERVER_NAME" -d "$DATABASE" -U "$SQL_USER" -P "$SQL_PASS" -Q "TRUNCATE TABLE [$schema].[$table]"
            if [ $? -eq 0 ]; then
                echo "Table $table truncated successfully"
            else
                echo "Failed truncating table $table"
            fi
        fi
    done
}

echo "=== AZURE SYNAPSE SCHEMA CLEANUP ==="

if [ -n "$SCHEMA_RAW" ] && [ -n "$SCHEMA_PROCESSED" ]; then
    echo "Cleaning both schemas: $SCHEMA_RAW and $SCHEMA_PROCESSED"
    cleanup_schema "$SCHEMA_RAW"
    cleanup_schema "$SCHEMA_PROCESSED"
elif [ -n "$SCHEMA_RAW" ] && [ -z "$SCHEMA_PROCESSED" ]; then
    echo "Cleaning only raw schema: $SCHEMA_RAW"
    cleanup_schema "$SCHEMA_RAW"
elif [ -z "$SCHEMA_RAW" ] && [ -n "$SCHEMA_PROCESSED" ]; then
    echo "Cleaning only processed schema: $SCHEMA_PROCESSED"
    cleanup_schema "$SCHEMA_PROCESSED"
else
    echo "No schemas specified for cleanup or wrong args"
    echo "Usage: ./sy_clear.sh [raw] [processed]"
    exit 1
fi

echo -e "\n=== CLEANUP COMPLETED ==="
