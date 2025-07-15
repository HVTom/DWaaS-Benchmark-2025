#!/bin/bash

STORAGE_ACCOUNT="dwbenchmarkstorage"
FILE_SYSTEM="dwbenchmarkfilesystem"
FOLDER_PATH=$1

# Dedicated SQL Pool info
SYNAPSE_WORKSPACE="dwbenchmarkworkspace"
SQL_POOL="dwbenchmarkdedicatedpool"
SERVER_NAME="${SYNAPSE_WORKSPACE}.sql.azuresynapse.net"
DATABASE="$SQL_POOL"

# variables for metrics
START_TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%S")
OPERATION_TYPE="LOAD"
TOTAL_TIME=0
SUCCESSFUL_FILES=0
FAILED_FILES=0
DATASET_SIZE=$2

# DB credentials
SQL_USER="sqladminuser"
SQL_PASS="j7A+2,;}xVm=p&5"

if [ $# -eq 0 ]; then
    echo "Usage: bash sy_ingestion.sh <dataset_name>"
    exit 1
fi

# calculation of dataset size usign azure cli commands
echo "Calculating dataset size..."
TOTAL_SIZE_BYTES=$(az storage fs file list \
    --account-name "$STORAGE_ACCOUNT" \
    --file-system "$FILE_SYSTEM" \
    --path "$FOLDER_PATH" \
    --auth-mode login \
    --recursive \
    --query "[?ends_with(name, '.csv')].contentLength" \
    --output tsv | awk '{sum+=$1} END {print sum}')

HUMAN_READABLE_SIZE=$(numfmt --to=iec-i --suffix=B --format="%.2f" $TOTAL_SIZE_BYTES)

CSV_FILES=$(az storage fs file list \
    --account-name "$STORAGE_ACCOUNT" \
    --file-system "$FILE_SYSTEM" \
    --path "$FOLDER_PATH" \
    --auth-mode login \
    --output tsv \
    --query "[?ends_with(name, '.csv')].name")

NUM_FILES=$(echo "$CSV_FILES" | wc -l)

echo "=============================================="
echo "Dataset name: $1"
echo "Storage path: abfs://${FILE_SYSTEM}@${STORAGE_ACCOUNT}.dfs.core.windows.net/${FOLDER_PATH}/"
echo "Number of CSV files: $NUM_FILES"
echo "Total dataset size: $HUMAN_READABLE_SIZE"
echo "=============================================="

# Create the raw schema if it doesn't exist
echo "Creating raw schema if not exists..."
sqlcmd -S "$SERVER_NAME" -d "$DATABASE" -U "$SQL_USER" -P "$SQL_PASS" -I -Q "
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'raw')
BEGIN
    EXEC('CREATE SCHEMA raw')
END"



for CSV_FILE in $CSV_FILES; do
    TABLE_NAME=$(basename "$CSV_FILE" .csv)
    echo "Processing $TABLE_NAME..."
    
    # Start timing individual file
    START_FILE_TIME=$(date +%s)
    
    # header extraction
    az storage fs file download \
        --account-name "$STORAGE_ACCOUNT" \
        --file-system "$FILE_SYSTEM" \
        --path "$CSV_FILE" \
        --auth-mode login \
        --destination /tmp/temp_header.csv \
        --overwrite > /dev/null 2>&1
    
    HEADER_ROW=$(head -1 /tmp/temp_header.csv)
    IFS=',' read -ra HEADER_ARRAY <<< "$HEADER_ROW"
    
    # same as with Redshift, theres no auto convertion from csv to db table
    # we extract again the columns in to an array
    # and clean the column names so it doesnt brake the table creation or data ingestion
    COLS=()
    for col in "${HEADER_ARRAY[@]}"; do
        CLEAN_COL=$(echo "$col" | tr -d '"' | tr ' ' '_' | sed -E 's/[^a-zA-Z0-9_]/_/g')
        COLS+=("[$CLEAN_COL] NVARCHAR(4000)") # used a generic data type to accomodate all the ingested data
    done
    
    COLUMN_DEFS=$(IFS=', '; echo "${COLS[*]}")

    # clean existing tables (if necessary)
    echo "Cleaning existing tables for $TABLE_NAME..."
    sqlcmd -S "$SERVER_NAME" -d "$DATABASE" -U "$SQL_USER" -P "$SQL_PASS" -I -Q "
    IF EXISTS (SELECT * FROM sys.tables WHERE name = '$TABLE_NAME' AND schema_id = SCHEMA_ID('raw'))
        DROP TABLE raw.[$TABLE_NAME]
    
    IF EXISTS (SELECT * FROM sys.tables WHERE name = '${TABLE_NAME}_new' AND schema_id = SCHEMA_ID('raw'))
        DROP TABLE raw.[${TABLE_NAME}_new]"

    # explicit table creation 
    echo "Creating table raw.[$TABLE_NAME]..."
    sqlcmd -S "$SERVER_NAME" -d "$DATABASE" -U "$SQL_USER" -P "$SQL_PASS" -I -Q "
    CREATE TABLE raw.[$TABLE_NAME] (
        $COLUMN_DEFS
    ) WITH (HEAP)"

    # COPY INTO with explicit option to prevent auto creation 
    # targte table must exist prior to ingestion
    echo "Loading data into $TABLE_NAME..."
    COPY_RESULT=$(sqlcmd -S "$SERVER_NAME" -d "$DATABASE" -U "$SQL_USER" -P "$SQL_PASS" -I -Q "
    COPY INTO raw.[$TABLE_NAME]
    FROM 'https://${STORAGE_ACCOUNT}.dfs.core.windows.net/${FILE_SYSTEM}/${CSV_FILE}'
    WITH (
        CREDENTIAL = (IDENTITY = 'Managed Identity'),
        FILE_TYPE = 'CSV',
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0A',
        AUTO_CREATE_TABLE = 'OFF' 
    )" 2>&1)


    END_FILE_TIME=$(date +%s)
    FILE_TIME=$((END_FILE_TIME-START_FILE_TIME))
    TOTAL_TIME=$((TOTAL_TIME + FILE_TIME))

    if echo "$COPY_RESULT" | grep -q -E "(Error|Failed|Msg [0-9]+.*Level [0-9]+.*State [0-9]+)"; then
        echo "FAILED to load $TABLE_NAME in $FILE_TIME seconds"
        echo "$COPY_RESULT"
        FAILED_FILES=$((FAILED_FILES + 1))
    else
        echo "Successfully loaded $TABLE_NAME in $FILE_TIME seconds"
        SUCCESSFUL_FILES=$((SUCCESSFUL_FILES + 1))
    fi
    
    rm -f /tmp/temp_header.csv
done

END_TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%S")

echo "=============================================="
echo "Checking raw schema table creation:"
sqlcmd -S "$SERVER_NAME" -d "$DATABASE" -U "$SQL_USER" -P "$SQL_PASS" \
       -I -W -s "|" -Q "
SET NOCOUNT ON;
SELECT 
    t.name AS table_name,
    SUM(p.rows) AS row_count,
    s.name AS schema_name
FROM sys.tables t
INNER JOIN sys.partitions p ON t.object_id = p.object_id
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE t.schema_id = SCHEMA_ID('raw') AND p.index_id IN (0,1)
GROUP BY t.name, s.name
ORDER BY t.name" | grep -v "^(" | grep -v "rows affected" | grep -v "^$" | column -t -s "|"

echo ""
echo "=============================================="


# capture row counts from table verufication
echo "Getting row counts for metrics integration..."
ROW_COUNTS_OUTPUT=$(sqlcmd -S "$SERVER_NAME" -d "$DATABASE" -U "$SQL_USER" -P "$SQL_PASS" -I -h -1 -Q "
SELECT 
    t.name AS table_name,
    SUM(p.rows) AS row_count
FROM sys.tables t
INNER JOIN sys.partitions p ON t.object_id = p.object_id
WHERE t.schema_id = SCHEMA_ID('raw') AND p.index_id IN (0,1)
GROUP BY t.name" 2>/dev/null)

# associative array with row counts
declare -A row_counts
while read -r table rows; do
    table=$(echo "$table" | xargs | tr -d '\r\n')
    rows=$(echo "$rows" | xargs | tr -d '\r\n')
    
    if [[ -n "$table" && "$rows" =~ ^[0-9]+$ ]]; then
        row_counts["$table"]=$rows
    fi
done <<< "$ROW_COUNTS_OUTPUT"


echo ""
echo "=================== INGESTION JOB TIMES =========================="
echo "Retrieving actual job execution times from Synapse..."

if [ -f "./sy_metrics.sh" ]; then
    metrics_data=$(bash ./sy_metrics.sh "$START_TIMESTAMP" "$END_TIMESTAMP" "$OPERATION_TYPE" "raw" "$NUM_FILES" "$DATASET_SIZE")
    
    # integrate row counts inside the metrics table
    {
        echo "$metrics_data" | head -n1
        echo "$metrics_data" | tail -n +2 | awk -F',' -v OFS=',' \
            'BEGIN {
                # populate array with row counts
                split("'"$(for table in "${!row_counts[@]}"; do echo "$table:${row_counts[$table]}"; done | tr '\n' ' ')"'", arr, " ")
                for (i in arr) {
                    if (split(arr[i], kv, ":") == 2) {
                        counts[kv[1]] = kv[2]
                    }
                }
            }
            {
                # table name extraction from 5th column (table_name)
                table_name = $5
                if (table_name in counts) {
                    $3 = counts[table_name]
                }
                print
            }'
    } | column -t -s, -o '   '
    
    total_ingestion_time=$(echo "$metrics_data" | awk -F',' 'NR>1 {sum+=$2} END {print sum+0}')
    successful_jobs=$(echo "$metrics_data" | awk -F',' 'NR>1 {count++} END {print count+0}')
else
    echo "sy_metrics.sh not found, using script timing"
    total_ingestion_time=$TOTAL_TIME
    successful_jobs=$SUCCESSFUL_FILES
fi

echo ""
echo "=================== INGESTION METRICS =========================="
echo "Files attempted: $NUM_FILES"
echo "Files successfully ingested: $SUCCESSFUL_FILES"
echo "Files failed: $FAILED_FILES"
echo "Dataset size: $HUMAN_READABLE_SIZE"
echo "Total script time: $TOTAL_TIME seconds"
echo "Total Synapse jobs processing time: $total_ingestion_time seconds"
if [ $TOTAL_TIME -gt 0 ] && [ $total_ingestion_time -gt 0 ]; then
    echo "Script overhead: $(echo "$TOTAL_TIME - $total_ingestion_time" | bc 2>/dev/null || echo "N/A") seconds"
fi


# write header 
if [ -z "$(cat sy_results.csv)" ]; then
    echo "platform,dataset_size,operation_type,job_id,duration_seconds,rows_processed,mb_processed,total_exec_time,slot_count,table_name,cost_usd,timestamp" > sy_results.csv
fi

echo ""
echo "=================== INGESTION COST ==========================="
echo "Calling sy_cost.sh..."
if [ -f "./sy_cost.sh" ]; then
    bash ./sy_cost.sh "$START_TIMESTAMP" "$END_TIMESTAMP" "$OPERATION_TYPE" "raw" "$NUM_FILES" "$DATASET_SIZE"
else
    echo "sy_cost.sh not found, skipping cost calculation"
fi

echo ""
echo "=== SYNAPSE INGESTION COMPLETED ==="

ts=$(date -u +"%Y-%m-%d %H:%M:%S")
echo "$metrics_data" | awk -F',' -v ds="$DATASET_SIZE" -v ts="$ts" '
BEGIN {OFS=","}
NR>1 {
    print "Synapse", ds, "LOAD", $1, $2, $3, $4, "", $5, $6, ts
}' >> sy_results.csv

