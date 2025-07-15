#!/bin/bash

# Define bucket and dataset
BUCKET="synthea-benchmark-bucket"
RAW_PATH="raw"
FOLDER_NAME=$1
DATASET="dataset_raw"
DATASET_SIZE=$2

# Redshift connection details
REDSHIFT_HOST="redshift-benchmark-cluster-1.csag37dwinrq.eu-central-1.redshift.amazonaws.com"
REDSHIFT_DB="benchmark"
REDSHIFT_USER="awsuser"
REDSHIFT_PORT="5439"
IAM_ROLE="arn:aws:iam::887137766384:role/service-role/AmazonRedshift-CommandsAccessRole-20250528T133054"

export PGPASSWORD='?Gn+3&t&9}vS!:)'
export PAGER=""

# Get all CSV files in the bucket/folder
CSV_FILES=$(aws s3 ls s3://${BUCKET}/${RAW_PATH}/${FOLDER_NAME}/ --recursive | grep "\.csv$" | awk '{print $4}'; echo "")

# Count number of files to process
NUM_FILES=$(echo "$CSV_FILES" | wc -l)

# Summary variables
TOTAL_TIME=0
TOTAL_FILES=0
TOTAL_SIZE=$(aws s3 ls s3://${BUCKET}/${RAW_PATH}/${FOLDER_NAME}/ --recursive --summarize | grep "Total Size" | awk '{print $3}')

# Record start timestamp for later querying
START_TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%S")

echo -e "Starting file ingestion for ${FOLDER_NAME} ...\n"
echo "=============================================="
echo "Dataset location: s3://${BUCKET}/${RAW_PATH}/${FOLDER_NAME}/"
echo "Total dataset size: ${TOTAL_SIZE} bytes"
echo "Number of files to ingest: $NUM_FILES"
#echo "Schema detection: Header-based (VARCHAR 500)"
echo -e "==============================================\n"

psql -h $REDSHIFT_HOST -p $REDSHIFT_PORT -U $REDSHIFT_USER -d $REDSHIFT_DB -c "SET enable_result_cache_for_session TO off; CREATE SCHEMA IF NOT EXISTS ${DATASET};" 2>/dev/null

for file_path in $CSV_FILES; do
    filename=$(basename "$file_path" .csv)
    file_size=$(aws s3 ls s3://${BUCKET}/${file_path} | awk '{print $3}')
    
    echo "Processing: $filename ($file_size bytes)"
    START_TIME=$(date +%s)
    
    echo " Reading header..."
    HEADER=$(aws s3 cp s3://${BUCKET}/${file_path} - | head -1)

    echo " Generating schema from array..."
    # Parse header into array using IFS
    IFS=',' read -ra HEADER_ARRAY <<< "$HEADER"

    # Build columns string using array
    # redshift does not have auto convesrion from csv to DB table
    # so we need to process the header manually with bash
    # it uses an array to store the strings
    # then the column names are sanitized dso they don't break the table
    COLUMNS=""
    for i in "${!HEADER_ARRAY[@]}"; do
        # Clean column name
        clean_col=$(echo "${HEADER_ARRAY[$i]}" | sed 's/[^a-zA-Z0-9_]/_/g' | sed 's/^[0-9]/_&/')
    
        if [ $i -eq 0 ]; then
            COLUMNS="\"$clean_col\" VARCHAR(500)"
        else
            COLUMNS="$COLUMNS, \"$clean_col\" VARCHAR(500)"
        fi
    done

    NUM_COLUMNS=${#HEADER_ARRAY[@]}
    echo " Detected $NUM_COLUMNS columns (all VARCHAR 500)"
    
    echo " Creating table and loading data..."   
    # create table + upload data
    echo "  Creating table and loading data..."
    psql -h $REDSHIFT_HOST -p $REDSHIFT_PORT -U $REDSHIFT_USER -d $REDSHIFT_DB << EOF
DROP TABLE IF EXISTS ${DATASET}.${filename};
CREATE TABLE ${DATASET}.${filename} (
${COLUMNS}
);

COPY ${DATASET}.${filename}
FROM 's3://${BUCKET}/${file_path}'
IAM_ROLE '${IAM_ROLE}'
CSV
IGNOREHEADER 1
FILLRECORD
ACCEPTINVCHARS
BLANKSASNULL
EMPTYASNULL
EOF

    exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
        echo "SUCCESS"
        TOTAL_FILES=$((TOTAL_FILES + 1))
    else
        echo "FAILED"   
    fi


    END_TIME=$(date +%s)
    INGESTION_TIME=$((END_TIME-START_TIME))
    
    if [ $exit_code -eq 0 ]; then
        echo "COMPLETED in $INGESTION_TIME seconds"
        TOTAL_TIME=$((TOTAL_TIME + INGESTION_TIME))
        TOTAL_FILES=$((TOTAL_FILES + 1))
    else
        echo "FAILED in $INGESTION_TIME seconds"
        # Show recent errors
        echo "  Recent errors:"
        psql -h $REDSHIFT_HOST -p $REDSHIFT_PORT -U $REDSHIFT_USER -d $REDSHIFT_DB -c "
            SELECT '    ' || err_reason 
            FROM stl_load_errors 
            WHERE starttime > CURRENT_TIMESTAMP - INTERVAL '2 minutes' 
            ORDER BY starttime DESC 
            LIMIT 3;" -t
    fi
    
    echo ""
done

echo -e "==============================================\n"
echo -e "INGESTION SUMMARY for ${FOLDER_NAME}\n"
echo "Files attempted: $NUM_FILES"
echo "Files successfully loaded: $TOTAL_FILES"
echo "Files failed: $(($NUM_FILES - $TOTAL_FILES))"
echo "Total processing time: $TOTAL_TIME seconds"
echo "Schema method: Header-based detection (VARCHAR 500)"


sleep  5
END_TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%S")

echo -e "\n=================== INGESTION JOB TIMES ==========================="
echo "Retrieving actual job execution times from Redshift..."

if [ -f "./rs_metrics.sh" ]; then
    actual_times=$(bash ./rs_metrics.sh "$START_TIMESTAMP" "$END_TIMESTAMP" "LOAD" "$DATASET" "$NUM_FILES" "$DATASET_SIZE")
    echo "$actual_times" | column -t -s,
    
    # calculate totals
    total_rs_time=$(echo "$actual_times" | awk -F',' 'NR>1 {sum+=$2} END {print sum+0}')
    successful_jobs=$(echo "$actual_times" | awk -F',' 'NR>1 {count++} END {print count+0}')
else
    echo "rs_metrics.sh not found, skipping detailed metrics"
    total_rs_time=0
    successful_jobs=$TOTAL_FILES
fi

echo -e "\n=================== INGESTION METRICS ==========================="
echo "Files attempted: $NUM_FILES"
echo "Files successfully ingested: $successful_jobs"
echo "Files failed: $(($NUM_FILES - $successful_jobs))"
echo "Total script time: $TOTAL_TIME seconds"
echo "Total Redshift jobs processing time: $total_rs_time seconds"
echo "Script overhead: $(echo "$TOTAL_TIME - $total_rs_time" | bc 2>/dev/null || echo "N/A") seconds"


# prepare file header
if [ -z "$(cat rs_results.csv)" ]; then
    echo "platform,dataset_size,operation_type,job_id,duration_seconds,rows_processed,mb_processed,total_exec_time,slot_count,table_name,cost_usd,timestamp" > rs_results.csv
fi

echo -e "\n=================== INGESTION COST ==========================="
if [ -f "./rs_cost.sh" ]; then
    echo -e "Calling rs_cost.sh...\n"
    bash ./rs_cost.sh "$START_TIMESTAMP" "$END_TIMESTAMP" "LOAD" "$DATASET" "$NUM_FILES" "$DATASET_SIZE"
else
    echo "rs_cost.sh not found, skipping cost calculation"
fi

echo -e "\n=== INGESTION COMPLETED ==="

# only the job metrics
echo "Writing ingestion results to CSV..."
{
    ts=$(date -u +"%Y-%m-%d %H:%M:%S")
    echo "$actual_times" | awk -F',' -v size="$DATASET_SIZE" -v ts="$ts" '
    BEGIN {OFS=","}
    NR>1 {
        slot_count = int($6)  
        print "Redshift", size, "LOAD", $1, $2, $3, $4, $5, slot_count, $7, "0.0", ts
    }' >> rs_results.csv
}
