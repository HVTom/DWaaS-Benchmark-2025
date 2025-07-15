#!/bin/bash

BUCKET="pipeline-v1-storage-bucket"
RAW_PATH="raw"
FOLDER_NAME=$1
DATASET_SIZE=${2:-"tiny"}  # second argument!! "tiny" default
DATASET="synthea_raw"
OPERATION_TYPE='LOAD'

# get ALL CSV files in the bucket/folder
CSV_FILES=$(gsutil ls gs://${BUCKET}/${RAW_PATH}/${FOLDER_NAME}/*.csv)

# count number of files to process
NUM_FILES=$(echo "$CSV_FILES" | wc -l)

# summary variables
TOTAL_TIME=0
TOTAL_FILES=0
TOTAL_SIZE=$(gsutil du -sh gs://${BUCKET}/${RAW_PATH}/${FOLDER_NAME}/ | awk '{print $1}')

# script start timestamp for metrics, cost
START_TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%S")

echo -e "Starting file ingestion for ${FOLDER_NAME} ...\n"
echo "=============================================="
echo "Dataset location: gs://${BUCKET}/${RAW_PATH}/${FOLDER_NAME}/"
echo "Total dataset size: ${TOTAL_SIZE}"
echo "Number of files to ingest: $NUM_FILES"
echo -e "==============================================\n"

for file_path in $CSV_FILES; do
    filename=$(basename "$file_path" .csv)
        
    # get file info
    file_info=$(gsutil ls -l "$file_path" | grep -v "objects")
    file_size_bytes=$(echo "$file_info" | awk 'NR==1{print $1}')
    
    TOTAL_SIZE_BYTES=$((TOTAL_SIZE_BYTES + file_size_bytes))
    
    # display file info
    echo "File information:"
    echo "$file_info" | awk '{
        if (NR > 0 && !match($0, /objects,/)) {
            size = $1;
            # Convert bytes to human readable format
            if (size < 1024) {
                unit = "B";
            } else if (size < 1048576) {
                size = size/1024;
                unit = "KB";
            } else if (size < 1073741824) {
                size = size/1048576;
                unit = "MB";
            } else {
                size = size/1073741824;
                unit = "GB";
            }
            
            # Extract filename from path
            split($3, path, "/");
            file = path[length(path)];
            
            printf "  %s - %.2f %s\n", file, size, unit;
        }
    }'
    
    
    
    # start timing per file
    START_TIME=$(date +%s)
    
    bq load \
      --replace \
      --autodetect \
      --source_format=CSV \
      ${DATASET}.${filename} \
      ${file_path}
     
    exit_code=$?

    # end timing
    END_TIME=$(date +%s)
    INGESTION_TIME=$((END_TIME-START_TIME))
 
    if [ $exit_code -eq 0 ]; then
        echo "SUCCESS: $filename loaded in $INGESTION_TIME seconds"
        TOTAL_TIME=$((TOTAL_TIME + INGESTION_TIME))
        TOTAL_FILES=$((TOTAL_FILES + 1)) 
    else
        echo "FAILED: $filename failed to load (exit code: $exit_code) in $INGESTION_TIME seconds"
    fi
done

END_TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%S")


echo -e "==============================================\n"
echo -e "SUMMARY for ${FOLDER_NAME} ...\n"



echo -e "\n=================== INGESTION JOB TIMES ==========================="
echo "Retrieving actual job execution times from BigQuery..."


actual_times=$(./bq_metrics.sh "$START_TIMESTAMP" "$END_TIMESTAMP" "$OPERATION_TYPE" "$DATASET" "$NUM_FILES")

# job times tabular format
echo "$actual_times" | column -t -s,

# awk pipe to extract and compute values
total_bq_time=$(echo "$actual_times" | awk -F',' 'NR>1 {sum+=$2} END {print sum}')
successful_jobs=$(echo "$actual_times" | awk -F',' 'NR>1 {count++} END {print count+0}')


echo -e "\n=================== ROWS INGESTED PER TABLE ==================="
echo "Querying actual row counts from BigQuery tables..."
printf "%-25s %s\n" "table_name" "row_count"
echo "----------------------------------------"

# get rows to check succesful ingestion
for file_path in $CSV_FILES; do
    filename=$(basename "$file_path" .csv)
    row_count=$(bq query --use_legacy_sql=false --format=csv --nouse_cache --max_rows=1 "SELECT COUNT(*) FROM \`${DATASET}.${filename}\`" | tail -n 1)
    printf "%-25s %s\n" "$filename" "$row_count"
done




echo -e "\n=================== INGESTION METRICS ==========================="
echo "Files attempted: $NUM_FILES"
echo "Files successfully ingested: $successful_jobs"
echo "Files failed: $(($NUM_FILES - $successful_jobs))"
echo "Dataset size: $TOTAL_SIZE"
echo "Total script time: $TOTAL_TIME seconds"
echo "Total BigQuery jobs processing time: $total_bq_time seconds"
echo "Script overhead: $(echo "$TOTAL_TIME - $total_bq_time" | bc) seconds"


# csv header 
if [ -z "$(cat bq_results.csv)" ]; then
    echo "platform,dataset_size,operation_type,job_id,duration_seconds,mb_processed,total_slots_ms,avg_slot_utilization,table_id,row_count,timestamp" > bq_results.csv
fi 


echo -e "\n=================== INGESTION COST ==========================="
echo -e "Calling bq_cost.sh...\n"
sh ./bq_cost.sh "$START_TIMESTAMP" "$END_TIMESTAMP" "$OPERATION_TYPE" "$DATASET" "$NUM_FILES" "$DATASET_SIZE"

echo -e "\n=================== INGESTION CLEANUP ==========================="
echo -e "Calling clear_bq.sh...\n"
sh ./clear_bq.sh "$DATASET"

echo -e "\n=== INGESTION COMPLETED ==="


echo "Writing results to CSV..."
{   
    # temp file with table_id -> row_count mapping
    temp_file=$(mktemp)
    for file_path in $CSV_FILES; do
        filename=$(basename "$file_path" .csv)
        row_count=$(bq query --use_legacy_sql=false --format=csv --nouse_cache --max_rows=1 "SELECT COUNT(*) FROM \`${DATASET}.${filename}\`" | tail -n 1)
        echo "$filename,$row_count" >> "$temp_file"
    done
    
    # # ONLY redirect the jobs (metrics), cost.sh writes by himself
    echo "$actual_times" | awk -F',' -v size="$DATASET_SIZE" -v lookup_file="$temp_file" '
    BEGIN {
        while ((getline line < lookup_file) > 0) {
            split(line, parts, ",")
            row_counts[parts[1]] = parts[2]
        }
        close(lookup_file)
    }
    NR>1 {
        table_id = $6
        row_count = row_counts[table_id]
        if (row_count == "") row_count = "0"
        printf "BigQuery,%s,LOAD,%s,%s,%s,%s,%s,%s,%s,%s\n", 
               size, $1, $2, $3, $4, $5, table_id, row_count, $7
    }'
    
    rm -f "$temp_file"
} >> bq_results.csv
echo "Results appended to bq_results.csv"

