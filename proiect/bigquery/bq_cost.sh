#!/bin/bash

START_TIMESTAMP=$1
END_TIMESTAMP=$2
OPERATION_TYPE=$3
DATASET=$4
NUM_JOBS=$5
DATASET_SIZE=$6 

echo -e "EU region costs:\n"

DATASET_CONDITION=""
if [ -n "$DATASET" ]; then
    DATASET_CONDITION="AND destination_table.dataset_id = '$DATASET'"
fi

#uses information schema, same as fot the metrics
cost_result=$(bq query --format=csv --use_legacy_sql=false --nouse_cache "
WITH relevant_jobs AS (
  SELECT total_bytes_processed
  FROM \`region-eu\`.INFORMATION_SCHEMA.JOBS
  WHERE creation_time BETWEEN TIMESTAMP('$START_TIMESTAMP')
    AND TIMESTAMP('$END_TIMESTAMP')
    AND job_type = '$OPERATION_TYPE'
    $DATASET_CONDITION
  ORDER BY creation_time ASC
  LIMIT $NUM_JOBS
)
SELECT
  SUM(total_bytes_processed)/1024/1024 AS MB_processed,
  FORMAT('%.6f', SUM(total_bytes_processed)/POWER(10,12)*6.00) AS estimated_cost_usd
FROM relevant_jobs
")

echo "$cost_result" | column -t -s,

# redirecting:
echo "Writing cost summary to CSV..."
{
    ts=$(date -u +"%Y-%m-%d %H:%M:%S")
    echo "$cost_result" | awk -F',' -v size="$DATASET_SIZE" -v op="$OPERATION_TYPE" -v ts="$ts" 'NR>1 {
    printf "BigQuery,%s,%s_COST,SUMMARY,0.0,%s,0.0,0.0,TOTAL,%s,%s\n", 
           size, op, $1, $2, ts
    }'
} >> bq_results.csv
