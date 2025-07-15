#!/bin/bash

START_TIMESTAMP=$1
END_TIMESTAMP=$2
OPERATION_TYPE=$3
DATASET=$4
NUM_JOBS=$5

# dataset condition check 
DATASET_CONDITION=""
if [ -n "$DATASET" ]; then
    DATASET_CONDITION="AND destination_table.dataset_id = '$DATASET'"
fi

# check job type, with column adaptation/dynamic params
if [ "$OPERATION_TYPE" = "QUERY" ]; then
    # only for QUERY jobs - no slot usage column, keep it 0 for csv column alignment
    bq query --format=csv --use_legacy_sql=false --nouse_cache "
    SELECT
    job_id,
    TIMESTAMP_DIFF(end_time, start_time, MILLISECOND)/1000 AS duration_seconds,
    total_bytes_processed/1024/1024 AS MB_processed,
    cache_hit,
    destination_table.table_id,
    creation_time
    FROM
    \`region-eu\`.INFORMATION_SCHEMA.JOBS
    WHERE
    creation_time BETWEEN TIMESTAMP('$START_TIMESTAMP') AND TIMESTAMP('$END_TIMESTAMP') -- here we used the passed timestamps to narrow down results
    AND job_type = '$OPERATION_TYPE'
    $DATASET_CONDITION
    ORDER BY creation_time ASC
    LIMIT $NUM_JOBS" # also  for narrowing down jobs
else
    # LOAD and TRANSFORMS - these have attached slot % usage
    bq query --format=csv --use_legacy_sql=false --nouse_cache "
    SELECT
    job_id,
    TIMESTAMP_DIFF(end_time, start_time, MILLISECOND)/1000 AS duration_seconds,
    total_bytes_processed/1024/1024 AS MB_processed,
    total_slot_ms,
    SAFE_DIVIDE(total_slot_ms, TIMESTAMP_DIFF(end_time, start_time, MILLISECOND)) AS avg_slot_utilization,
    destination_table.table_id,
    creation_time
    FROM
    \`region-eu\`.INFORMATION_SCHEMA.JOBS
    WHERE
    creation_time BETWEEN TIMESTAMP('$START_TIMESTAMP') AND TIMESTAMP('$END_TIMESTAMP')
    AND job_type = '$OPERATION_TYPE'
    $DATASET_CONDITION
    ORDER BY creation_time ASC
    LIMIT $NUM_JOBS"
fi
