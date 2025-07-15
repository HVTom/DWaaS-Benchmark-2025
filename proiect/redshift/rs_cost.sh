#!/bin/bash

REDSHIFT_CLUSTER="redshift-benchmark-cluster-1"
REDSHIFT_HOST="${REDSHIFT_CLUSTER}.csag37dwinrq.eu-central-1.redshift.amazonaws.com"
REDSHIFT_PORT="5439"
REDSHIFT_DB="benchmark"
REDSHIFT_USER="awsuser"
export PGPASSWORD='?Gn+3&t&9}vS!:)'
export PAGER=""

# pricing provisioned - per node per hour
NODE_TYPE="dc2.large"
NODES_COUNT=1  
HOURLY_RATE_PER_NODE=0.25  # ~$0.32/hour for eu-central-1
# cost = execution_hours * 1 node * $0.32

START_TIMESTAMP=$1
END_TIMESTAMP=$2
OPERATION_TYPE=$3
DATASET=$4
NUM_JOBS=${5:-10}
DATASET_SIZE=$6


echo -e "EU Central region costs:\n"

# similar to metrics, we need to look for COPY labeled jobs
calculate_load_cost() {
    psql -h ${REDSHIFT_HOST} -p ${REDSHIFT_PORT} -d ${REDSHIFT_DB} -U ${REDSHIFT_USER} -t -A -F',' -c "
    SET enable_result_cache_for_session TO off;
    WITH load_data AS (
        SELECT
            q.query,
            -- apply the mnimum 60s per query
            --GREATEST(EXTRACT(EPOCH FROM (q.endtime - q.starttime)), 60) / 3600.0 as execution_hours,
            EXTRACT(EPOCH FROM (q.endtime - q.starttime)) / 3600.0 as execution_hours,
            COALESCE(fs.bytes, 0) as bytes_processed
        FROM STL_QUERY q
        LEFT JOIN STL_FILE_SCAN fs ON q.query = fs.query
        WHERE q.starttime BETWEEN '${START_TIMESTAMP}' AND '${END_TIMESTAMP}'
        AND q.querytxt ILIKE '%COPY%'
        AND q.querytxt ILIKE '%${DATASET}%'
        AND q.aborted = 0
    )
    SELECT
        ROUND(SUM(bytes_processed)/1024.0/1024.0, 2) AS MB_processed,
        COUNT(*) AS total_operations,
        ROUND(
            SUM(execution_hours) * ${NODES_COUNT} * ${HOURLY_RATE_PER_NODE}, 4
        ) as estimated_cost_usd
    FROM load_data;
    "
}


# QUERY and TRANSFORM go together here
# we look for SELECT and CREATE operations 
calculate_query_cost() {
    psql -h ${REDSHIFT_HOST} -p ${REDSHIFT_PORT} -d ${REDSHIFT_DB} -U ${REDSHIFT_USER} -t -A -F',' -c "
    SET enable_result_cache_for_session TO off;
    WITH query_data AS (
        SELECT
            q.query,
            --EXTRACT(EPOCH FROM (q.endtime - q.starttime)) / 3600.0 as execution_hours,
            --GREATEST(EXTRACT(EPOCH FROM (q.endtime - q.starttime)), 60) / 3600.0 as execution_hours,
            EXTRACT(EPOCH FROM (q.endtime - q.starttime)) / 3600.0 as execution_hours,
            COALESCE(qm.query_scan_size * 1024 * 1024, sc.bytes_scanned, 0) as bytes_processed
        FROM STL_QUERY q
        LEFT JOIN STL_QUERY_METRICS qm ON q.query = qm.query AND qm.segment = -1
        LEFT JOIN (
            SELECT query, SUM(COALESCE(bytes, 0)) as bytes_scanned
            FROM STL_SCAN
            GROUP BY query
        ) sc ON q.query = sc.query
        WHERE q.starttime BETWEEN '${START_TIMESTAMP}' AND '${END_TIMESTAMP}'
        AND (q.querytxt ILIKE '%SELECT%' OR q.querytxt ILIKE '%CREATE%' OR q.querytxt ILIKE '%DROP%')
        AND q.querytxt NOT ILIKE '%stl_%'
        AND q.querytxt NOT ILIKE '%svl_%'
        AND q.userid = (SELECT usesysid FROM pg_user WHERE usename = '${REDSHIFT_USER}')
        AND q.aborted = 0
    )
    SELECT
        ROUND(SUM(bytes_processed)/1024.0/1024.0, 2) AS MB_processed,
        COUNT(*) AS total_operations,
        ROUND(
            SUM(execution_hours) * ${NODES_COUNT} * ${HOURLY_RATE_PER_NODE}, 4
        ) as estimated_cost_usd
    FROM query_data;
    "
}


# main
case "$OPERATION_TYPE" in
    "LOAD")
        cost_result=$(calculate_load_cost)
        ;;
    "QUERY"|"TRANSFORM")
        cost_result=$(calculate_query_cost)
        ;;
    *)
        echo "Error: OPERATION_TYPE must be 'LOAD', 'QUERY', or 'TRANSFORM'"
        exit 1
        ;;
esac

# table alignment
{
    echo "mb_processed,total_operations,estimated_cost_usd"
    echo "$cost_result" | tail -n +2
} | column -t -s,

unset PGPASSWORD


echo "Writing cost summary to CSV..."
{  
    ts=$(date -u +"%Y-%m-%dT%H:%M:%S")
    #echo "$cost_result" | awk -F',' -v size="$DATASET_SIZE" -v op="$OPERATION_TYPE" -v ts="$ts" 'NR>1 {
    echo "$cost_result" | tail -n1 | awk -F',' -v size="$DATASET_SIZE" -v op="$OPERATION_TYPE" -v ts="$ts" '{
        printf "Redshift,%s,%s_COST,SUMMARY,0.0,0,%.2f,0,0,TOTAL,%.4f,%s\n",
               size, op, $1, $3, ts
    }' >> rs_results.csv
}


