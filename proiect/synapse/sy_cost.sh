#!/bin/bash

START_TIMESTAMP=$1
END_TIMESTAMP=$2
OPERATION_TYPE=$3
SCHEMA=$4
NUM_JOBS=$5
DATASET_SIZE=$6

SYNAPSE_WORKSPACE="dwbenchmarkworkspace"
SQL_POOL="dwbenchmarkdedicatedpool"
SERVER_NAME="${SYNAPSE_WORKSPACE}.sql.azuresynapse.net"
DATABASE="$SQL_POOL"
SQL_USER="sqladminuser"
SQL_PASS="j7A+2,;}xVm=p&5"

echo -e "EU region costs (Azure Synapse):\n"

#uses sys.dm_pdw_exec_requests table
# measures resource usage and cost for data loading
# look for COPY INTO labeled operations
calculate_load_cost() {
    sqlcmd -S "$SERVER_NAME" -d "$DATABASE" -U "$SQL_USER" -P "$SQL_PASS" \
        -I -h -1 -W -Q "
    SET NOCOUNT ON;
    
    WITH ranked_requests AS (
        SELECT 
            DATEDIFF(SECOND, submit_time, end_time) as duration_seconds,
            total_elapsed_time/1000.0 as mb_processed,
            ROW_NUMBER() OVER (
                PARTITION BY request_id
                ORDER BY submit_time ASC
            ) as rn
        FROM sys.dm_pdw_exec_requests
        WHERE submit_time >= '$START_TIMESTAMP'
            AND submit_time <= '$END_TIMESTAMP'
            AND status IN ('Completed', 'Failed')
            AND command LIKE '%COPY INTO%'
            AND session_id != SESSION_ID()
    )
    SELECT 
        CAST(ROUND(SUM(mb_processed), 6) AS VARCHAR) + ',' +
        CAST(ROUND((SUM(duration_seconds) / 3600.0) * 1.20, 6) AS VARCHAR) as result
    FROM ranked_requests
    WHERE rn = 1;"
}


# uses sys.dm_pdw_exec_requests
# look for table names when pattern matching
calculate_query_cost() {
sqlcmd -S "$SERVER_NAME" -d "$DATABASE" -U "$SQL_USER" -P "$SQL_PASS" \
-I -h -1 -W -Q "
SET NOCOUNT ON;
WITH ranked_requests AS (
    SELECT
        DATEDIFF(SECOND, submit_time, end_time) as duration_seconds,
        total_elapsed_time/1000.0 as mb_processed,
        CASE
            WHEN COALESCE(command2, command) LIKE '%base_aggregation AS%'
                 AND COALESCE(command2, command) LIKE '%p.GENDER%' THEN 'patients_encounters_basic'
            WHEN COALESCE(command2, command) LIKE '%encounter_medication_matrix AS%'
                 AND COALESCE(command2, command) LIKE '%patient_complexity_score%' THEN 'encounters_medications_complex'
            WHEN COALESCE(command2, command) LIKE '%condition_sequences AS%'
                 AND COALESCE(command2, command) LIKE '%condition_networks%' THEN 'conditions_progression'
            WHEN COALESCE(command2, command) LIKE '%patient_observation_timeline AS%'
                 AND COALESCE(command2, command) LIKE '%total_observations%' THEN 'observations_patients_analysis'
            ELSE 'query_operation'
        END as table_name,
        ROW_NUMBER() OVER (
            PARTITION BY
            CASE
                WHEN COALESCE(command2, command) LIKE '%base_aggregation AS%'
                     AND COALESCE(command2, command) LIKE '%p.GENDER%' THEN 'patients_encounters_basic'
                WHEN COALESCE(command2, command) LIKE '%encounter_medication_matrix AS%'
                     AND COALESCE(command2, command) LIKE '%patient_complexity_score%' THEN 'encounters_medications_complex'
                WHEN COALESCE(command2, command) LIKE '%condition_sequences AS%'
                     AND COALESCE(command2, command) LIKE '%condition_networks%' THEN 'conditions_progression'
                WHEN COALESCE(command2, command) LIKE '%patient_observation_timeline AS%'
                     AND COALESCE(command2, command) LIKE '%total_observations%' THEN 'observations_patients_analysis'
                ELSE 'query_operation'
            END
            ORDER BY LEN(COALESCE(command2, command)) DESC, submit_time ASC
        ) as rn
    FROM sys.dm_pdw_exec_requests
    WHERE submit_time >= '$START_TIMESTAMP'
    AND submit_time <= '$END_TIMESTAMP'
    AND status IN ('Completed', 'Failed')
    -- WHERE clause for QUERY operations
    AND (
        COALESCE(command2, command) LIKE '%base_aggregation AS%' OR
        COALESCE(command2, command) LIKE '%encounter_medication_matrix AS%' OR
        COALESCE(command2, command) LIKE '%condition_sequences AS%' OR
        COALESCE(command2, command) LIKE '%patient_observation_timeline AS%'
    )
    AND session_id != SESSION_ID()
)
SELECT
    CAST(ROUND(SUM(mb_processed), 6) AS VARCHAR) + ',' +
    CAST(ROUND((SUM(duration_seconds) / 3600.0) * 1.20, 6) AS VARCHAR) as result
FROM ranked_requests
WHERE rn = 1;"
}

# uses sys.dm_pdw_exec_requests
# uses patttern matching for the created table names
calculate_transform_cost() {
sqlcmd -S "$SERVER_NAME" -d "$DATABASE" -U "$SQL_USER" -P "$SQL_PASS" \
-I -h -1 -W -Q "
SET NOCOUNT ON;
WITH ranked_requests AS (
    SELECT
        DATEDIFF(SECOND, submit_time, end_time) as duration_seconds,
        total_elapsed_time/1000.0 as mb_processed,
        CASE
            WHEN COALESCE(command2, command) LIKE '%WITH patient_cleaning AS%' THEN 'patients_clean'
            WHEN COALESCE(command2, command) LIKE '%WITH medication_normalization AS%' THEN 'medications_normalized'
            WHEN COALESCE(command2, command) LIKE '%WITH procedure_normalization AS%' THEN 'procedures_enriched'
            ELSE 'transform_operation'
        END as table_name,
        ROW_NUMBER() OVER (
            PARTITION BY
            CASE
                WHEN COALESCE(command2, command) LIKE '%WITH patient_cleaning AS%' THEN 'patients_clean'
                WHEN COALESCE(command2, command) LIKE '%WITH medication_normalization AS%' THEN 'medications_normalized'
                WHEN COALESCE(command2, command) LIKE '%WITH procedure_normalization AS%' THEN 'procedures_enriched'
                ELSE 'transform_operation'
            END
            ORDER BY LEN(COALESCE(command2, command)) DESC, submit_time ASC
        ) as rn
    FROM sys.dm_pdw_exec_requests
    WHERE submit_time >= '$START_TIMESTAMP'
    AND submit_time <= '$END_TIMESTAMP'
    AND status IN ('Completed', 'Failed')
    -- WHERE clause for TRANSFORM operations
    AND (
        COALESCE(command2, command) LIKE '%WITH patient_cleaning AS%' OR
        COALESCE(command2, command) LIKE '%WITH medication_normalization AS%' OR
        COALESCE(command2, command) LIKE '%WITH procedure_normalization AS%'
    )
    AND session_id != SESSION_ID()
)
SELECT
    CAST(ROUND(SUM(mb_processed), 6) AS VARCHAR) + ',' +
    CAST(ROUND((SUM(duration_seconds) / 3600.0) * 1.20, 6) AS VARCHAR) as result
FROM ranked_requests
WHERE rn = 1;"
}




{
echo "MB_processed,estimated_cost_usd" # header

case "$OPERATION_TYPE" in
"LOAD")
    calculate_load_cost | tail -n 1 | tr -d '\r\n '
    ;;
"QUERY")
    calculate_query_cost | tail -n 1 | tr -d '\r\n '
    ;;
"TRANSFORM")
    calculate_transform_cost | tail -n 1 | tr -d '\r\n '
    ;;
*)
    echo "Error: OPERATION_TYPE invalid" >&2
    exit 1
    ;;
esac
} | column -t -s, # IDENTICAL FORMATTING as BigQuery/Redshift



cost_data=$(case "$OPERATION_TYPE" in
"LOAD") calculate_load_cost | tail -n 1 | tr -d '\r\n ' ;;
"QUERY") calculate_query_cost | tail -n 1 | tr -d '\r\n ' ;;
"TRANSFORM") calculate_transform_cost | tail -n 1 | tr -d '\r\n ' ;;
esac)


if [[ -n "$cost_data" && "$cost_data" =~ ^[0-9,.]+$ ]]; then
    mb_processed=$(echo "$cost_data" | cut -d',' -f1)
    estimated_cost_usd=$(echo "$cost_data" | cut -d',' -f2)
else
    mb_processed="0"
    estimated_cost_usd="0.00"
fi

ts=$(date -u +"%Y-%m-%d %H:%M:%S")
echo "Synapse,$DATASET_SIZE,${OPERATION_TYPE}_COST,SUMMARY,,,,$mb_processed,,$estimated_cost_usd,$ts" | tee -a sy_results.csv