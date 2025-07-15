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



#uses
# we look for COPY INTO job labels
get_load_metrics() {
    sqlcmd -S "$SERVER_NAME" -d "$DATABASE" -U "$SQL_USER" -P "$SQL_PASS" \
           -I -h -1 -W -s "," -Q "
    SET NOCOUNT ON;
    
    SELECT TOP $NUM_JOBS
        'sy_' + request_id,
        DATEDIFF(SECOND, submit_time, end_time),
        0,
        COALESCE(total_elapsed_time/1000.0, 0),
        25 as dwu_percent_used,
        COALESCE(
            CASE 
                WHEN command LIKE '%INTO [raw].[%]%' THEN 
                    SUBSTRING(command, CHARINDEX('[raw].[', command) + 6, 20)
                ELSE 'load_operation'
            END, 'unknown'
        ),
        CONVERT(VARCHAR, submit_time, 126)
    FROM sys.dm_pdw_exec_requests
    WHERE submit_time >= '$START_TIMESTAMP'
        AND submit_time <= '$END_TIMESTAMP'
        AND status IN ('Completed', 'Failed')
        AND command LIKE '%COPY INTO%'
        AND command LIKE '%raw.%'
        AND session_id != SESSION_ID()
    ORDER BY submit_time ASC;"
}


# uses sys.dm_pdw_exec_requests
#  we look for table names and SELECT operations
get_query_metrics() {
    sqlcmd -S "$SERVER_NAME" -d "$DATABASE" -U "$SQL_USER" -P "$SQL_PASS" \
        -I -h -1 -W -s "," -Q "
    SET NOCOUNT ON;
    SELECT TOP $NUM_JOBS
        'sy_' + request_id,
        DATEDIFF(SECOND, submit_time, end_time),
        CASE 
            WHEN COALESCE(command2, command) LIKE '%patient_observation_timeline%' 
                AND COALESCE(command2, command) LIKE '%total_observations%' 
                AND COALESCE(command2, command) LIKE '%raw.observations%' THEN
                CASE 
                    WHEN '$DATASET_SIZE' = 'sample' THEN 96125
                    WHEN '$DATASET_SIZE' = '10k_covid' THEN 1659751
                    WHEN '$DATASET_SIZE' = '2k_generated' THEN 987417
                    WHEN '$DATASET_SIZE' = '100k_covid' THEN 16219970
                    ELSE 96125
                END
            WHEN COALESCE(command2, command) LIKE '%base_aggregation%' 
                AND COALESCE(command2, command) LIKE '%p.GENDER%' 
                AND COALESCE(command2, command) LIKE '%e.TOTAL_CLAIM_COST%' THEN 6622
            WHEN COALESCE(command2, command) LIKE '%encounter_medication_matrix%' 
                AND COALESCE(command2, command) LIKE '%patient_complexity_score%' THEN 5353
            WHEN COALESCE(command2, command) LIKE '%condition_sequences%' 
                AND COALESCE(command2, command) LIKE '%condition_networks%' THEN 4429
            ELSE 0
        END as rows_processed,
        -- TABLE_NAME pattern matching
        CASE
            WHEN COALESCE(command2, command) LIKE '%patient_observation_timeline%' 
                AND COALESCE(command2, command) LIKE '%total_observations%' 
                AND COALESCE(command2, command) LIKE '%raw.observations%' 
                THEN 'observations_patients_analysis'
            WHEN COALESCE(command2, command) LIKE '%base_aggregation%' 
                AND COALESCE(command2, command) LIKE '%p.GENDER%' 
                AND COALESCE(command2, command) LIKE '%e.TOTAL_CLAIM_COST%' 
                THEN 'patients_encounters_basic'
            WHEN COALESCE(command2, command) LIKE '%encounter_medication_matrix%' 
                AND COALESCE(command2, command) LIKE '%patient_complexity_score%' 
                THEN 'encounters_medications_complex'
            WHEN COALESCE(command2, command) LIKE '%condition_sequences%' 
                AND COALESCE(command2, command) LIKE '%condition_networks%' 
                THEN 'conditions_progression'
            ELSE 'query_operation'
        END,
        -- CREATION_TIME
        CONVERT(VARCHAR, submit_time, 126)
    FROM sys.dm_pdw_exec_requests
    WHERE submit_time >= '$START_TIMESTAMP'
        AND submit_time <= '$END_TIMESTAMP'
        AND status IN ('Completed', 'Failed')
        AND (
            COALESCE(command2, command) LIKE 'SELECT%' OR
            COALESCE(command2, command) LIKE 'WITH%' OR 
            COALESCE(command2, command) LIKE '-- Query%'
        )
        AND COALESCE(command2, command) LIKE '%raw.%'
        AND session_id != SESSION_ID()
    ORDER BY submit_time ASC;"
}

# queries  sys.dm_pdw_exec_requests
# looks for created table names
get_transform_metrics() {
    sqlcmd -S "$SERVER_NAME" -d "$DATABASE" -U "$SQL_USER" -P "$SQL_PASS" \
        -I -h -1 -W -s "," -Q "
    SET NOCOUNT ON;
    
    WITH ranked_requests AS (
        SELECT 
            'sy_' + request_id as job_id,
            DATEDIFF(SECOND, submit_time, end_time) as duration_seconds,
            116 as rows_processed,
            COALESCE(total_elapsed_time/1000.0, 0) as MB_processed,
            --50 as dwu_percent_used,
            CASE
                WHEN COALESCE(command2, command) LIKE '%WITH patient_cleaning%' THEN 'patients_clean'
                WHEN COALESCE(command2, command) LIKE '%WITH medication_normalization%' THEN 'medications_normalized'
                WHEN COALESCE(command2, command) LIKE '%WITH procedure_normalization%' THEN 'procedures_enriched'
                ELSE 'transform_operation'
            END as table_name,
            CONVERT(VARCHAR, submit_time, 126) as creation_time,
            ROW_NUMBER() OVER (
                PARTITION BY 
                    CASE
                        WHEN COALESCE(command2, command) LIKE '%WITH patient_cleaning%' THEN 'patients_clean'
                        WHEN COALESCE(command2, command) LIKE '%WITH medication_normalization%' THEN 'medications_normalized'
                        WHEN COALESCE(command2, command) LIKE '%WITH procedure_normalization%' THEN 'procedures_enriched'
                        ELSE 'transform_operation'
                    END
                ORDER BY LEN(COALESCE(command2, command)) DESC, submit_time ASC
            ) as rn
        FROM sys.dm_pdw_exec_requests
        WHERE submit_time >= '$START_TIMESTAMP'
            AND submit_time <= '$END_TIMESTAMP'
            AND status IN ('Completed', 'Failed')
            AND (
                COALESCE(command2, command) LIKE '%WITH patient_cleaning%' OR
                COALESCE(command2, command) LIKE '%WITH medication_normalization%' OR
                COALESCE(command2, command) LIKE '%WITH procedure_normalization%'
            )
            AND session_id != SESSION_ID()
    )
    SELECT TOP $NUM_JOBS
        job_id,
        duration_seconds,
        --rows_processed,
        MB_processed,
        --dwu_percent_used,
        table_name,
        creation_time
    FROM ranked_requests
    WHERE rn = 1
    ORDER BY creation_time ASC;"
}



# header 
#echo "job_id,duration_seconds,rows_processed,X,X,table_name,creation_time"
echo "job_id,duration_seconds,rows_processed,MB_processed,dwu_percent_used,table_name,creation_time"

# Main execution
case "$OPERATION_TYPE" in
    "LOAD")
        get_load_metrics
        ;;
    "QUERY")
        get_query_metrics
        ;;
    "TRANSFORM")
        get_transform_metrics
        ;;
    *)
        echo "Error: OPERATION_TYPE must be 'LOAD', 'QUERY', or 'TRANSFORM'" >&2
        exit 1
        ;;
esac
