#!/bin/bash


REDSHIFT_CLUSTER="redshift-benchmark-cluster-1"
REDSHIFT_HOST="${REDSHIFT_CLUSTER}.csag37dwinrq.eu-central-1.redshift.amazonaws.com"
REDSHIFT_PORT="5439"
REDSHIFT_DB="benchmark"
REDSHIFT_USER="awsuser"
export PGPASSWORD='?Gn+3&t&9}vS!:)'


START_TIMESTAMP=$1
END_TIMESTAMP=$2
OPERATION_TYPE=$3
DATASET=$4
NUM_JOBS=${5:-10}
DATASET_SIZE=$6



# here for the metrics it is not possible to create dynamic queries
# like in BigQyery, mostly becaseu we need multiple system tables to ectract the metrics


# load_metrics uses 3 tables>
#STL_QUERY: execution info for all queries (query ID, start/end time, query text, etc.)
#STL_FILE_SCAN: info about file scans during COPY operations (query ID, lines/bytes processed)
# STL_WLM_QUERY: workload management info for each query (execution time, slot count, etc.)
# function look for the COPY labeled operations
# and also look for the targeyt dataset
get_load_metrics() {
    psql -h ${REDSHIFT_HOST} -p ${REDSHIFT_PORT} -d ${REDSHIFT_DB} -U ${REDSHIFT_USER} -A -F',' -c  "
    SET enable_result_cache_for_session TO off;
    WITH load_operations AS (
        SELECT
            q.query,
            SPLIT_PART(SPLIT_PART(q.querytxt, '${DATASET}.', 2), ' ', 1) as table_name,
            q.starttime,
            q.endtime,
            EXTRACT(EPOCH FROM (q.endtime - q.starttime)) as duration_seconds
        FROM STL_QUERY q
        WHERE q.starttime BETWEEN '${START_TIMESTAMP}' AND '${END_TIMESTAMP}'
            AND q.querytxt ILIKE '%COPY%'
            AND q.querytxt ILIKE '%${DATASET}%'
            AND q.aborted = 0
    ),
    load_stats AS (
        SELECT
            lo.query,
            lo.table_name,
            lo.duration_seconds,
            COALESCE(fs.lines, 0) as rows_processed,
            ROUND(COALESCE(fs.bytes, 0)/1024.0/1024.0, 2) as MB_processed,
            COALESCE(wlm.total_exec_time/1000000, 0) as total_exec_time,
            COALESCE(wlm.slot_count, 0) as slot_count,
            lo.starttime
        FROM load_operations lo
        LEFT JOIN STL_FILE_SCAN fs ON lo.query = fs.query
        LEFT JOIN STL_WLM_QUERY wlm ON lo.query = wlm.query
    )
    SELECT
        'rs_' || query as job_id,
        ROUND(duration_seconds, 3) as duration_seconds,
        rows_processed,
        MB_processed,
        total_exec_time,
        slot_count,
        table_name,
        starttime as creation_time
    FROM load_stats
    WHERE table_name IS NOT NULL AND table_name != ''
    ORDER BY starttime ASC
    LIMIT ${NUM_JOBS};
    "
}


# STL_QUERY
# STL_QUERY_METRICS: metrics for completed queries (rows processed, query scan size, etc.)
# STL_SCAN: info about table scans (rows affected, bytes scanned)
# STL_WLM_QUERY: workload management info (execution time, slot count, etc.)
# SVL_QUERY_METRICS_SUMMARY : aggregated metrics for each query (return row count, etc.)
#here there are targeted SELECT labeled queries and the table names 
get_query_metrics() {
    sleep 2
    psql -h ${REDSHIFT_HOST} -p ${REDSHIFT_PORT} -d ${REDSHIFT_DB} -U ${REDSHIFT_USER} -A -F',' -c  "
    SET enable_result_cache_for_session TO off;
    WITH query_operations AS (
        SELECT
            q.query,
            q.starttime,
            q.endtime,
            -- filter the timestamp of the script that ran
            EXTRACT(EPOCH FROM (q.endtime - q.starttime)) as duration_seconds,
            q.querytxt,
            ROW_NUMBER() OVER (ORDER BY q.starttime) as query_order
        FROM STL_QUERY q
        WHERE q.starttime BETWEEN '${START_TIMESTAMP}' AND '${END_TIMESTAMP}'
            AND q.querytxt ILIKE '%SELECT%'
            AND (
                q.querytxt ILIKE '%dataset_raw%' OR
                q.querytxt ILIKE '%encounters%' OR
                q.querytxt ILIKE '%patients%' OR
                q.querytxt ILIKE '%observations%'
            )
            AND q.querytxt NOT ILIKE '%stl_%'
            AND q.querytxt NOT ILIKE '%svl_%'
            AND q.querytxt NOT ILIKE '%pg_%'
            AND q.userid = (SELECT usesysid FROM pg_user WHERE usename = '${REDSHIFT_USER}')
            AND q.aborted = 0
    ),
    query_stats AS (
    SELECT
        qo.query,
        qo.duration_seconds,
        qo.query_order,
        --COALESCE(qm.rows, sc.rows_affected, 0) as rows_processed,
        --COALESCE(qms.return_row_count, qm.rows, sc.rows_affected, 0) as rows_processed,
        CASE 
            WHEN COALESCE(qms.return_row_count, qm.rows, sc.rows_affected, 0) <= 0 THEN 0
            ELSE COALESCE(qms.return_row_count, qm.rows, sc.rows_affected, 0)
        END as rows_processed,
        ROUND(COALESCE(qm.query_scan_size, sc.bytes_scanned/1024.0/1024.0, 1.0), 2) as MB_processed,
        COALESCE(wlm.total_exec_time/1000000, 0) as total_exec_time,
        COALESCE(wlm.slot_count, 0) as slot_count,

        -- we need to scan for the tables that have been processed in
        -- each query; this filtering ensures we find the right tables
        CASE
            -- Query 1: Basic Aggregation (with MEDIAN and cost_per_patient)
            -- fields that need to be scanned for in the 
            WHEN qo.querytxt ILIKE '%patients%' AND qo.querytxt ILIKE '%encounters%' 
                AND qo.querytxt ILIKE '%MEDIAN%' 
                AND qo.querytxt ILIKE '%cost_per_patient%'
                AND qo.querytxt NOT ILIKE '%medications%' 
                AND qo.querytxt NOT ILIKE '%conditions%'
                THEN 'patients_encounters_basic'
    
            -- Query 2: Multi-Table Analysis (with complexity_score and medications)
            WHEN qo.querytxt ILIKE '%encounters%' AND qo.querytxt ILIKE '%medications%'
                AND qo.querytxt ILIKE '%complexity_score%'
                AND qo.querytxt ILIKE '%patient_base_stats%'
                THEN 'encounters_medications_analysis'
    
            -- Query 3: Condition Progression (with conditions self-join)
            WHEN qo.querytxt ILIKE '%conditions%' AND qo.querytxt ILIKE '%c1%' AND qo.querytxt ILIKE '%c2%'
                AND qo.querytxt ILIKE '%days_between_conditions%'
                AND qo.querytxt ILIKE '%condition_sequences%'
                THEN 'conditions_progression_analysis'
    
            -- Query 4: Observations Analysis (with massive window functions)
            WHEN qo.querytxt ILIKE '%observations%' AND qo.querytxt ILIKE '%patients%'
                AND qo.querytxt ILIKE '%massive_rolling_count%'
                AND qo.querytxt ILIKE '%patient_observation_timeline%'
                THEN 'observations_patients_analysis'
    
            -- Fallback for unknown queries 
            WHEN qo.querytxt ILIKE '%SELECT%' AND qo.querytxt ILIKE '%dataset_raw%'
                THEN 'general_dataset_query'
            ELSE 'unidentified_query'
        END as table_name,
        qo.starttime
    FROM query_operations qo
    LEFT JOIN STL_WLM_QUERY wlm ON qo.query = wlm.query
    LEFT JOIN STL_QUERY_METRICS qm ON qo.query = qm.query AND qm.segment = -1
    LEFT JOIN (
        SELECT query, 
           SUM(COALESCE(rows, 0)) as rows_affected,
           SUM(COALESCE(bytes, 0)) as bytes_scanned
        FROM STL_SCAN
        GROUP BY query
    ) sc ON qo.query = sc.query
    LEFT JOIN SVL_QUERY_METRICS_SUMMARY qms ON qo.query = qms.query
    )
    SELECT
        'rs_' || query as job_id,
        ROUND(duration_seconds, 3) as duration_seconds,
        rows_processed,
        MB_processed,
        total_exec_time,
        slot_count,
        table_name,
        starttime as creation_time
    FROM query_stats
    ORDER BY starttime ASC
    LIMIT ${NUM_JOBS};
    "
}


# System Tables Used:
#   - STL_QUERY
#   - STL_QUERY_METRICS
#   - STL_WLM_QUERY
#   - STL_SCAN
#   - SVV_TABLE_INFO: Info about tables in the database (row count for created tables)
#   - SVL_QUERY_METRICS_SUMMARY
# here we only look for the names of the created tables
get_transform_metrics() {
    sleep 2
    psql -h ${REDSHIFT_HOST} -p ${REDSHIFT_PORT} -d ${REDSHIFT_DB} -U ${REDSHIFT_USER} -A -F',' -c  "
    SET enable_result_cache_for_session TO off;
    WITH transform_operations AS (
        SELECT
            q.query,
            q.starttime,
            q.endtime,
            EXTRACT(EPOCH FROM (q.endtime - q.starttime)) as duration_seconds,
            q.querytxt,
            ROW_NUMBER() OVER (ORDER BY q.starttime) as transform_order
        FROM STL_QUERY q
        WHERE q.starttime BETWEEN '${START_TIMESTAMP}' AND '${END_TIMESTAMP}'
            AND (
                q.querytxt ILIKE '%CREATE%TABLE%patients_clean%' OR
                q.querytxt ILIKE '%CREATE%TABLE%medications_normalized%' OR  
                q.querytxt ILIKE '%CREATE%TABLE%procedures_enriched%' OR     
                q.querytxt ILIKE '%CREATE%TABLE%patient_medical_summary%' OR
                q.querytxt ILIKE '%CREATE%TABLE%patient_risk_analysis%' OR
                q.querytxt ILIKE '%DROP%TABLE%${DATASET}%'
            )
            AND q.userid = (SELECT usesysid FROM pg_user WHERE usename = '${REDSHIFT_USER}')
            AND q.aborted = 0
    ),
    transform_stats AS (
    SELECT
        to_op.query,
        to_op.duration_seconds,
        to_op.transform_order,
        --COALESCE(qm.rows, ti.tbl_rows, 0) as rows_processed,
        CASE
            WHEN COALESCE(qms.return_row_count, qm.rows, ti.tbl_rows, 0) <= 0 THEN 0
            ELSE COALESCE(qms.return_row_count, qm.rows, ti.tbl_rows, 0)
        END as rows_processed,
        ROUND(COALESCE(qm.query_scan_size, sc.bytes_scanned/1024.0/1024.0, 1.0), 2) as MB_processed,
        COALESCE(wlm.total_exec_time/1000000, 0) as total_exec_time,
        COALESCE(wlm.slot_count, 0) as slot_count,
        CASE
            WHEN to_op.querytxt ILIKE '%patients_clean%' THEN 'patients_clean'
            WHEN to_op.querytxt ILIKE '%medications_normalized%' THEN 'medications_normalized'      
            WHEN to_op.querytxt ILIKE '%procedures_enriched%' THEN 'procedures_enriched'            
            WHEN to_op.querytxt ILIKE '%patient_medical_summary%' THEN 'patient_medical_summary'
            WHEN to_op.querytxt ILIKE '%patient_risk_analysis%' THEN 'patient_risk_analysis'
            ELSE 'transform_operation'
        END as table_name,
        to_op.starttime
    FROM transform_operations to_op
    LEFT JOIN STL_QUERY_METRICS qm ON to_op.query = qm.query AND qm.segment = -1
    LEFT JOIN STL_WLM_QUERY wlm ON to_op.query = wlm.query
    -- adds info about the created table dimensions
    LEFT JOIN svv_table_info ti ON ti.table = CASE
        WHEN to_op.querytxt ILIKE '%patients_clean%' THEN 'patients_clean'
        WHEN to_op.querytxt ILIKE '%patient_medical_summary%' THEN 'patient_medical_summary'
        WHEN to_op.querytxt ILIKE '%patient_risk_analysis%' THEN 'patient_risk_analysis'
    END AND ti.schema = 'dataset_processed'
    -- adds scan metrics for the bytes processed column
    LEFT JOIN (
        SELECT query, SUM(COALESCE(bytes, 0)) as bytes_scanned
        FROM STL_SCAN
        GROUP BY query
    ) sc ON to_op.query = sc.query
    LEFT JOIN SVL_QUERY_METRICS_SUMMARY qms ON to_op.query = qms.query
    )
    SELECT
        'rs_' || query as job_id,
        ROUND(duration_seconds, 3) as duration_seconds,
        rows_processed,
        MB_processed,
        total_exec_time,
        slot_count,
        table_name,
        starttime as creation_time
    FROM transform_stats
    ORDER BY starttime ASC
    LIMIT ${NUM_JOBS};
    "
}

# main
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
        echo "Error: OPERATION_TYPE must be 'LOAD', 'QUERY', or 'TRANSFORM'"
        exit 1
        ;;
esac

unset PGPASSWORD
