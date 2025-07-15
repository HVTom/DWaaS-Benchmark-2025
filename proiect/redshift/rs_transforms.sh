#!/bin/bash

# Redshift connection details
REDSHIFT_HOST="redshift-benchmark-cluster-1.csag37dwinrq.eu-central-1.redshift.amazonaws.com"
REDSHIFT_DB="benchmark"
REDSHIFT_USER="awsuser"
REDSHIFT_PORT="5439"
export PGPASSWORD='?Gn+3&t&9}vS!:)'
export PAGER=""

DATASET_RAW="dataset_raw"
DATASET_PROCESSED="dataset_processed"
OPERATION_TYPE="TRANSFORM"

DATASET_SIZE=$1

echo "=== TRANSFORM BENCHMARK ==="
echo "Source: $DATASET_RAW"
echo "Target: $DATASET_PROCESSED"
echo "========================================"

START_TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%S")

# create schema if not exists
psql -h $REDSHIFT_HOST -p $REDSHIFT_PORT -U $REDSHIFT_USER -d $REDSHIFT_DB -c "CREATE SCHEMA IF NOT EXISTS ${DATASET_PROCESSED};"

# Transform 1: Data Cleaning & Standardization
echo "Transform 1: Data Cleaning & Standardization"
START_TIME=$(date +%s)

psql -h $REDSHIFT_HOST -p $REDSHIFT_PORT -U $REDSHIFT_USER -d $REDSHIFT_DB -c "
SET enable_result_cache_for_session TO off;
-- psql uses different string interpolation, no back ticks
DROP TABLE IF EXISTS ${DATASET_PROCESSED}.patients_clean;

CREATE TABLE ${DATASET_PROCESSED}.patients_clean AS
WITH patient_cleaning AS (
  SELECT 
    Id,
    TRIM(UPPER(COALESCE(FIRST, 'UNKNOWN'))) as first_name_clean,
    TRIM(UPPER(COALESCE(LAST, 'UNKNOWN'))) as last_name_clean,
    
    CASE 
      WHEN GENDER IN ('M', 'Male', 'MALE', 'm') THEN 'MALE'
      WHEN GENDER IN ('F', 'Female', 'FEMALE', 'f') THEN 'FEMALE'
      ELSE 'UNKNOWN'
    END as gender_standard,
    
    CASE 
      WHEN BIRTHDATE IS NULL OR BIRTHDATE::DATE > CURRENT_DATE 
      THEN '1900-01-01'::DATE
      ELSE BIRTHDATE::DATE 
    END as birthdate_clean,
    
    TRIM(UPPER(COALESCE(RACE, 'UNKNOWN'))) as race_normalized,
    TRIM(UPPER(COALESCE(STATE, 'UNKNOWN'))) as state_normalized,
    
    CASE 
      WHEN ZIP IS NULL OR ZIP::INTEGER < 10000 OR ZIP::INTEGER > 99999 
      THEN 0 
      ELSE ZIP::INTEGER 
    END as zip_validated
    
  FROM ${DATASET_RAW}.patients
  WHERE Id IS NOT NULL
)
SELECT * FROM patient_cleaning;"

END_TIME=$(date +%s)
echo "Transform 1 completed in $((END_TIME-START_TIME)) seconds"
sleep 10


# Transform 2: Aggregation & Denormalization – NO LISTAGG to avoid errors and syntax limitation
echo "Transform 2: Aggregation & Denormalization"
START_TIME=$(date +%s)
psql -h $REDSHIFT_HOST -p $REDSHIFT_PORT -U $REDSHIFT_USER -d $REDSHIFT_DB -c "
SET enable_result_cache_for_session TO off;
-- Redshift medications_normalized - corrected because of the overflow warning
DROP TABLE IF EXISTS ${DATASET_PROCESSED}.medications_normalized;

CREATE TABLE ${DATASET_PROCESSED}.medications_normalized AS
WITH medication_normalization AS (
  SELECT 
    PATIENT,
    ENCOUNTER,
    
    CASE 
      WHEN "START" IS NULL OR "START" > CURRENT_TIMESTAMP 
      THEN '1900-01-01 00:00:00'::TIMESTAMP
      ELSE "START"::TIMESTAMP 
    END as start_normalized,
    
    CASE 
      WHEN STOP IS NULL OR STOP < "START" 
      THEN NULL::TIMESTAMP
      ELSE STOP::TIMESTAMP 
    END as stop_validated,
    
    CODE::VARCHAR as code_standard,
    TRIM(COALESCE(DESCRIPTION, 'Unknown Medication')) as description_clean,
    
    CASE 
      WHEN BASE_COST IS NULL OR BASE_COST < 0 
      THEN 0.0::DECIMAL(18,2)
      ELSE ROUND(BASE_COST::DECIMAL(18,2), 2)
    END as base_cost_validated,
    
    -- uses ROUND(value::DECIMAL(18,2), 2) for precision
    CASE 
      WHEN TOTALCOST IS NULL OR TOTALCOST < 0 
      THEN 0.0::DECIMAL(18,2)
      ELSE ROUND(TOTALCOST::DECIMAL(18,2), 2)
    END as total_cost_validated,
    
    CASE 
      WHEN DISPENSES IS NULL OR DISPENSES <= 0 
      THEN 1::INTEGER
      ELSE DISPENSES::INTEGER 
    END as dispenses_validated,
    
    CASE 
      WHEN DISPENSES IS NULL OR DISPENSES = 0 OR TOTALCOST IS NULL 
      THEN 0.0::DECIMAL(18,4)
      ELSE ROUND(TOTALCOST::DECIMAL(18,4) / NULLIF(DISPENSES::DECIMAL(18,4), 0), 4)
    END as cost_per_dispense,
    
    COALESCE(REASONCODE::VARCHAR, '0') as reason_code_standard
    
  FROM ${DATASET_RAW}.medications
  WHERE PATIENT IS NOT NULL 
    AND CODE IS NOT NULL
    AND "START" IS NOT NULL
)
SELECT * FROM medication_normalization;"

END_TIME=$(date +%s)
echo "Transform 2 completed in $((END_TIME-START_TIME)) seconds"

sleep 10

# Transform 3: Procedures Enrichment & Standardization
# procedures_enriched - souble quotes for column names
# that are reserved keywords
echo "Transform 3: Procedures Enrichment & Standardization"
START_TIME=$(date +%s)
psql -h $REDSHIFT_HOST -p $REDSHIFT_PORT -U $REDSHIFT_USER -d $REDSHIFT_DB -c "
SET enable_result_cache_for_session TO off;
DROP TABLE IF EXISTS ${DATASET_PROCESSED}.procedures_enriched;

CREATE TABLE ${DATASET_PROCESSED}.procedures_enriched AS
WITH procedure_normalization AS (
  SELECT
    PATIENT,
    ENCOUNTER,
    
    /* 
    ================== V1: synthea_latest and 2k ==================
    -- uses START/STOP/SYSTEM columns
    CASE
        WHEN "start" IS NULL OR "start"::TIMESTAMP > CURRENT_TIMESTAMP
        THEN '1900-01-01 00:00:00'::TIMESTAMP
        ELSE "start"::TIMESTAMP
    END as start_normalized,

    -- DATE_PART - same as date_DIFF in bigquery
    CASE
        WHEN "STOP" IS NOT NULL AND "start" IS NOT NULL AND "STOP" > "start"
        THEN DATE_PART('minute', "STOP"::TIMESTAMP - "start"::TIMESTAMP) 
        ELSE 0
    END as procedure_duration_minutes,

    CASE
        WHEN UPPER("SYSTEM") LIKE '%SNOMED%' THEN 'SNOMED_CT'
        WHEN UPPER("SYSTEM") LIKE '%CPT%' THEN 'CPT'
        WHEN UPPER("SYSTEM") LIKE '%ICD%' THEN 'ICD'
        ELSE 'OTHER'
    END as coding_system_standard,
    */
    
    -- ================== V2: 10k and 100k ==================
    -- uses DATE code classification
    CASE
        WHEN "date" IS NULL OR "date"::TIMESTAMP > CURRENT_TIMESTAMP
        THEN '1900-01-01 00:00:00'::TIMESTAMP
        ELSE "date"::TIMESTAMP
    END as procedure_date_normalized,

    0 as procedure_duration_minutes,

    CASE
        WHEN CODE LIKE '%SNOMED%' THEN 'SNOMED_CT'
        WHEN CODE LIKE '%CPT%' THEN 'CPT'
        WHEN CODE LIKE '%ICD%' THEN 'ICD'
        ELSE 'OTHER'
    END as coding_system_standard,

    -- code standardization with coalesce
    CODE::VARCHAR as code_standard,
    COALESCE(REASONCODE::VARCHAR, '0') as reason_code_standard,
    
    CASE
      WHEN BASE_COST IS NULL OR BASE_COST < 0
      THEN 0.0::DECIMAL(18,2)
      ELSE ROUND(BASE_COST::DECIMAL(18,2), 2)
    END as cost_validated,
    
    CASE
      WHEN BASE_COST <= 50 THEN 'SIMPLE'
      WHEN BASE_COST <= 500 THEN 'MODERATE' 
      WHEN BASE_COST <= 2000 THEN 'COMPLEX'
      ELSE 'HIGHLY_COMPLEX'
    END as procedure_complexity,
    
    TRIM(UPPER(COALESCE(DESCRIPTION, 'UNKNOWN_PROCEDURE'))) as description_clean,
    TRIM(UPPER(COALESCE(REASONDESCRIPTION, 'NO_REASON'))) as reason_clean
    
  FROM ${DATASET_RAW}.procedures
  WHERE PATIENT IS NOT NULL AND CODE IS NOT NULL
)
SELECT * FROM procedure_normalization;"

END_TIME=$(date +%s)
echo "Transform 3 completed in $((END_TIME-START_TIME)) seconds"
sleep 10
END_TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%S")

echo -e "\n=================== TRANSFORM JOB TIMES ====================="
echo "Retrieving actual transform execution times from Redshift..."

echo "Getting row counts for verification..."
VERIFICATION_OUTPUT=$(psql -h $REDSHIFT_HOST -p $REDSHIFT_PORT -U $REDSHIFT_USER -d $REDSHIFT_DB -t -A -F '|' -c "
SET enable_result_cache_for_session TO off;
SELECT
'patients_clean' as table_name,
COUNT(*) as rows
FROM ${DATASET_PROCESSED}.patients_clean
UNION ALL
SELECT
'medications_normalized',           
COUNT(*)
FROM ${DATASET_PROCESSED}.medications_normalized
UNION ALL
SELECT
'procedures_enriched',              
COUNT(*)
FROM ${DATASET_PROCESSED}.procedures_enriched;" 2>/dev/null)


# associative array with row counts
declare -A row_counts
while IFS='|' read -r table rows; do
    table=$(echo "$table" | xargs)  # trim whitespace
    rows=$(echo "$rows" | xargs)
    if [[ -n "$table" && "$table" != "" ]]; then
        row_counts["$table"]=$rows
    fi
done <<< "$VERIFICATION_OUTPUT"

# capture rs_metrics.sh
if [ -f "./rs_metrics.sh" ]; then
    metrics_data=$(bash ./rs_metrics.sh "$START_TIMESTAMP" "$END_TIMESTAMP" "$OPERATION_TYPE" "$DATASET_PROCESSED" "3" "$DATASET_SIZE")
    
    # parse WHOLE output (header + data) with row counts
    {
        echo "$metrics_data" | head -n1
        
        # process data with row counts
        echo "$metrics_data" | tail -n +2 | awk -F',' -v OFS=',' \
            -v pc="${row_counts[patients_clean]}" \
            -v pms="${row_counts[medications_normalized]}" \
            -v pra="${row_counts[procedures_enriched]}" \
            '{
                if ($5 == "patients_clean") $3=pc
                else if ($5 == "medications_normalized") $3=pms  
                else if ($5 == "procedures_enriched") $3=pra
                print
            }'
    } | column -t -s, -o '   '  # tweaking column horizontal sepparation
    
    total_rs_time=$(echo "$metrics_data" | awk -F',' 'NR>1 {sum+=$2} END {print sum+0}')
    successful_transforms=$(echo "$metrics_data" | awk -F',' 'NR>1 {count++} END {print count+0}')
else
    echo "rs_metrics.sh not found, skipping detailed metrics"
    total_rs_time=0
    successful_transforms=3
fi

echo -e "\n=================== TRANSFORM METRICS ====================="
echo "Transforms attempted: 3"
echo "Transforms successfully completed: $successful_transforms"
echo "Total Redshift transform processing time: $total_rs_time seconds"

echo -e "\n=================== TRANSFORM COST ==========================="
if [ -f "./rs_cost.sh" ]; then
    bash ./rs_cost.sh "$START_TIMESTAMP" "$END_TIMESTAMP" "$OPERATION_TYPE" "$DATASET_PROCESSED" "3" "$DATASET_SIZE"
else
    echo "rs_cost.sh not found, skipping cost calculation"
fi

echo -e "\n=================== VERIFYING TRANSFORM RESULTS ====================="
echo "Row counts have been integrated into job times above:"
echo "$VERIFICATION_OUTPUT" | while IFS='|' read -r table rows; do
    echo "  ✓ $table: $rows rows"
done

echo -e "\n=== TRANSFORMS COMPLETED ==="


unset PGPASSWORD

echo "Writing transform results to CSV..."
{
    if [ ! -f rs_results.csv ]; then
        echo "platform,dataset_size,operation_type,job_id,duration_seconds,rows_processed,mb_processed,total_exec_time,slot_count,table_name,cost_usd,timestamp"
    fi

    ts=$(date -u +"%Y-%m-%d %H:%M:%S")
    echo "$metrics_data" | awk -F',' -v size="$DATASET_SIZE" -v ts="$ts" '
    BEGIN {OFS=","}
    NR>1 {
        slot_count = int($6)  # convert to string
        print "Redshift", size, "TRANSFORM", $1, $2, $3, $4, $5, slot_count, $7, "0.0", ts
    }' >> rs_results.csv
}
