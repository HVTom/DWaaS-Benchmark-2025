#!/bin/bash

DATASET_RAW="synthea_raw"
DATASET_PROCESSED="synthea_processed"
OPERATION_TYPE="QUERY"  # transform queries of type QUERY inside BigQuery!
DATASET_SIZE=${1:-"unknown"} 

echo "=== TRANSFORM BENCHMARK ==="
echo "Source: $DATASET_RAW"
echo "Target: $DATASET_PROCESSED"
echo "========================================"

START_TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%S")

# Transform 1: Window Analytics
echo "Transform 1: Window Analytics"
START_TIME=$(date +%s)

bq query --use_legacy_sql=false --nouse_cache "
CREATE OR REPLACE TABLE \`${DATASET_PROCESSED}.patients_clean\` AS
WITH patient_cleaning AS (
  SELECT 
    Id,
    -- data cleaning./trimming
    -- here we fill empty fields using coalesce
    TRIM(UPPER(COALESCE(FIRST, 'UNKNOWN'))) as first_name_clean,
    TRIM(UPPER(COALESCE(LAST, 'UNKNOWN'))) as last_name_clean,
    
    -- revrsed tsandardization
    -- inside the tables, the defaults are M F but we 
    -- force this reversal
    CASE 
      WHEN GENDER IN ('M', 'Male', 'MALE', 'm') THEN 'MALE'
      WHEN GENDER IN ('F', 'Female', 'FEMALE', 'f') THEN 'FEMALE'
      ELSE 'UNKNOWN'
    END as gender_standard,
    
    -- vlaidation cleaning the timestamp-typed row
    CASE 
      WHEN BIRTHDATE IS NULL OR BIRTHDATE > CURRENT_DATE() 
      THEN DATE('1900-01-01')
      ELSE BIRTHDATE 
    END as birthdate_clean,
    
    TRIM(UPPER(COALESCE(RACE, 'UNKNOWN'))) as race_normalized,
    TRIM(UPPER(COALESCE(STATE, 'UNKNOWN'))) as state_normalized,
    
    -- numeric validation
    CASE 
      WHEN ZIP IS NULL OR ZIP < 10000 OR ZIP > 99999 
      THEN 0 
      ELSE ZIP 
    END as zip_validated
    
  FROM \`${DATASET_RAW}.patients\`
  WHERE Id IS NOT NULL
)
SELECT * FROM patient_cleaning;"

END_TIME=$(date +%s)
echo "Transform 1 completed in $((END_TIME-START_TIME)) seconds"

sleep 10

# # Transform 2: Aggregation & Denormalization  
echo "Transform 2: Multi-Self-Join Analysis"
START_TIME=$(date +%s)

bq query --use_legacy_sql=false --nouse_cache "
CREATE OR REPLACE TABLE \`${DATASET_PROCESSED}.medications_normalized\` AS
WITH medication_normalization AS (
  SELECT 
    PATIENT,
    ENCOUNTER,
    
    -- timestamp normalization
    --CASE 
      --WHEN \`START\` IS NULL OR \`START\` > CURRENT_TIMESTAMP() 
      --THEN TIMESTAMP('1900-01-01 00:00:00')
      --ELSE \`START\` 
    --END as start_normalized,
    CASE 
      WHEN \`START\` IS NULL OR \`START\` > CURRENT_DATE()
      THEN DATE('1900-01-01')
      ELSE \`START\` 
    END as start_normalized,
    
    CASE 
      WHEN \`STOP\` IS NULL OR \`STOP\` < \`START\`
      --WHEN \`STOP\` IS NULL OR CAST(\`STOP\` AS TIMESTAMP) < CAST(\`START\` AS TIMESTAMP) 
      THEN NULL
      ELSE \`STOP\` 
    END as stop_validated,
    
    -- code standarsization
    CAST(CODE as STRING) as code_standard,
    TRIM(COALESCE(DESCRIPTION, 'Unknown Medication')) as description_clean,
    
    -- cost cleaning, same as vefore
    CASE 
      WHEN BASE_COST IS NULL OR BASE_COST < 0 
      THEN 0.0
      ELSE ROUND(BASE_COST, 2)
    END as base_cost_validated,
    
    CASE 
      WHEN TOTALCOST IS NULL OR TOTALCOST < 0 
      THEN 0.0
      ELSE ROUND(TOTALCOST, 2)
    END as total_cost_validated,
    
    -- quantity checking and validation
    CASE 
      WHEN DISPENSES IS NULL OR DISPENSES <= 0 
      THEN 1
      ELSE DISPENSES 
    END as dispenses_validated,
    
    -- this is where we enrich the new table and add another column
    CASE 
      WHEN DISPENSES IS NULL OR DISPENSES = 0 OR TOTALCOST IS NULL 
      THEN 0.0
      ELSE ROUND(TOTALCOST / DISPENSES, 4)
    END as cost_per_dispense,
    
    -- reason code standardization
    CAST(COALESCE(REASONCODE, 0) as STRING) as reason_code_standard
    
  FROM \`${DATASET_RAW}.medications\`
  WHERE PATIENT IS NOT NULL 
    AND CODE IS NOT NULL
    AND \`START\` IS NOT NULL
)
SELECT * FROM medication_normalization;"

END_TIME=$(date +%s)
echo "Transform 2 completed in $((END_TIME-START_TIME)) seconds"

sleep 10

# Transform 3: Procedures Enrichment & Standardization
echo "Transform 3: Procedures Enrichment & Standardization"
START_TIME=$(date +%s)

bq query --use_legacy_sql=false --nouse_cache "
CREATE OR REPLACE TABLE \`${DATASET_PROCESSED}.procedures_enriched\` AS
WITH procedure_normalization AS (
  SELECT
    PATIENT,
    ENCOUNTER,
    
    -- here the sql code is not dynamically partitioned
    -- there are 2 versions of schema 1 for synthea_latest and 2k
    -- and one for 10k si 100k
    -- we just do the switch when testing, 
    -- automated/dynamic switch for future improvements

    /* 
    =============== V1: pt synthea_latest si 2k =============== 
    -- uses the columns START/STOP/SYSTEM
    */
    /*
    -- timestamp checking/normalization (START)
    CASE
      WHEN START IS NULL OR START > CURRENT_DATE()
      THEN DATE('1900-01-01')
      ELSE START
    END as start_normalized,

    -- procedure duration (START/STOP)
    CASE
      WHEN STOP IS NOT NULL AND START IS NOT NULL AND STOP > START
      THEN DATETIME_DIFF(DATETIME(STOP), DATETIME(START), MINUTE)
      ELSE 0
    END as procedure_duration_minutes,

    -- extra operation to  codificate (SYSTEM)
    CASE
      WHEN UPPER(SYSTEM) LIKE '%SNOMED%' THEN 'SNOMED_CT'
      WHEN UPPER(SYSTEM) LIKE '%CPT%' THEN 'CPT'
      WHEN UPPER(SYSTEM) LIKE '%ICD%' THEN 'ICD'
      ELSE 'OTHER'
    END as coding_system_standard,
    */

    /* 
    =============== V2: pt 10k si 100k =============== 
    -- here we use DATE intead of START/STOP, similar working column type
    */
    -- normalize date (DATE), same extra operation
    -- to increase processing time
    CASE
      WHEN DATE IS NULL OR DATE > CURRENT_DATE()
      THEN DATE('1900-01-01')
      ELSE DATE
    END as date_normalized,

    -- no STOP column here, so a fixed 0 vlaue is uesd
    0 as procedure_duration_minutes,

    -- code_based classification (CODE)
    CASE
      WHEN UPPER(CODE) LIKE '%SNOMED%' THEN 'SNOMED_CT'
      WHEN UPPER(CODE) LIKE '%CPT%' THEN 'CPT'
      WHEN UPPER(CODE) LIKE '%ICD%' THEN 'ICD'
      ELSE 'UNKNOWN'
    END as coding_system_standard,


    -- codes check and standardization (as in medications)
    -- replaces NULL with 0 + string conversion
    CAST(CODE AS STRING) as code_standard,
    CAST(COALESCE(REASONCODE, 0) AS STRING) as reason_code_standard,
    
    -- cost validation and cleaning
    CASE
      WHEN BASE_COST IS NULL OR BASE_COST < 0
      THEN 0.0
      ELSE ROUND(BASE_COST, 2)
    END as cost_validated,
    
    -- procedure complexity classification
    CASE
      WHEN BASE_COST <= 50 THEN 'SIMPLE'
      WHEN BASE_COST <= 500 THEN 'MODERATE' 
      WHEN BASE_COST <= 2000 THEN 'COMPLEX'
      ELSE 'HIGHLY_COMPLEX'
    END as procedure_complexity,
    
    -- description cleaning
    TRIM(UPPER(COALESCE(DESCRIPTION, 'UNKNOWN_PROCEDURE'))) as description_clean,
    TRIM(UPPER(COALESCE(REASONDESCRIPTION, 'NO_REASON'))) as reason_clean
    
  FROM \`${DATASET_RAW}.procedures\`
  WHERE PATIENT IS NOT NULL AND CODE IS NOT NULL
)
SELECT * FROM procedure_normalization;"

END_TIME=$(date +%s)
echo "Transform 3 completed in $((END_TIME-START_TIME)) seconds"
sleep 10

END_TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%S")


echo -e "\n=================== TRANSFORM OUTPUT ROWS =================="
echo "Querying row counts for created tables..."
printf "%-25s %s\n" "Table_Created" "Rows_Created"
echo "----------------------------------------"

# Transform 1 output rows
t1_rows=$(bq query --use_legacy_sql=false --format=csv --nouse_cache --max_rows=1 "SELECT COUNT(*) FROM \`$DATASET_PROCESSED.patients_clean\`" | tail -n 1)
printf "%-25s %s\n" "patients_clean" "$t1_rows"

# Transform 2 output rows
t2_rows=$(bq query --use_legacy_sql=false --format=csv --nouse_cache --max_rows=1 "SELECT COUNT(*) FROM \`$DATASET_PROCESSED.medications_normalized\`" | tail -n 1)
printf "%-25s %s\n" "medications_normalized" "$t2_rows"

# Transform 3 output rows
t3_rows=$(bq query --use_legacy_sql=false --format=csv --nouse_cache --max_rows=1 "SELECT COUNT(*) FROM \`$DATASET_PROCESSED.procedures_enriched\`" | tail -n 1)
printf "%-25s %s\n" "procedures_enriched" "$t3_rows"



echo -e "\n=================== TRANSFORM JOB TIMES ====================="
echo "Retrieving actual transform execution times from BigQuery..."
metrics_data=$(./bq_metrics.sh "$START_TIMESTAMP" "$END_TIMESTAMP" "$OPERATION_TYPE" "$DATASET_PROCESSED" "3")

echo "$metrics_data" | column -t -s,

total_bq_time=$(echo "$metrics_data" | awk -F',' 'NR>1 {sum+=$2} END {print sum}')
successful_transforms=$(echo "$metrics_data" | awk -F',' 'NR>1 {count++} END {print count+0}')

echo -e "\n=================== TRANSFORM METRICS ====================="
echo "Transforms attempted: 3"
echo "Transforms successfully completed: $successful_transforms"
echo "Total BigQuery transform processing time: $total_bq_time seconds"

echo -e "\n=================== TRANSFORM COST ==========================="
./bq_cost.sh "$START_TIMESTAMP" "$END_TIMESTAMP" "$OPERATION_TYPE" "$DATASET_PROCESSED" "3" "$DATASET_SIZE"

echo -e "\n=================== TRANSFORM CLEANUP ==========================="
echo -e "Calling bq_clear.sh...\n"
sh ./bq_clear.sh "$DATASET_RAW" "$DATASET_PROCESSED"
echo -e "\n=== TRANSFORMS COMPLETED ==="


echo "Writing transform results to CSV..."
{
    # ONLY the jobs
    echo "$metrics_data" | awk -F',' -v size="$DATASET_SIZE" 'NR>1 {
        printf "BigQuery,%s,TRANSFORM,%s,%s,%s,0,0,%s,0,%s\n", 
               size, $1, $2, $3, $5, $6
    }'
} >> bq_results.csv
echo "Transform results appended to bq_results.csv"



