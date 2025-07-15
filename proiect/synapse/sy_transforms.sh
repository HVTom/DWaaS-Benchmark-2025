#!/bin/bash

SYNAPSE_WORKSPACE="dwbenchmarkworkspace"
SQL_POOL="dwbenchmarkdedicatedpool"
SERVER_NAME="${SYNAPSE_WORKSPACE}.sql.azuresynapse.net"
DATABASE="$SQL_POOL"
SQL_USER="sqladminuser"
SQL_PASS="j7A+2,;}xVm=p&5"

SCHEMA_RAW="raw"
SCHEMA_PROCESSED="processed"
OPERATION_TYPE="TRANSFORM"

DATASET_SIZE=$1


# execute query with error checking
run_transform_with_check() {
    local query="$1"
    local transform_name="$2"
    local table_name="$3"
    
    echo "$transform_name"
    
    # output catch
    result=$(sqlcmd -S "$SERVER_NAME" -d "$DATABASE" -U "$SQL_USER" -P "$SQL_PASS" -I -Q "$query" 2>&1)
    
    if echo "$result" | grep -q "Msg.*Level.*Error"; then
        echo "ERROR in $transform_name:"
        echo "$result" | grep "Msg.*Level"
        return 1
    else
        echo "$transform_name completed successfully"
        
        # check table creation
        row_count=$(sqlcmd -S "$SERVER_NAME" -d "$DATABASE" -U "$SQL_USER" -P "$SQL_PASS" -I -h -1 -Q "
        SELECT COUNT(*) FROM [$SCHEMA_PROCESSED].[$table_name]" 2>/dev/null | tr -d ' ')
        
        if [[ "$row_count" =~ ^[0-9]+$ ]]; then
            echo "   Created table: $table_name with $row_count rows"
        fi
        return 0
    fi
}

echo "=== SYNAPSE TRANSFORM BENCHMARK ==="
echo "Source Schema: $SCHEMA_RAW"
echo "Target Schema: $SCHEMA_PROCESSED" 
echo "Synapse Pool: $SQL_POOL"
echo "========================================"

START_TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%S")

# processed schema creation
sqlcmd -S "$SERVER_NAME" -d "$DATABASE" -U "$SQL_USER" -P "$SQL_PASS" -I -Q "
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '$SCHEMA_PROCESSED')
BEGIN
    EXEC('CREATE SCHEMA [$SCHEMA_PROCESSED]')
END"

# Transform 1: Data Cleaning
echo "Transform 1: Data Cleaning & Standardization"
START_TIME=$(date +%s)

run_transform_with_check "
SET NOCOUNT ON;
SET RESULT_SET_CACHING OFF;
IF OBJECT_ID('[$SCHEMA_PROCESSED].[patients_clean]', 'U') IS NOT NULL
    DROP TABLE [$SCHEMA_PROCESSED].[patients_clean];

WITH patient_cleaning AS (
    SELECT 
        Id,
        LTRIM(RTRIM(UPPER(COALESCE(FIRST, 'UNKNOWN')))) as first_name_clean,
        LTRIM(RTRIM(UPPER(COALESCE(LAST, 'UNKNOWN')))) as last_name_clean,
        
        CASE 
            WHEN GENDER IN ('M', 'Male', 'MALE', 'm') THEN 'MALE'
            WHEN GENDER IN ('F', 'Female', 'FEMALE', 'f') THEN 'FEMALE'
            ELSE 'UNKNOWN'
        END as gender_standard,
        
        CASE 
            WHEN BIRTHDATE IS NULL OR BIRTHDATE > CAST(GETDATE() AS DATE)
            THEN CAST('1900-01-01' AS DATE)
            ELSE BIRTHDATE 
        END as birthdate_clean,
        
        LTRIM(RTRIM(UPPER(COALESCE(RACE, 'UNKNOWN')))) as race_normalized,
        LTRIM(RTRIM(UPPER(COALESCE(STATE, 'UNKNOWN')))) as state_normalized,
        
        CASE 
            WHEN ZIP IS NULL OR ZIP < 10000 OR ZIP > 99999 
            THEN 0 
            ELSE ZIP 
        END as zip_validated
        
    FROM [$SCHEMA_RAW].[patients]
    WHERE Id IS NOT NULL
)
SELECT *
INTO [$SCHEMA_PROCESSED].[patients_clean]
FROM patient_cleaning;"

END_TIME=$(date +%s)
TRANSFORM1_TIME=$((END_TIME-START_TIME))
TRANSFORM1_STATUS=$?
echo "Transform 1 completed in $TRANSFORM1_TIME seconds"
echo "========================================"

sleep 10

# Transform 2: Aggregation & Denormalization (no STRING_AGG for compatibility)
echo "Transform 2: Aggregation & Denormalization"
START_TIME=$(date +%s)

run_transform_with_check "
SET NOCOUNT ON;
SET RESULT_SET_CACHING OFF;
IF OBJECT_ID('[$SCHEMA_PROCESSED].[medications_normalized]', 'U') IS NOT NULL
    DROP TABLE [$SCHEMA_PROCESSED].[medications_normalized];

WITH medication_normalization AS (
    SELECT 
        PATIENT,
        ENCOUNTER,
        
        CASE 
            WHEN START IS NULL OR START > GETDATE() 
            THEN CAST('1900-01-01 00:00:00' AS DATETIME2)
            ELSE START 
        END as start_normalized,
        
        CASE 
            WHEN STOP IS NULL OR STOP < START 
            THEN NULL
            ELSE STOP 
        END as stop_validated,
        
        CAST(CODE AS NVARCHAR(MAX)) as code_standard,
        LTRIM(RTRIM(COALESCE(DESCRIPTION, 'Unknown Medication'))) as description_clean,
        
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
        
        CASE 
            WHEN DISPENSES IS NULL OR DISPENSES <= 0 
            THEN 1
            ELSE DISPENSES 
        END as dispenses_validated,
        
        CASE 
            WHEN DISPENSES IS NULL OR DISPENSES = 0 OR TOTALCOST IS NULL 
            THEN 0.0
            ELSE ROUND(CAST(TOTALCOST AS FLOAT) / CAST(DISPENSES AS FLOAT), 4)
        END as cost_per_dispense,
        
        CAST(COALESCE(REASONCODE, 0) AS NVARCHAR(MAX)) as reason_code_standard
        
    FROM [$SCHEMA_RAW].[medications]
    WHERE PATIENT IS NOT NULL 
        AND CODE IS NOT NULL
        AND START IS NOT NULL
)
SELECT *
INTO [$SCHEMA_PROCESSED].[medications_normalized]
FROM medication_normalization;"

END_TIME=$(date +%s)
TRANSFORM2_TIME=$((END_TIME-START_TIME))
#TRANSFORM2_STATUS=$?
echo "Transform 2 completed in $TRANSFORM2_TIME seconds"
echo "========================================"

sleep 10

# Transform 3: Procedures Enrichment & Standardization
echo "Transform 3: Procedures Enrichment & Standardization"
START_TIME=$(date +%s)

run_transform_with_check "
SET NOCOUNT ON;
SET RESULT_SET_CACHING OFF;
IF OBJECT_ID('[$SCHEMA_PROCESSED].[procedures_enriched]', 'U') IS NOT NULL
    DROP TABLE [$SCHEMA_PROCESSED].[procedures_enriched];

WITH procedure_normalization AS (
    SELECT
        PATIENT,
        ENCOUNTER,
        
        /* 
        ================== V1: synthea_latest and 2k ==================
        -- uses START/STOP/SYSTEM
        CASE
            WHEN START IS NULL OR CAST(START AS DATE) > CAST(GETDATE() AS DATE)
            THEN CAST('1900-01-01' AS DATE)
            ELSE CAST(START AS DATE)
        END as start_normalized,

        CASE
            WHEN STOP IS NOT NULL AND START IS NOT NULL AND STOP > START
            THEN DATEDIFF(MINUTE, START, STOP)
            ELSE 0
        END as procedure_duration_minutes,

        CASE
            WHEN UPPER(SYSTEM) LIKE '%SNOMED%' THEN 'SNOMED_CT'
            WHEN UPPER(SYSTEM) LIKE '%CPT%' THEN 'CPT'
            WHEN UPPER(SYSTEM) LIKE '%ICD%' THEN 'ICD'
            ELSE 'OTHER'
        END as coding_system_standard,
        */
    
        -- ================== V2: 10k and 100k ==================
        -- uses DATE and  CODE classification
        CASE
            WHEN DATE IS NULL OR CAST(DATE AS DATE) > CAST(GETDATE() AS DATE)
            THEN CAST('1900-01-01' AS DATE)
            ELSE CAST(DATE AS DATE)
        END as date_normalized,

        0 as procedure_duration_minutes,

        CASE
            WHEN UPPER(CODE) LIKE '%SNOMED%' THEN 'SNOMED_CT'
            WHEN UPPER(CODE) LIKE '%CPT%' THEN 'CPT'
            WHEN UPPER(CODE) LIKE '%ICD%' THEN 'ICD'
            ELSE 'UNKNOWN'
        END as coding_system_standard,
        
        CAST(CODE AS NVARCHAR(MAX)) as code_standard,
        CAST(COALESCE(REASONCODE, 0) AS NVARCHAR(MAX)) as reason_code_standard,
        
        CASE
            WHEN BASE_COST IS NULL OR BASE_COST < 0
            THEN 0.0
            ELSE ROUND(BASE_COST, 2)
        END as cost_validated,
        
        CASE
            WHEN BASE_COST <= 50 THEN 'SIMPLE'
            WHEN BASE_COST <= 500 THEN 'MODERATE' 
            WHEN BASE_COST <= 2000 THEN 'COMPLEX'
            ELSE 'HIGHLY_COMPLEX'
        END as procedure_complexity,
        
        LTRIM(RTRIM(UPPER(COALESCE(DESCRIPTION, 'UNKNOWN_PROCEDURE')))) as description_clean,
        LTRIM(RTRIM(UPPER(COALESCE(REASONDESCRIPTION, 'NO_REASON')))) as reason_clean
        
    FROM [$SCHEMA_RAW].[procedures]
    WHERE PATIENT IS NOT NULL AND CODE IS NOT NULL
)
SELECT *
INTO [$SCHEMA_PROCESSED].[procedures_enriched]
FROM procedure_normalization;"
    


END_TIME=$(date +%s)
echo "Transform 3 completed in $((END_TIME-START_TIME)) seconds"
sleep 10 
END_TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%S")


# capture row counts
#echo "Getting row counts for verification..."
ROW_COUNTS_OUTPUT=$(sqlcmd -S "$SERVER_NAME" -d "$DATABASE" -U "$SQL_USER" -P "$SQL_PASS" -I -h -1 -Q "
SELECT 'patients_clean' as table_name, COUNT(*) as rows FROM [processed].[patients_clean]
UNION ALL
SELECT 'medications_normalized', COUNT(*) FROM [processed].[medications_normalized]
UNION ALL
SELECT 'procedures_enriched', COUNT(*) FROM [processed].[procedures_enriched];" 2>/dev/null)

#echo "DEBUG Row counts output:"
#echo "$ROW_COUNTS_OUTPUT" | cat -A

# associative array with row counts (no verbose logging)
declare -A row_counts
while read -r table rows; do
    table=$(echo "$table" | xargs | tr -d '\r\n')
    rows=$(echo "$rows" | xargs | tr -d '\r\n')
    
    # Skip metadata
    if [[ "$table" =~ ^[0-9]*$ ]] || [[ "$table" =~ "rows affected" ]] || [[ -z "$table" ]]; then
        continue
    fi
    
    if [[ -n "$table" && "$rows" =~ ^[0-9]+$ ]]; then
        row_counts["$table"]=$rows
    fi
done <<< "$ROW_COUNTS_OUTPUT"


echo "========================================"

END_TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%S")
TOTAL_TRANSFORM_TIME=$((TRANSFORM1_TIME + TRANSFORM2_TIME + TRANSFORM3_TIME))
SUCCESSFUL_TRANSFORMS=$(( (1-TRANSFORM1_STATUS) + (1-TRANSFORM2_STATUS) + (1-TRANSFORM3_STATUS) ))


echo ""
echo "=================== TRANSFORM JOB TIMES ===================="
echo "Retrieving actual transform execution times from Synapse..."

if [ -f "./sy_metrics.sh" ]; then
    metrics_data=$(bash ./sy_metrics.sh "$START_TIMESTAMP" "$END_TIMESTAMP" "TRANSFORM" "$SCHEMA_PROCESSED" "3" "$DATASET_SIZE")
    
    {
        echo "$metrics_data" | head -n1
        echo "$metrics_data" | tail -n +2 | awk -F',' -v OFS=',' \
            -v pc="${row_counts[patients_clean]}" \
            -v mn="${row_counts[medications_normalized]}" \
            -v pe="${row_counts[procedures_enriched]}" \
            '{
            if ($5 == "patients_clean") $3=pc
            else if ($5 == "medications_normalized") $3=mn
            else if ($5 == "procedures_enriched") $3=pe
            print
        }'
    } | column -t -s, -o '   '
    
    total_transform_time=$(echo "$metrics_data" | awk -F',' 'NR>1 {sum+=$2} END {print sum+0}')
    successful_transforms_metrics=$(echo "$metrics_data" | awk -F',' 'NR>1 {count++} END {print count+0}')
else
    echo "sy_metrics.sh not found, using script timing"
    total_transform_time=$TOTAL_TRANSFORM_TIME
    successful_transforms_metrics=$SUCCESSFUL_TRANSFORMS
fi

echo ""
echo "=================== TRANSFORM METRICS ====================="
echo "Transforms attempted: 3"
echo "Transforms successfully completed: $SUCCESSFUL_TRANSFORMS"
echo "Total Synapse transform processing time: $total_transform_time seconds"

echo ""
echo "=================== TRANSFORM COST ==========================="
echo "Calling sy_cost.sh..."
if [ -f "./sy_cost.sh" ]; then
    bash ./sy_cost.sh "$START_TIMESTAMP" "$END_TIMESTAMP" "TRANSFORM" "$SCHEMA_PROCESSED" "3" "$DATASET_SIZE"
else
    echo "sy_cost.sh not found, skipping cost calculation"
fi



ts=$(date -u +"%Y-%m-%d %H:%M:%S")
echo "$metrics_data" | awk -F',' -v ds="$DATASET_SIZE" -v ts="$ts" '
BEGIN {OFS=","}
NR>1 {
    print "Synapse", ds, "TRANSFORM", $1, $2, $3, $4, "", $5, $6, ts
}' >> sy_results.csv
