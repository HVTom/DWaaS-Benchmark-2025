#!/bin/bash


SYNAPSE_WORKSPACE="dwbenchmarkworkspace"
SQL_POOL="dwbenchmarkdedicatedpool"
SERVER_NAME="${SYNAPSE_WORKSPACE}.sql.azuresynapse.net"
DATABASE="$SQL_POOL"
SQL_USER="sqladminuser"
SQL_PASS="j7A+2,;}xVm=p&5"

SCHEMA="raw"
OPERATION_TYPE="QUERY"


DATASET_SIZE=$1

# function needed to keep terminal output formating
run_query_with_headers() {
    local query="$1"
    local query_name="$2"
    
    echo "$query_name"
    # avoid using -h -1 flasgs to keep header
    sqlcmd -S "$SERVER_NAME" -d "$DATABASE" -U "$SQL_USER" -P "$SQL_PASS" \
           -I -W -s "|" -Q "SET NOCOUNT ON; $query" | \
    grep -v "^(" | grep -v "rows affected" | grep -v "^$" | \
    column -t -s "|"
}

echo "=== SYNAPSE QUERY PERFORMANCE BENCHMARK ==="
echo "Schema: $SCHEMA"
echo "Synapse Pool: $SQL_POOL"
echo "========================================"

START_TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%S")

# Query 1 - Simple scan
echo "Running Query 1: Simple Scan..."
START_TIME=$(date +%s)

run_query_with_headers "
-- Query 1: Basic Aggregation pentru Synapse (FIXED - CAST to MAX)
SET RESULT_SET_CACHING OFF;
WITH base_aggregation AS (
  SELECT
    p.GENDER,
    p.RACE,
    p.STATE,
    p.COUNTY,
    COUNT(DISTINCT e.Id) as total_encounters,
    -- explicit casting for these aggregation, unlike BigQuery
    AVG(CAST(e.TOTAL_CLAIM_COST AS FLOAT)) as avg_cost,
    MAX(e.START) as latest_encounter,
    STDEV(CAST(e.TOTAL_CLAIM_COST AS FLOAT)) as cost_std_dev,
    SUM(CAST(e.TOTAL_CLAIM_COST AS FLOAT)) as total_cost,
    COUNT(DISTINCT p.Id) as unique_patients,
    COUNT(DISTINCT e.ENCOUNTERCLASS) as encounter_types,
    MIN(CAST(e.TOTAL_CLAIM_COST AS FLOAT)) as min_cost,
    MAX(CAST(e.TOTAL_CLAIM_COST AS FLOAT)) as max_cost,
    COUNT(DISTINCT YEAR(e.START)) as years_with_encounters,
    
    -- forced to CAST to NVARCHAR(MAX) to dodge the 8000 bytes processing limit/warning
    STRING_AGG(CAST(e.ENCOUNTERCLASS AS NVARCHAR(MAX)), ', ') 
      -- withing gorup + order by is mandatory in T-SQl
      -- order is needed so it knows in which order to process the values
      WITHIN GROUP (ORDER BY e.ENCOUNTERCLASS) as encounter_classes
  FROM raw.patients p
  JOIN raw.encounters e ON p.Id = e.PATIENT
  WHERE e.START >= '2018-01-01'
  GROUP BY p.GENDER, p.RACE, p.STATE, p.COUNTY
)
SELECT *,
  total_cost / unique_patients as cost_per_patient,
  (max_cost - min_cost) as cost_range,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg_cost) 
    OVER (PARTITION BY GENDER) as median_cost_by_gender,
  RANK() OVER (ORDER BY total_encounters DESC) as encounter_rank
FROM base_aggregation
ORDER BY total_encounters DESC, avg_cost DESC;"

END_TIME=$(date +%s)
QUERY1_TIME=$((END_TIME-START_TIME))
echo "Query 1 completed in $QUERY1_TIME seconds"
echo "========================================"

sleep 5

# Query 2 - Multi-Table Analysis
echo "Running Query 2: Multi-Table Analysis..."
START_TIME=$(date +%s)

run_query_with_headers "
-- Query 2: Multi-Table Analysis
SET RESULT_SET_CACHING OFF;
WITH encounter_medication_matrix AS (
  SELECT
    e.PATIENT,
    e.Id as encounter_id,
    e.START as encounter_date,
    e.ENCOUNTERCLASS,
    m.CODE as medication_code,
    m.DESCRIPTION as medication_desc,
    m.START as medication_start,
    CAST(m.TOTALCOST AS FLOAT) as medication_cost,
    DATEDIFF(DAY, e.START, m.START) as days_diff,
    CONCAT(e.ENCOUNTERCLASS, '_', LEFT(m.DESCRIPTION, 20)) as encounter_med_key,
    LEN(m.DESCRIPTION) as description_length,
    CASE
      -- ISNULL instead of coalesce for simplicity
      WHEN m.START BETWEEN e.START AND ISNULL(e.STOP, DATEADD(DAY, 30, e.START))
      THEN 'CONCURRENT'
      WHEN m.START > ISNULL(e.STOP, DATEADD(DAY, 30, e.START))
      THEN 'AFTER_ENCOUNTER'
      ELSE 'BEFORE_ENCOUNTER'
    END as timing_relationship
  FROM raw.encounters e
  JOIN raw.medications m ON e.PATIENT = m.PATIENT
  -- here simple sintax is used on column with reserved keywork
  WHERE e.START >= '2018-01-01'
),
patient_complexity_score AS (
  SELECT
    PATIENT,
    COUNT(DISTINCT encounter_id) as total_encounters,
    COUNT(DISTINCT medication_code) as unique_medications,
    AVG(medication_cost) as avg_medication_cost,
    MIN(medication_cost) as min_medication_cost,
    MAX(medication_cost) as max_medication_cost,
    COUNT(DISTINCT timing_relationship) as timing_patterns_count,
    AVG(CAST(description_length AS FLOAT)) as avg_description_length,
    STDEV(medication_cost) as cost_std_dev,
    (COUNT(DISTINCT encounter_id) * 0.3 + 
     COUNT(DISTINCT medication_code) * 0.7) as complexity_score
  FROM encounter_medication_matrix
  GROUP BY PATIENT
)
SELECT
  p.GENDER,
  p.RACE,
  p.STATE,
  p.COUNTY,
  AVG(pcs.complexity_score) as avg_complexity,
  COUNT(*) as patient_count,
  SUM(pcs.total_encounters) as total_encounters,
  SUM(pcs.unique_medications) as total_medications,
  AVG(pcs.cost_std_dev) as avg_cost_variability,
  AVG(pcs.max_medication_cost - pcs.min_medication_cost) as avg_cost_range,
  AVG(pcs.avg_description_length) as avg_description_length,
  RANK() OVER (ORDER BY AVG(pcs.complexity_score) DESC) as complexity_rank
FROM raw.patients p
JOIN patient_complexity_score pcs ON p.Id = pcs.PATIENT
GROUP BY p.GENDER, p.RACE, p.STATE, p.COUNTY
ORDER BY avg_complexity DESC, complexity_rank ASC;"

END_TIME=$(date +%s)
QUERY2_TIME=$((END_TIME-START_TIME))
echo "Query 2 completed in $QUERY2_TIME seconds"
echo "========================================"

sleep 5

# Query 3 - Condition Progression Analysis 
echo "Running Query 3: Condition Progression Analysis..."
START_TIME=$(date +%s)

run_query_with_headers "
-- Query 3: Condition Progression Analysis
SET RESULT_SET_CACHING OFF;
WITH condition_sequences AS (
  SELECT
    c1.PATIENT,
    c1.CODE as condition1_code,
    c1.DESCRIPTION as condition1_desc,
    c1.START as condition1_start,
    c2.CODE as condition2_code,
    c2.DESCRIPTION as condition2_desc,
    c2.START as condition2_start,
    DATEDIFF(DAY, c1.START, c2.START) as days_between_conditions,
    -- explicit cast for CONCAT
    CONCAT(CAST(c1.CODE AS VARCHAR), '_TO_', CAST(c2.CODE AS VARCHAR)) as transition_key,
    LEN(c1.DESCRIPTION) + LEN(c2.DESCRIPTION) as combined_desc_length,
    CASE
      WHEN DATEDIFF(DAY, c1.START, c2.START) <= 30 THEN 'RAPID'
      WHEN DATEDIFF(DAY, c1.START, c2.START) <= 90 THEN 'MODERATE'
      ELSE 'SLOW'
    END as progression_speed
  FROM raw.conditions c1
  JOIN raw.conditions c2
    ON c1.PATIENT = c2.PATIENT
    AND c2.START > c1.START
    AND c2.START <= DATEADD(DAY, 100, c1.START)
    AND c1.CODE != c2.CODE
),
condition_networks AS (
  SELECT
    condition1_code,
    condition2_code,
    COUNT(*) as co_occurrence_count,
    AVG(CAST(days_between_conditions AS FLOAT)) as avg_days_between,
    STDEV(CAST(days_between_conditions AS FLOAT)) as std_days_between,
    COUNT(DISTINCT PATIENT) as affected_patients,
    MIN(days_between_conditions) as min_days_between,
    MAX(days_between_conditions) as max_days_between,
    AVG(CAST(combined_desc_length AS FLOAT)) as avg_desc_length,
    COUNT(DISTINCT progression_speed) as speed_varieties,
    
    -- STRING_AGG with distinct not supported in synapse/t-sql
    STRING_AGG(CAST(transition_key AS NVARCHAR(MAX)), ' | ') as transition_patterns,
    STRING_AGG(CAST(progression_speed AS NVARCHAR(MAX)), ',') as speed_distribution
  FROM condition_sequences
  GROUP BY condition1_code, condition2_code
  HAVING COUNT(*) >= 2
)
SELECT
  cn.*,
  (cn.max_days_between - cn.min_days_between) as days_range,
  CASE 
    WHEN cn.avg_days_between = 0 THEN 0
    ELSE cn.std_days_between / cn.avg_days_between 
  END as coefficient_of_variation,
  RANK() OVER (ORDER BY co_occurrence_count DESC) as frequency_rank,
  PERCENT_RANK() OVER (ORDER BY avg_days_between) as timing_percentile,
  DENSE_RANK() OVER (ORDER BY affected_patients DESC) as patient_impact_rank
FROM condition_networks cn
ORDER BY co_occurrence_count DESC, frequency_rank ASC, affected_patients DESC;"

END_TIME=$(date +%s)
QUERY3_TIME=$((END_TIME-START_TIME))
echo "Query 3 completed in $QUERY3_TIME seconds"
echo "========================================"

sleep 5

# Query 4 - Observations Volume Analysis (Complete Scan Stress Test)
echo "Running Query 4: Observations Volume Analysis (Complete Scan Stress Test)..."
START_TIME=$(date +%s)

run_query_with_headers "
-- Query 4: Enhanced Observations Analysis (Synapse Compatible - ECHIVALENT REAL)
SET RESULT_SET_CACHING OFF;
WITH patient_observation_timeline AS (
  SELECT
    o.PATIENT,
    o.CODE,
    o.DESCRIPTION,
    o.VALUE,
    o.DATE,
    p.GENDER,
    p.RACE,
    
    -- Window functions exactly like BigQuery
    LAG(o.VALUE) OVER (PARTITION BY o.PATIENT, o.CODE ORDER BY o.DATE) as prev_value,
    LEAD(o.VALUE) OVER (PARTITION BY o.PATIENT, o.CODE ORDER BY o.DATE) as next_value,
    ROW_NUMBER() OVER (PARTITION BY o.PATIENT ORDER BY o.DATE DESC) as obs_rank,
    DENSE_RANK() OVER (ORDER BY o.DATE) as global_date_rank,
    
    -- Window functions cfor slowdown testing
    -- reduced drastically because of the time needed for processing
    COUNT(*) OVER (
      PARTITION BY p.GENDER 
      ORDER BY o.DATE 
      ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING
    ) as massive_rolling_count_6_window,
    
    AVG(CAST(LEN(o.DESCRIPTION) AS FLOAT)) OVER (
      PARTITION BY o.CODE 
      ORDER BY o.DATE 
      ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING
    ) as rolling_avg_desc_length_6_window,
    
    -- STRING_AGG window function equivalent to BigQuery version
    COUNT(CASE WHEN LEN(ISNULL(o.VALUE, '')) > 0 THEN 1 END) OVER (
      PARTITION BY p.RACE 
      ORDER BY o.DATE 
      ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING
    ) as rolling_value_concat_6_window
    
  FROM raw.observations o
  JOIN raw.patients p ON o.PATIENT = p.Id
),
complex_aggregations AS (
  SELECT
    GENDER,
    RACE,
    CODE,
    COUNT(*) as total_observations,
    COUNT(DISTINCT PATIENT) as unique_patients,
    -- VALUE NOT LIKE '%[^0-9.-]% - REGEXP_CONTAINS equivalent
    AVG(CASE 
        WHEN ISNUMERIC(VALUE) = 1 AND VALUE NOT LIKE '%[^0-9.-]%' 
        THEN TRY_CAST(VALUE AS NUMERIC) 
        END) as avg_numeric_value,
    STRING_AGG(CAST(DESCRIPTION AS NVARCHAR(MAX)), '||') 
    WITHIN GROUP (ORDER BY DESCRIPTION) as all_descriptions,
    -- TIMESTAMP_DIFF din BigQuery
    DATEDIFF(DAY, MIN(DATE), MAX(DATE)) as observation_span_days,
    
    -- ECHIVALENT cu BigQuery
    AVG(CAST(massive_rolling_count_6_window AS FLOAT)) as avg_massive_rolling_count,
    MAX(rolling_avg_desc_length_6_window) as max_rolling_desc_length,
    -- ECHIVALENT cu COUNT(DISTINCT rolling_value_concat_6_window)
    -- COUNT(DISTINCT) pe COUNT window function va da rezultate similare
    COUNT(DISTINCT rolling_value_concat_6_window) as unique_rolling_concatenations
    
  FRVING COUNT(*) > 10
)
SELECT * FROM complex_aggregations
ORDER BY total_observations DESC, avg_massive_rolling_count DESC;"


# END_TIME=$(date +%s)
# QUERY4_TIME=$((END_TIME-START_TIME))
# echo "Query 4 completed in $QUERY4_TIME seconds"
# echo "========================================"
# sleep 5OM patient_observation_timeline
#   GROUP BY GENDER, RACE, CODE
#   HAVING COUNT(*) > 10
# )
# SELECT * FROM complex_aggregations
# ORDER BY total_observations DESC, avg_massive_rolling_count DESC;"


END_TIME=$(date +%s)
QUERY4_TIME=$((END_TIME-START_TIME))
echo "Query 4 completed in $QUERY4_TIME seconds"
echo "========================================"
sleep 5
END_TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%S")
TOTAL_QUERY_TIME=$((QUERY1_TIME + QUERY2_TIME + QUERY3_TIME))


echo ""
echo "=================== QUERY JOB TIMES =========================="
echo "Retrieving actual job execution times from Synapse..."

if [ -f "./sy_metrics.sh" ]; then
    metrics_data=$(bash ./sy_metrics.sh "$START_TIMESTAMP" "$END_TIMESTAMP" "$OPERATION_TYPE" "" "4" "$DATASET_SIZE")
    echo "$metrics_data" | column -t -s,
    
    total_query_time=$(echo "$metrics_data" | awk -F',' 'NR>1 {sum+=$2} END {print sum+0}')
    successful_queries=$(echo "$metrics_data" | awk -F',' 'NR>1 {count++} END {print count+0}')
else
    echo "sy_metrics.sh not found, using script timing"
    total_query_time=$TOTAL_QUERY_TIME
    successful_queries=3
fi

echo ""
echo "=================== SYNAPSE QUERY METRICS ====================="
echo "Queries attempted: 4"
echo "Queries successfully completed: $successful_queries"
echo "Total Synapse query processing time: $total_query_time seconds"

echo ""
echo "=================== QUERY COST ==========================="
echo "Calling sy_cost.sh..."
if [ -f "./sy_cost.sh" ]; then
    bash ./sy_cost.sh "$START_TIMESTAMP" "$END_TIMESTAMP" "$OPERATION_TYPE" "" "4" "$DATASET_SIZE"
else
    echo "sy_cost.sh not found, skipping cost calculation"
fi



ts=$(date -u +"%Y-%m-%d %H:%M:%S")
echo "$metrics_data" | awk -F',' -v ds="$DATASET_SIZE" -v ts="$ts" '
BEGIN {OFS=","}
NR>1 {
    print "Synapse", ds, "QUERY", $1, $2, $3, $4, "", $5, $6, ts
}' >> sy_results.csv

