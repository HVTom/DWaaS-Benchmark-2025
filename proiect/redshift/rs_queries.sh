#!/bin/bash

DATASET="dataset_raw"
OPERATION_TYPE="QUERY"
DATASET_SIZE=$1

# Redshift connection details
REDSHIFT_HOST="redshift-benchmark-cluster-1.csag37dwinrq.eu-central-1.redshift.amazonaws.com"
REDSHIFT_DB="benchmark"
REDSHIFT_USER="awsuser"
REDSHIFT_PORT="5439"
export PGPASSWORD='?Gn+3&t&9}vS!:)'
export PAGER=""

echo "=== QUERY PERFORMANCE BENCHMARK ==="
echo "Dataset: $DATASET"
echo "========================================"

START_TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%S")


# the scripts are intented to have the same effect as the BigQuery version

# Query 1: Basic Aggregation 
# percentile_data CTE is eliminated and  window function had to be used inside SELECT
echo "Running Query 1: Simple Scan..."
START_TIME=$(date +%s)
psql -h $REDSHIFT_HOST -p $REDSHIFT_PORT -U $REDSHIFT_USER -d $REDSHIFT_DB -c "

SET enable_result_cache_for_session TO off;

WITH base_data AS (
  SELECT
    p."GENDER", p."RACE", p."STATE", p."COUNTY",
    e."Id" as encounter_id,
    p."Id" as patient_id,
    e."ENCOUNTERCLASS",
    CASE 
      WHEN e."TOTAL_CLAIM_COST" IS NOT NULL AND e."TOTAL_CLAIM_COST" != '' 
      THEN e."TOTAL_CLAIM_COST"::FLOAT8
      ELSE NULL 
    END as clean_cost,
    -- operator :: used for type conversion
    DATE_PART('year', e."START"::timestamp) as encounter_year,
    e."START"::timestamp as encounter_start
  FROM ${DATASET}.patients p
  JOIN ${DATASET}.encounters e ON p."Id" = e."PATIENT"
  WHERE e."START"::date >= '2018-01-01'
),

base_aggregation AS (
  SELECT
    "GENDER", "RACE", "STATE", "COUNTY",
    COUNT(DISTINCT encounter_id) as total_encounters,
    AVG(clean_cost) as avg_cost,
    MAX(encounter_start) as latest_encounter,
    STDDEV(clean_cost) as cost_std_dev,
    SUM(COALESCE(clean_cost, 0)) as total_cost,
    COUNT(DISTINCT patient_id) as unique_patients,
    COUNT(DISTINCT "ENCOUNTERCLASS") as encounter_types,
    MIN(clean_cost) as min_cost,
    MAX(clean_cost) as max_cost,
    COUNT(DISTINCT encounter_year) as years_with_encounters
  FROM base_data
  GROUP BY "GENDER", "RACE", "STATE", "COUNTY"
),

encounter_classes AS (
  SELECT
    "GENDER", "RACE", "STATE", "COUNTY",
    LISTAGG(DISTINCT "ENCOUNTERCLASS", ', ') 
    WITHIN GROUP (ORDER BY "ENCOUNTERCLASS") as encounter_classes
  FROM base_data
  GROUP BY "GENDER", "RACE", "STATE", "COUNTY"
)

SELECT 
  ba.*,
  ec.encounter_classes,
  CASE WHEN ba.unique_patients > 0 THEN ba.total_cost / ba.unique_patients ELSE 0 END as cost_per_patient,
  (ba.max_cost - ba.min_cost) as cost_range,
  
  -- SOLUȚIA: Median pe avg_cost calculat (ca BigQuery)
  MEDIAN(ba.avg_cost) OVER (PARTITION BY ba."GENDER") as median_cost_by_gender,
  
  RANK() OVER (ORDER BY ba.total_encounters DESC) as encounter_rank
FROM base_aggregation ba
LEFT JOIN encounter_classes ec ON ba."GENDER" = ec."GENDER" 
  AND ba."RACE" = ec."RACE"
  AND ba."STATE" = ec."STATE" 
  AND ba."COUNTY" = ec."COUNTY"
ORDER BY ba.total_encounters DESC, ba.avg_cost DESC;"


sleep 5

# Query 2 - Aggregations 
echo "Running Query 2: Aggregation..."
START_TIME=$(date +%s)
psql -h $REDSHIFT_HOST -p $REDSHIFT_PORT -U $REDSHIFT_USER -d $REDSHIFT_DB -c "
-- Query 2: Multi-Table Analysis
SET enable_result_cache_for_session TO off;

WITH encounter_medication_matrix AS (
  SELECT
    e."PATIENT",
    e."Id" as encounter_id,
    e."START"::timestamp as encounter_date,
    e."ENCOUNTERCLASS",
    m."CODE" as medication_code,
    m."DESCRIPTION" as medication_desc,
    m."START"::timestamp as medication_start,
    CASE 
        WHEN m."TOTALCOST" IS NOT NULL AND m."TOTALCOST" != ''
        THEN m."TOTALCOST"::FLOAT8
        ELSE 0 
    END as medication_cost,
    -- Redshift uses similar syntax to the classic SQL used by BigQuery, with `day` in front
    DATEDIFF('day', e."START"::timestamp, m."START"::timestamp) as days_diff,
    (e."ENCOUNTERCLASS" || '_' || LEFT(m."DESCRIPTION", 20)) as encounter_med_key,
    LENGTH(m."DESCRIPTION") as description_length,
    CASE
      WHEN m."START"::timestamp BETWEEN e."START"::timestamp 
        AND COALESCE(e."STOP"::timestamp, e."START"::timestamp + INTERVAL '30 days')
      THEN 'CONCURRENT'
      WHEN m."START"::timestamp > COALESCE(e."STOP"::timestamp, e."START"::timestamp + INTERVAL '30 days')
      THEN 'AFTER_ENCOUNTER'
      ELSE 'BEFORE_ENCOUNTER'
    END as timing_relationship
  FROM ${DATASET}.encounters e
  JOIN ${DATASET}.medications m ON e."PATIENT" = m."PATIENT"
  WHERE e."START"::date >= '2018-01-01'
),

-- CTE 1: only aggregation with DISTINCT (no PERCENTILE_CONT/LISTAGG), 
-- had to be simplified, didn't work as for BigQuery
patient_base_stats AS (
  SELECT
    "PATIENT",
    COUNT(DISTINCT encounter_id) as total_encounters,
    COUNT(DISTINCT medication_code) as unique_medications,
    COUNT(DISTINCT timing_relationship) as timing_patterns_count,
    (COUNT(DISTINCT encounter_id) * 0.3 + COUNT(DISTINCT medication_code) * 0.7) as complexity_score
  FROM encounter_medication_matrix
  GROUP BY "PATIENT"
),

-- CTE 2: simple aggregations here too  
patient_numeric_stats AS (
  SELECT
    "PATIENT",
    AVG(medication_cost) as avg_medication_cost,
    STDDEV(medication_cost) as cost_std_dev,
    MIN(medication_cost) as min_medication_cost,
    MAX(medication_cost) as max_medication_cost,
    AVG(description_length::FLOAT8) as avg_description_length
  FROM encounter_medication_matrix
  GROUP BY "PATIENT"
),

-- CTE 3: extra simple operation, kep for adding a bit of computation, and to be fair with bigquery (same operation)
patient_percentiles AS (
  SELECT
    "PATIENT",
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY medication_cost) as median_cost
  FROM encounter_medication_matrix
  GROUP BY "PATIENT"
),

-- CTE 4: same as CTE3, duplicates a bigquery operation for fairness
patient_timing_patterns AS (
  SELECT
    "PATIENT",
    LISTAGG(DISTINCT timing_relationship, ' | ') 
    WITHIN GROUP (ORDER BY timing_relationship) as timing_patterns
  FROM encounter_medication_matrix
  GROUP BY "PATIENT"
)

SELECT
  p."GENDER",
  p."RACE",
  p."STATE", 
  p."COUNTY",
  AVG(pbs.complexity_score) as avg_complexity,
  COUNT(*) as patient_count,
  SUM(pbs.total_encounters) as total_encounters,
  SUM(pbs.unique_medications) as total_medications,
  AVG(pns.cost_std_dev) as avg_cost_variability,
  AVG(pns.max_medication_cost - pns.min_medication_cost) as avg_cost_range,
  AVG(pns.avg_description_length) as avg_description_length,
  RANK() OVER (ORDER BY AVG(pbs.complexity_score) DESC) as complexity_rank
FROM ${DATASET}.patients p
JOIN patient_base_stats pbs ON p."Id" = pbs."PATIENT"
LEFT JOIN patient_numeric_stats pns ON pbs."PATIENT" = pns."PATIENT"

-- same operations as in bigquery
LEFT JOIN patient_percentiles pp ON pbs."PATIENT" = pp."PATIENT"
LEFT JOIN patient_timing_patterns ptp ON pbs."PATIENT" = ptp."PATIENT"

GROUP BY p."GENDER", p."RACE", p."STATE", p."COUNTY"
ORDER BY avg_complexity DESC, complexity_rank ASC;"

sleep 5

# # Query 3 - Complex Join 
echo "Running Query 3: Complex Join..."
START_TIME=$(date +%s)
psql -h $REDSHIFT_HOST -p $REDSHIFT_PORT -U $REDSHIFT_USER -d $REDSHIFT_DB -c "
-- Query 3: Condition Progression Analysis pentru Redshift 
SET enable_result_cache_for_session TO off;

WITH condition_sequences AS (
  SELECT
    c1."PATIENT",
    c1."CODE" as condition1_code,
    c1."DESCRIPTION" as condition1_desc,
    c1."START"::timestamp as condition1_start,
    c2."CODE" as condition2_code,
    c2."DESCRIPTION" as condition2_desc,
    c2."START"::timestamp as condition2_start,
    DATEDIFF('day', c1."START"::timestamp, c2."START"::timestamp) as days_between_conditions,
    (c1."CODE"::text || '_TO_' || c2."CODE"::text) as transition_key,
    LENGTH(c1."DESCRIPTION") + LENGTH(c2."DESCRIPTION") as combined_desc_length,
    CASE
      WHEN DATEDIFF('day', c1."START"::timestamp, c2."START"::timestamp) <= 30 THEN 'RAPID'
      WHEN DATEDIFF('day', c1."START"::timestamp, c2."START"::timestamp) <= 90 THEN 'MODERATE'
      ELSE 'SLOW'
    END as progression_speed
  FROM ${DATASET}.conditions c1
  JOIN ${DATASET}.conditions c2
    ON c1."PATIENT" = c2."PATIENT"
    AND c2."START"::timestamp > c1."START"::timestamp
    AND c2."START"::timestamp <= c1."START"::timestamp + INTERVAL '365 days'
    AND c1."CODE" != c2."CODE"
),

condition_networks_base AS (
  SELECT
    condition1_code,
    condition2_code,
    COUNT(*) as co_occurrence_count,
    AVG(days_between_conditions) as avg_days_between,
    STDDEV(days_between_conditions) as std_days_between,
    COUNT(DISTINCT "PATIENT") as affected_patients,
    MIN(days_between_conditions) as min_days_between,
    MAX(days_between_conditions) as max_days_between,
    AVG(combined_desc_length) as avg_desc_length,
    COUNT(DISTINCT progression_speed) as speed_varieties
  FROM condition_sequences
  GROUP BY condition1_code, condition2_code
  HAVING COUNT(*) >= 2
),

-- we separate LISTAGGs inside different CTEs with the same ORDER BY
transition_patterns AS (
  SELECT
    condition1_code,
    condition2_code,
    LISTAGG(DISTINCT transition_key, ' | ') 
    WITHIN GROUP (ORDER BY condition1_code) as transition_patterns
  FROM condition_sequences
  GROUP BY condition1_code, condition2_code
  HAVING COUNT(*) >= 2
),

speed_patterns AS (
  SELECT
    condition1_code,
    condition2_code,
    LISTAGG(DISTINCT progression_speed, ',') 
    WITHIN GROUP (ORDER BY condition1_code) as speed_distribution
  FROM condition_sequences
  GROUP BY condition1_code, condition2_code
  HAVING COUNT(*) >= 2
)

SELECT
  cnb.*,
  tp.transition_patterns,
  sp.speed_distribution,
  (cnb.max_days_between - cnb.min_days_between) as days_range,
  CASE WHEN cnb.avg_days_between > 0 THEN cnb.std_days_between / cnb.avg_days_between ELSE 0 END as coefficient_of_variation,
  RANK() OVER (ORDER BY cnb.co_occurrence_count DESC) as frequency_rank,
  PERCENT_RANK() OVER (ORDER BY cnb.avg_days_between) as timing_percentile,
  DENSE_RANK() OVER (ORDER BY cnb.affected_patients DESC) as patient_impact_rank
FROM condition_networks_base cnb
LEFT JOIN transition_patterns tp ON cnb.condition1_code = tp.condition1_code
  AND cnb.condition2_code = tp.condition2_code
LEFT JOIN speed_patterns sp ON cnb.condition1_code = sp.condition1_code
  AND cnb.condition2_code = sp.condition2_code
ORDER BY cnb.co_occurrence_count DESC, frequency_rank ASC, cnb.affected_patients DESC;"

sleep 5

# Query 4 - Observations Volume Analysis (Complete Scan Stress Test)
# uses LIST_AGG intead of STRING_AGG
echo "Running Query 4: Observations Volume Analysis (Complete Scan Stress Test)..."
START_TIME=$(date +%s)

psql -h $REDSHIFT_HOST -p $REDSHIFT_PORT -U $REDSHIFT_USER -d $REDSHIFT_DB -c "
SET enable_result_cache_for_session TO off;

WITH patient_observation_timeline AS (
  SELECT
    o.\"PATIENT\",
    o.\"CODE\",
    o.\"DESCRIPTION\",
    o.\"VALUE\",
    o.\"DATE\"::timestamp,
    p.\"GENDER\",
    p.\"RACE\",
    LAG(o.\"VALUE\") OVER (PARTITION BY o.\"PATIENT\", o.\"CODE\" ORDER BY o.\"DATE\"::timestamp) as prev_value,
    LEAD(o.\"VALUE\") OVER (PARTITION BY o.\"PATIENT\", o.\"CODE\" ORDER BY o.\"DATE\"::timestamp) as next_value,
    ROW_NUMBER() OVER (PARTITION BY o.\"PATIENT\" ORDER BY o.\"DATE\"::timestamp DESC) as obs_rank,
    DENSE_RANK() OVER (ORDER BY o.\"DATE\"::timestamp) as global_date_rank,
    
    -- same frame sizes for fair benchmarking
    COUNT(*) OVER (
      PARTITION BY p.\"GENDER\"
      ORDER BY o.\"DATE\"::timestamp
      ROWS BETWEEN 500 PRECEDING AND 500 FOLLOWING
    ) as massive_rolling_count_1000_window,
    
    AVG(LENGTH(o.\"DESCRIPTION\")::FLOAT8) OVER (
      PARTITION BY o.\"CODE\"
      ORDER BY o.\"DATE\"::timestamp
      ROWS BETWEEN 400 PRECEDING AND 400 FOLLOWING
    ) as rolling_avg_desc_length_800_window
  FROM ${DATASET}.observations o
  JOIN ${DATASET}.patients p ON o.\"PATIENT\" = p.\"Id\"
),

complex_aggregations_base AS (
  SELECT
    \"GENDER\",
    \"RACE\",
    \"CODE\", 
    COUNT(*) as total_observations,
    COUNT(DISTINCT \"PATIENT\") as unique_patients,
    AVG(CASE 
        WHEN \"VALUE\" ~ '^[0-9]+\.?[0-9]*$' 
        THEN \"VALUE\"::NUMERIC 
        ELSE NULL 
    END) as avg_numeric_value,
    DATEDIFF('day', MIN(\"DATE\"::timestamp), MAX(\"DATE\"::timestamp)) as observation_span_days,
    AVG(massive_rolling_count_1000_window) as avg_massive_rolling_count,
    MAX(rolling_avg_desc_length_800_window) as max_rolling_desc_length
  FROM patient_observation_timeline
  GROUP BY \"GENDER\", \"RACE\", \"CODE\"
  HAVING COUNT(*) > 10
),

description_aggregations AS (
  SELECT
    \"GENDER\",
    \"RACE\", 
    \"CODE\",
    LISTAGG(DISTINCT \"DESCRIPTION\", '||') 
    WITHIN GROUP (ORDER BY \"DESCRIPTION\") as all_descriptions
  FROM patient_observation_timeline
  GROUP BY \"GENDER\", \"RACE\", \"CODE\"
  HAVING COUNT(*) > 10
),

-- simulates unique_rolling_concatenations without STRING_AGG/LIST_AGG
rolling_concat_simulation AS (
  SELECT
    \"GENDER\",
    \"RACE\",
    \"CODE\",
    COUNT(DISTINCT LEFT(\"VALUE\", 5)) as unique_rolling_concatenations
  FROM patient_observation_timeline
  GROUP BY \"GENDER\", \"RACE\", \"CODE\"
  HAVING COUNT(*) > 10
)

SELECT
  cab.*,
  da.all_descriptions,
  rcs.unique_rolling_concatenations
FROM complex_aggregations_base cab
LEFT JOIN description_aggregations da ON cab.\"GENDER\" = da.\"GENDER\"
  AND cab.\"RACE\" = da.\"RACE\"
  AND cab.\"CODE\" = da.\"CODE\"
LEFT JOIN rolling_concat_simulation rcs ON cab.\"GENDER\" = rcs.\"GENDER\"
  AND cab.\"RACE\" = rcs.\"RACE\"
  AND cab.\"CODE\" = rcs.\"CODE\"
ORDER BY cab.total_observations DESC, cab.avg_massive_rolling_count DESC;"

END_TIME=$(date +%s)
echo "Query 4 completed in $((END_TIME-START_TIME)) seconds"
sleep 5
END_TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%S")

echo -e "\n=================== QUERY JOB TIMES ==========================="
echo "Retrieving actual job execution times from Redshift..."
sleep 2

if [ -f "./rs_metrics.sh" ]; then
    metrics_data=$(bash ./rs_metrics.sh "$START_TIMESTAMP" "$END_TIMESTAMP" "$OPERATION_TYPE" "" "4" "$DATASET_SIZE")
    # display and parse
    echo "$metrics_data" | column -t -s,
    total_query_time=$(echo "$metrics_data" | awk -F',' 'NR>1 {sum+=$2} END {print sum+0}')
    successful_queries=$(echo "$metrics_data" | awk -F',' 'NR>1 {count++} END {print count+0}')
else
    echo "rs_metrics.sh not found, skipping detailed metrics"
    total_query_time=0
    successful_queries=3
fi

echo -e "\n=================== QUERY METRICS ====================="
echo "Retrieving actual query execution times from Redshift..."
echo "Queries attempted: 4"
echo "Queries successfully completed: $successful_queries"
echo "Total Redshift query processing time: $total_query_time seconds"

echo -e "\n=================== QUERY COST ==========================="
echo -e "Calling rs_cost.sh...\n"
if [ -f "./rs_cost.sh" ]; then
    bash ./rs_cost.sh "$START_TIMESTAMP" "$END_TIMESTAMP" "$OPERATION_TYPE" "" "4" "$DATASET_SIZE"
else
    echo "rs_cost.sh not found, skipping cost calculation"
fi

echo -e "\n=== QUERIES COMPLETED ==="

unset PGPASSWORD


echo "Writing query results to CSV..."
{
    ts=$(date -u +"%Y-%m-%d %H:%M:%S")
    echo "$metrics_data" | awk -F',' -v size="$DATASET_SIZE" -v ts="$ts" '
    BEGIN {OFS=","}
    NR>1 {
        slot_count = int($6) 
        print "Redshift", size, "QUERY", $1, $2, $3, $4, $5, slot_count, $7, "0.0", ts
    }' >> rs_results.csv
}
