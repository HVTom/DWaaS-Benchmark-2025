#!/bin/bash

DATASET="synthea_raw"
OPERATION_TYPE="QUERY"
DATASET_SIZE=${1:-"unknown"}


echo "=== QUERY PERFORMANCE BENCHMARK ==="
echo "Dataset: $DATASET"
echo "Dataset size: $DATASET_SIZE" 
echo "========================================"

START_TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%S")

# Query 1 - Basic aggregation with simple WHERE filtering
# used to perform basic aggregations on patient-encounter data 
# and with financial metrics
echo "Running Query 1: Basic aggregation..."
START_TIME=$(date +%s)

bq query --use_legacy_sql=false --nouse_cache "
-- Query 1:  Basic Aggregation
WITH base_aggregation AS (
  SELECT
    p.GENDER,
    p.RACE,
    p.STATE,
    p.COUNTY, -- add COUNTY for more groups
    COUNT(DISTINCT e.Id) as total_encounters,
    AVG(e.TOTAL_CLAIM_COST) as avg_cost,
    MAX(e.\`START\`) as latest_encounter,
    STDDEV(e.TOTAL_CLAIM_COST) as cost_std_dev,
    SUM(e.TOTAL_CLAIM_COST) as total_cost,
    COUNT(DISTINCT p.Id) as unique_patients,
    COUNT(DISTINCT e.ENCOUNTERCLASS) as encounter_types,
    
    -- extra compute
    MIN(e.TOTAL_CLAIM_COST) as min_cost,
    MAX(e.TOTAL_CLAIM_COST) as max_cost,
    COUNT(DISTINCT EXTRACT(YEAR FROM e.\`START\`)) as years_with_encounters,
    
    -- simple string operation
    STRING_AGG(DISTINCT e.ENCOUNTERCLASS, ', ' ORDER BY e.ENCOUNTERCLASS) as encounter_classes
  FROM \`${DATASET}.patients\` p
  JOIN \`${DATASET}.encounters\` e ON p.Id = e.PATIENT
  WHERE e.\`START\` >= '2018-01-01'
  GROUP BY p.GENDER, p.RACE, p.STATE, p.COUNTY
)
SELECT
  *,
  total_cost / unique_patients as cost_per_patient,
  (max_cost - min_cost) as cost_range,
  
  -- 2 window functions
  -- calculate median of avg_cost by gender
  -- PERCENTILE_CONT(0.5) computes the 50th percentile (median)
  -- partitioning by gender: calculations are separate for M/F/UNKNOWN
  PERCENTILE_CONT(avg_cost, 0.5) OVER (PARTITION BY GENDER) as median_cost_by_gender,
  -- calculate global rank of records by total_encounters
  -- RANK() assigns relative position ordered by total_encounters DESC
  -- identical total_encounters get same rank; next rank skips numbers
  RANK() OVER (ORDER BY total_encounters DESC) as encounter_rank
FROM base_aggregation
ORDER BY total_encounters DESC, avg_cost DESC;"


END_TIME=$(date +%s)
echo "Query 1 completed in $((END_TIME-START_TIME)) seconds"
sleep 5

# Query 2 - Multi-Table Analysis
# used to nalyze patient encounters and medications to identify temporal relationships
# way of functioning: joins encounters and medications, classifies timing, computes complexity scores
echo "Running Query 2: Multi-Table Analysis..."
START_TIME=$(date +%s)

bq query --use_legacy_sql=false --nouse_cache "
-- Query 2: Enhanced Multi-Table Analysis
WITH encounter_medication_matrix AS (
    SELECT
        e.PATIENT,
        e.Id as encounter_id,
        e.\`START\` as encounter_date,
        e.ENCOUNTERCLASS,
        m.CODE as medication_code,
        m.DESCRIPTION as medication_desc,
        m.\`START\` as medication_start,
        m.TOTALCOST as medication_cost,
        --TIMESTAMP_DIFF(m.\`START\`, e.\`START\`, DAY) as days_diff,
        DATE_DIFF(CAST(m.\`START\` AS DATE), CAST(e.\`START\` AS DATE), DAY) as days_diff,
        
        -- costly string operations to enhance slowdown
        CONCAT(e.ENCOUNTERCLASS, '_', LEFT(m.DESCRIPTION, 20)) as encounter_med_key,
        LENGTH(m.DESCRIPTION) as description_length,
        
        -- Identify temporal relationships between medications and encounters:
        -- - 'CONCURRENT': medication administered during encounter
        -- - 'AFTER_ENCOUNTER': medication prescribed after encounter
        -- - 'BEFORE_ENCOUNTER': medication prescribed before encounter
        CASE
            --WHEN m.\`START\` BETWEEN e.\`START\` AND COALESCE(e.\`STOP\`, TIMESTAMP_ADD(e.\`START\`, INTERVAL 30 DAY))
            WHEN TIMESTAMP(m.\`START\`) BETWEEN e.\`START\` AND COALESCE(e.\`STOP\`, TIMESTAMP_ADD(e.\`START\`, INTERVAL 30 DAY))
              THEN 'CONCURRENT'
            --WHEN m.\`START\` > COALESCE(e.\`STOP\`, TIMESTAMP_ADD(e.\`START\`, INTERVAL 30 DAY))
            WHEN TIMESTAMP(m.\`START\`) > COALESCE(e.\`STOP\`, TIMESTAMP_ADD(e.\`START\`, INTERVAL 30 DAY))
              THEN 'AFTER_ENCOUNTER'
            ELSE 'BEFORE_ENCOUNTER'
        END as timing_relationship
    FROM \`${DATASET}.encounters\` e
    JOIN \`${DATASET}.medications\` m ON e.PATIENT = m.PATIENT
    WHERE e.\`START\` >= '2018-01-01' 
),
patient_complexity_score AS (
    SELECT
        PATIENT,
        COUNT(DISTINCT encounter_id) as total_encounters,
        COUNT(DISTINCT medication_code) as unique_medications,
        AVG(medication_cost) as avg_medication_cost,
        -- aproximates limits for 100 quantiles
        -- OFFSET(50) - select the 51st array element, sepparates upper and lower distribution
        APPROX_QUANTILES(medication_cost, 100)[OFFSET(50)] as median_cost, 
        STDDEV(medication_cost) as cost_std_dev,
        
        -- extra statistics
        MIN(medication_cost) as min_medication_cost,
        MAX(medication_cost) as max_medication_cost,
        COUNT(DISTINCT timing_relationship) as timing_patterns_count,
        AVG(description_length) as avg_description_length,
        
        -- String aggregation - costly, especially for high number of unique values
        -- concatenates non null strings
        -- ”|”  = value delimiter
        STRING_AGG(DISTINCT timing_relationship, ' | ' ORDER BY timing_relationship) as timing_patterns,
        
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
    
    -- extra financial compute
    AVG(pcs.cost_std_dev) as avg_cost_variability,
    AVG(pcs.max_medication_cost - pcs.min_medication_cost) as avg_cost_range,
    AVG(pcs.avg_description_length) as avg_description_length,
    
    -- window function for extra compute 
    RANK() OVER (ORDER BY AVG(pcs.complexity_score) DESC) as complexity_rank
FROM \`${DATASET}.patients\` p
JOIN patient_complexity_score pcs ON p.Id = pcs.PATIENT
GROUP BY p.GENDER, p.RACE, p.STATE, p.COUNTY
ORDER BY avg_complexity DESC, complexity_rank ASC;
"

END_TIME=$(date +%s)
echo "Query 2 completed in $((END_TIME-START_TIME)) seconds"
sleep 5

#Query 3 - Self-Join Temporal Analysis
# used to see the condition progression sequences and co-occurrence relationships
# self-joins conditions within 100-day windows (to increase the processing time)
# classifies progression speed (RAPID/MODERATE/SLOW) based on day/timestamp gaps
echo "Running Query 3: Self-Join Temporal Analysis..."
START_TIME=$(date +%s)

bq query --use_legacy_sql=false --nouse_cache "
-- Query 3: Self-Join Temporal Analysis...
WITH condition_sequences AS (
  SELECT
    c1.PATIENT,
    c1.CODE as condition1_code,
    c1.DESCRIPTION as condition1_desc,
    c1.\`START\` as condition1_start,
    c2.CODE as condition2_code,
    c2.DESCRIPTION as condition2_desc,
    c2.\`START\` as condition2_start,
    --TIMESTAMP_DIFF(c2.\`START\`, c1.\`START\`, DAY) as days_between_conditions,
    DATE_DIFF(CAST(c2.\`START\` AS DATE), CAST(c1.\`START\` AS DATE), DAY) as days_between_conditions,
    
    -- forced string operation
    CONCAT(CAST(c1.CODE AS STRING), '_TO_', CAST(c2.CODE AS STRING)) as transition_key,
    LENGTH(c1.DESCRIPTION) + LENGTH(c2.DESCRIPTION) as combined_desc_length,
    
    -- clasiffy progresion speed
    CASE 
      --WHEN TIMESTAMP_DIFF(c2.\`START\`, c1.\`START\`, DAY) <= 30 THEN 'RAPID'
      --WHEN TIMESTAMP_DIFF(c2.\`START\`, c1.\`START\`, DAY) <= 90 THEN 'MODERATE'
      WHEN DATE_DIFF(CAST(c2.\`START\` AS DATE), CAST(c1.\`START\` AS DATE), DAY) <= 30 THEN 'RAPID'
      WHEN DATE_DIFF(CAST(c2.\`START\` AS DATE), CAST(c1.\`START\` AS DATE), DAY) <= 90 THEN 'MODERATE'
      ELSE 'SLOW'
    END as progression_speed
  FROM \`${DATASET}.conditions\` c1
  JOIN \`${DATASET}.conditions\` c2 
    ON c1.PATIENT = c2.PATIENT
    AND c2.\`START\` > c1.\`START\`
    AND c2.\`START\` <= TIMESTAMP_ADD(c1.\`START\`, INTERVAL 100 DAY)
    AND c1.CODE != c2.CODE
),
condition_networks AS (
  SELECT
    condition1_code,
    condition2_code,
    COUNT(*) as co_occurrence_count,
    AVG(days_between_conditions) as avg_days_between,
    STDDEV(days_between_conditions) as std_days_between,
    COUNT(DISTINCT PATIENT) as affected_patients,
    
    -- add statistics, extra compute
    MIN(days_between_conditions) as min_days_between,
    MAX(days_between_conditions) as max_days_between,
    AVG(combined_desc_length) as avg_desc_length,
    COUNT(DISTINCT progression_speed) as speed_varieties,
    
    -- sring operations
    STRING_AGG(DISTINCT transition_key, ' | ' ORDER BY transition_key) as transition_patterns,
    STRING_AGG(DISTINCT progression_speed, ',' ORDER BY progression_speed) as speed_distribution
  FROM condition_sequences
  GROUP BY condition1_code, condition2_code
  HAVING COUNT(*) >= 2 
)
SELECT
  cn.*,
  
  -- supplimentary columns
  (cn.max_days_between - cn.min_days_between) as days_range,
  cn.std_days_between / cn.avg_days_between as coefficient_of_variation,
  
  -- window functions
  RANK() OVER (ORDER BY co_occurrence_count DESC) as frequency_rank,
  PERCENT_RANK() OVER (ORDER BY avg_days_between) as timing_percentile,
  DENSE_RANK() OVER (ORDER BY affected_patients DESC) as patient_impact_rank
FROM condition_networks cn
ORDER BY co_occurrence_count DESC, frequency_rank ASC, affected_patients DESC;"

END_TIME=$(date +%s)
echo "Query 3 completed in $((END_TIME-START_TIME)) seconds"
sleep 5

# Query 4 - Full Scan Window Functions
# for stress-testing with large window functions and aggregations applied to these results
# resource-intensive window frames (1000-row windows)
# Uses LAG/LEAD for value sequencing and ROW_NUMBER for recency ranking
# calculates rolling string aggregations and description length metrics
echo "Running Query 4: Advanced Observations Analysis..."
START_TIME=$(date +%s)

bq query --use_legacy_sql=false --nouse_cache "
-- Query 4: Large window frames, cross-patient timestamp operations
WITH patient_observation_timeline AS (
  SELECT
    o.PATIENT,
    o.CODE,
    o.DESCRIPTION,
    o.VALUE,
    o.DATE,
    p.GENDER,
    p.RACE,
    
    -- Window functions 
    -- for each patient and observation code, get the value from the previous record in chronological order
    LAG(o.VALUE) OVER (PARTITION BY o.PATIENT, o.CODE ORDER BY o.DATE) as prev_value,
    -- for each patient and observation code, get the value from the next record in chronological order
    LEAD(o.VALUE) OVER (PARTITION BY o.PATIENT, o.CODE ORDER BY o.DATE) as next_value,
    -- for each patient, assign a unique rank to observations in descending date order (most recent = 1, 2 etc)
    ROW_NUMBER() OVER (PARTITION BY o.PATIENT ORDER BY o.DATE DESC) as obs_rank,
    -- assign a global rank to observations based on date (all patients, same date = same rank, no gaps)
    DENSE_RANK() OVER (ORDER BY o.DATE) as global_date_rank,
    
    -- large window frames to stress and check platform response
    COUNT(*) OVER (
      PARTITION BY p.GENDER 
      ORDER BY o.DATE 
      ROWS BETWEEN 500 PRECEDING AND 500 FOLLOWING
    ) as massive_rolling_count_1000_window,
    
    AVG(LENGTH(o.DESCRIPTION)) OVER (
      PARTITION BY o.CODE 
      ORDER BY o.DATE 
      ROWS BETWEEN 400 PRECEDING AND 400 FOLLOWING
    ) as rolling_avg_desc_length_800_window,
    
    STRING_AGG(LEFT(o.VALUE, 5), '|') OVER (
      PARTITION BY p.RACE 
      ORDER BY o.DATE 
      ROWS BETWEEN 300 PRECEDING AND 300 FOLLOWING
    ) as rolling_value_concat_600_window
  FROM \`${DATASET}.observations\` o
  JOIN \`${DATASET}.patients\` p ON o.PATIENT = p.Id
),
complex_aggregations AS (
  SELECT
    GENDER,
    RACE,
    CODE,
    COUNT(*) as total_observations,
    COUNT(DISTINCT PATIENT) as unique_patients,
    AVG(CASE WHEN REGEXP_CONTAINS(VALUE, r'^[0-9]+\.?[0-9]*$') THEN CAST(VALUE AS NUMERIC) END) as avg_numeric_value,
    STRING_AGG(DISTINCT DESCRIPTION, '||' ORDER BY DESCRIPTION) as all_descriptions,
    --TIMESTAMP_DIFF(MAX(DATE), MIN(DATE), DAY) as observation_span_days,
    DATE_DIFF(MAX(DATE), MIN(DATE), DAY) as observation_span_days,
    
    -- applying aggregations on the window functions results
    AVG(massive_rolling_count_1000_window) as avg_massive_rolling_count,
    MAX(rolling_avg_desc_length_800_window) as max_rolling_desc_length,
    COUNT(DISTINCT rolling_value_concat_600_window) as unique_rolling_concatenations
  FROM patient_observation_timeline
  GROUP BY GENDER, RACE, CODE
  HAVING COUNT(*) > 10
)
SELECT * FROM complex_aggregations
ORDER BY total_observations DESC, avg_massive_rolling_count DESC;"

END_TIME=$(date +%s)
echo "Query 4 completed in $((END_TIME-START_TIME)) seconds"
sleep 5

END_TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%S")


echo -e "==============================================\n"

echo -e "\n=================== QUERY JOB TIMES ==========================="
echo "Retrieving actual job execution times from BigQuery..."
metrics_data=$(./bq_metrics.sh "$START_TIMESTAMP" "$END_TIMESTAMP" "$OPERATION_TYPE" "" "4" "$DATASET_SIZE")

echo "$metrics_data" | column -t -s,

total_query_time=$(echo "$metrics_data" | awk -F',' 'NR>1 {sum+=$2} END {print sum}')
successful_queries=$(echo "$metrics_data" | awk -F',' 'NR>1 {count++} END {print count+0}')


echo -e "\n=================== QUERY METRICS ====================="
echo "Retrieving actual query execution times from BigQuery..."
echo "Queries executed: 4"
echo "Queries successfully completed: $successful_queries"
echo "Total BigQuery query processing time: $total_query_time seconds"



echo -e "\n=================== QUERY COST ==========================="
echo -e "Calling bq_cost.sh...\n"
./bq_cost.sh "$START_TIMESTAMP" "$END_TIMESTAMP" "$OPERATION_TYPE" "" "4" "$DATASET_SIZE"

# apel cleanup
echo -e "\n=================== QUERY CLEANUP ==========================="
echo -e "Calling clear_bq.sh...\n"
sh ./bq_clear.sh "$DATASET" ""
echo -e "\n=== QUERIES COMPLETED ==="


echo "Writing query results to CSV..."
{
    # ONLY job redirecting
    echo "$metrics_data" | awk -F',' -v size="$DATASET_SIZE" 'NR>1 {
        printf "BigQuery,%s,QUERY,%s,%s,%s,0,0,%s,0,%s\n", 
               size, $1, $2, $3, $5, $6
    }'
} >> bq_results.csv
echo "Query results appended to bq_results.csv"



