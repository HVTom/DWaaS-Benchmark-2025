#!/bin/bash
DATASET_SIZE=$1
echo "=== BigQuery Benchmark ==="
echo "=== Dataset size: $DATASET_SIZE ==="


echo -e "\nRunning Ingestion ..."
#./bq_ingestion.sh synthea_sample_data_csv_latest $DATASET_SIZE  # tiny
#./bq_ingestion.sh 10k_synthea_covid19_csv $DATASET_SIZE         # small
#./bq_ingestion.sh 2k_synthea_generated_csv $DATASET_SIZE        # medium
./bq_ingestion.sh 100k_synthea_covid19_csv $DATASET_SIZE        # large


echo -e "\nRunning Queries ..."
./bq_queries.sh $DATASET_SIZE

echo -e "\nRunning Transforms"
./bq_transforms.sh $DATASET_SIZE

echo -e "\nCleaning up..."
./bq_clear.sh synthea_raw synthea_processed
