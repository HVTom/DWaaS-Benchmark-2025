#!/bin/bash
DATASET_SIZE=$1
echo "=== Synapse Benchmark ==="
echo "=== Dataset size: $DATASET_SIZE ==="


echo -e "\nRunning Ingestion ..."
#./sy_ingestion.sh synthea_sample_data_csv_latest $DATASET_SIZE  # tiny
#./sy_ingestion.sh 10k_synthea_covid19_csv $DATASET_SIZE         # small
#./sy_ingestion.sh 2k_synthea_generated_csv $DATASET_SIZE        # medium
./sy_ingestion.sh 100k_synthea_covid19_csv $DATASET_SIZE        # large


echo -e "\nRunning Queries ..."
./sy_queries.sh $DATASET_SIZE

echo -e "\nRunning Transforms"
./sy_transforms.sh $DATASET_SIZE

echo -e "\nCleaning up..."
./sy_clear.sh raw processed
