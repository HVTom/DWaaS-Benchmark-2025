#!/bin/bash
DATASET_SIZE=$1
echo "=== Redshift Benchmark ==="
echo "=== Dataset size: $DATASET_SIZE ==="


echo -e "\nRunning Ingestion ..."
#./rs_ingestion.sh synthea_sample_data_csv_latest $DATASET_SIZE  # tiny
#./rs_ingestion.sh 10k_synthea_covid19_csv $DATASET_SIZE         # small
#./rs_ingestion.sh 2k_synthea_generated_csv $DATASET_SIZE        # medium
./rs_ingestion.sh 100k_synthea_covid19_csv $DATASET_SIZE        # large


echo -e "\nRunning Queries ..."
./rs_queries.sh $DATASET_SIZE

echo -e "\nRunning Transforms"
./rs_transforms.sh $DATASET_SIZE

#echo -e "\nCleaning up..."
./rs_clear.sh synthea_raw synthea_processed
