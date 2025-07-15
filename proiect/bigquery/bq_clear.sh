#!/bin/bash

# usage: ./cleanup.sh [synthea_raw] [synthea_processed]
# arg 1: synthea_raw (if not empty, clea raw dataset tables)
# arg 2: synthea_processed (same)

DATASET_RAW=$1
DATASET_PROCESSED=$2

cleanup_dataset() {
    local dataset=$1
    
    echo "Cleaning all tables from dataset: $dataset"

    # get dataset table names
    tables=$(bq ls $dataset | grep TABLE | awk '{print $1}')    
    
    if [ -z "$tables" ]; then
        echo "Nothing to delete in dataset $dataset"
        return 0
    fi

    for table in $tables; do
        echo "Deleting table: $dataset.$table"
        bq rm -f $dataset.$table

        if [ $? -eq 0 ]; then  
            echo "Tabel $table deleted successfully"
        else 
            echo "Failed deleting table $table"
        fi
    done
}



echo "=== BIGQUERY DATASET CLEANUP ==="

# arg based cleaning
if [ -n "$DATASET_RAW" ] && [ -n "$DATASET_PROCESSED" ]; then
    echo "Cleaning both datasets: $DATASET_RAW and $DATASET_PROCESSED"
    cleanup_dataset "$DATASET_RAW"
    cleanup_dataset "$DATASET_PROCESSED"
    
elif [ -n "$DATASET_RAW" ] && [ -z "$DATASET_PROCESSED" ]; then
    echo "Cleaning only raw dataset: $DATASET_RAW"
    cleanup_dataset "$DATASET_RAW"
    
elif [ -z "$DATASET_RAW" ] && [ -n "$DATASET_PROCESSED" ]; then
    echo "Cleaning only processed dataset: $DATASET_PROCESSED"
    cleanup_dataset "$DATASET_PROCESSED"
    
else
    echo "No datasets specified for cleanup or wrong args"
    echo "Usage: ./cleanup.sh [synthea_raw] [synthea_processed]"
    exit 1
fi

echo -e "\n=== CLEANUP COMPLETED ==="
