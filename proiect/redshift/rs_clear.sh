#!/bin/bash

# usage: ./rs_clear.sh [dataset_raw] [dataset_processed]

REDSHIFT_HOST="redshift-benchmark-cluster-1.csag37dwinrq.eu-central-1.redshift.amazonaws.com"
REDSHIFT_DB="benchmark"
REDSHIFT_USER="awsuser"
REDSHIFT_PORT="5439"
export PGPASSWORD='?Gn+3&t&9}vS!:)'

DATASET_RAW=$1
DATASET_PROCESSED=$2

cleanup_dataset() {
    local dataset=$1
    echo "Cleaning all tables from dataset: $dataset"
    
    # ccomplete dataset tables list
    tables=$(psql -h $REDSHIFT_HOST -p $REDSHIFT_PORT -U $REDSHIFT_USER -d $REDSHIFT_DB -t -c "
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = '$dataset';" | tr -d ' ')
    
    if [ -z "$tables" ]; then
        echo "Nothing to delete in dataset $dataset"
        return 0
    fi
    
    for table in $tables; do
        if [ -n "$table" ]; then
            echo "Deleting table: $dataset.$table"

            psql -h $REDSHIFT_HOST -p $REDSHIFT_PORT -U $REDSHIFT_USER -d $REDSHIFT_DB -c "DROP TABLE IF EXISTS $dataset.\"$table\" CASCADE;"
            if [ $? -eq 0 ]; then
                echo "Tabel $table deleted successfully"
            else
                echo "Failed deleting table $table"
            fi
        fi
    done
}

echo "=== REDSHIFT DATASET CLEANUP ==="

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
    echo "Usage: ./rs_clear.sh [dataset_raw] [dataset_processed]"
    exit 1
fi

echo -e "\n=== CLEANUP COMPLETED ==="

unset PGPASSWORD
