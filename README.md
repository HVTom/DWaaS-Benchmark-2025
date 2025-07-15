# Data Warehouses as a Service (DWaaS)
**Google BigQuery, Amazon Redshift, and Microsoft Azure Synapse Benchmark, 2025**

---

## Overview

This project is a modular, reproducible benchmarking framework for evaluating three important CDW (Cloud Data Warehouse) platforms: **Google BigQuery**, **Amazon Redshift**, and **Microsoft Azure Synapse**. The framework offers objective metrics on performance, cost, scalability, and usability, using synthetic medical datasets modeled after real-world complexity.

---

## Functionality

- **Multi-platform benchmarking:** Google BigQuery, Amazon Redshift, Azure Synapse
- Modular, automated Bash scripts for orchestration
- Flexible data volumes (80MB–5GB+)
- Metrics tracked: ingestion, query, transformation times + cost per operation
- Metrics extraction from system tables
- Portable and reusable: works with other datasets
- Free-tier/academic-friendly; minimal setup
- Transparent reporting: all outputs as CSV

---

## Architecture, Scripts and Data Flow

### Three-Layer Design


<img width="356" height="319" alt="image" src="https://github.com/user-attachments/assets/86f8b2d4-7849-457d-aa66-94d7d42e67b2" />


1. **Orchestration Layer**
   - `main.sh`: Coordinates sequential execution of all workflows
2. **Processing Layer**
   - `ingestion.sh`: Data ingest from storage to warehouse
   - `queries.sh`: Executes queries of increasing complexity
   - `transforms.sh`: Runs ETL-style transformation queries
3. **Metrics & Storage Layer**
   - `metrics.sh`: Extracts timings and data volumes
   - `cost.sh`: Calculates per-operation costs
   - `clear.sh`: Resets state for repeatable tests
  


Data Flow Overview


<img width="727" height="502" alt="image" src="https://github.com/user-attachments/assets/93cfce28-784f-48df-b203-a0b65999fc72" />


---

## Benchmark Methodology

Automated shell scripts are used for flexibility and reusability.

**Five Test Steps:**
1. **Load** — Ingest CSVs from storage into the warehouse
2. **Query** — Run four progressive SQL queries of increasing complexity
3. **Transform** — Apply three common data transformation queries
4. **Reset** — Remove/test data to prepare for the next dataset
5. **Document** — Log all metrics and costs

**Metrics Tracked:**
- Ingestion time
- Query latency and data scanned
- Transformation time
- Cost per operation
- Usability scoring

---

## Datasets

- **Source:** [MITRE Synthea](https://duckduckgo.com ) synthetic medical datasets
- **Format:** 16–18 related CSV files (patients, medications, conditions, etc.)
- **Sizes:**
  - 80 MB (small)
  - 500 MB (medium)
  - 1 GB (medium-large)
  - 5 GB (large)
- Datasets simulate realistic, relational medical data with controllable scale.

---

## Metrics Summary

| Platform   | Performance                       | Cost                        | Usability & UX                     |
|------------|-----------------------------------|-----------------------------|------------------------------------|
| BigQuery   | Predictable, fast for all ops     | Lowest/cost-effective       | Intuitive UI, easy to learn        |
| Redshift   | Fast after warm-up                | Moderate, compute-based     | Powerful, but complex UI           |
| Synapse    | Slowest, esp. for large/complex   | Highest for heavy workload  | Familiar MS, less cost-efficient   |

- **BigQuery:** Fastest and most linear scaling; low cost; friendly UI/UX.
- **Redshift:** Very efficient on cached/repeated queries; moderate cost; power-user focus.
- **Synapse:** Slow for complex/big data; most expensive; clear but less transparent metrics.

---

## Usability Notes

- **BigQuery**
  - Quick setup, Google login integration
  - Clean, minimal UI
  - Strong documentation

- **Redshift**
  - More complex setup (IAM, cluster config)
  - Flexible, enterprise-oriented controls
  - Sometimes requires using external tools for monitoring

- **Synapse**
  - Visual Studio–like IDE
  - Deep Azure ecosystem integration
  - Monitoring less transparent, errors can be harder to debug

---

## How to Run

1. **Configure Cloud Accounts**
   - Set up free/academic resources on Google Cloud, AWS, Azure as applicable
2. **Set Credentials**
   - Update environment variables in scripts for storage and warehouse
3. **Upload Datasets**
   - Place Synthea CSVs in the correct bucket/directory per provider
4. **Execute Scripts**
   - Run `main.sh` with dataset name/size argument. Orchestrator manages all steps and saves results as `results.csv`
5. **Analyze Outputs**
   - Review logs/CSV, or import into analytic tools (Jupyter, etc.) for detailed review

**Requirements:**
- Bash shell (Linux/macOS or Git Bash on Windows)
- Cloud provider auth tools (`gcloud`, `aws`, `az`) installed and authenticated



