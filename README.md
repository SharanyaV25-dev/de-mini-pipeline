# Data Engineering Mini Pipeline

This project demonstrates an end-to-end batch data pipeline:

CSV → MySQL (Raw) → Clean/Error Tables → Aggregation → JSON Export

## Pipeline Stages
1. Raw ingestion
2. Data cleaning & validation
3. Aggregation
4. JSON export

## Tech Stack
- Python
- MySQL
- CSV, JSON

## How to Run
Run scripts in order:
1. 1_ingest_raw.py
2. 2_clean_data.py
3. 3_aggregate.py
4. 4_export_json.py
