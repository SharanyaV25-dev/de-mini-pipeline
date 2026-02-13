# 🏗 Mini Data Engineering Pipeline (Python + MySQL)

This project demonstrates a simple end-to-end data engineering pipeline built using Python and MySQL.

CSV → MySQL (Raw) → Clean/Error Tables → Aggregation → JSON Export

## 🔄 Pipeline Stages

1. **Ingestion Layer**
   - Reads raw order data from MySQL
   - Handles schema mismatch and bad rows

2. **Data Quality & Cleaning**
   - Applies business rules to separate clean and error records
   - Stores valid data in `clean_orders`
   - Stores invalid data in `error_orders` with failure reasons

3. **Aggregation Layer**
   - Aggregates total sales amount per product
   - Idempotent reload using truncate strategy

4. **Export Layer**
   - Exports final aggregated data to JSON for downstream consumption

## 🛠 Tech Stack

- Python
- MySQL
- SQL
- Git & GitHub

## How to Run
Run scripts in order:
1. 1_ingest_raw.py
2. 2_clean_data.py
3. 3_aggregate.py
4. 4_export_json.py


## 🎯 What This Project Demonstrates

- ETL pipeline design
- Data quality checks
- Idempotent batch processing
- Production-style engineering practices
