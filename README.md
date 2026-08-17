# Mini Data Engineering Pipeline — Python + MySQL

A reproducible batch ETL pipeline that turns a dirty orders CSV into clean, analytics-ready product sales data.

**Flow:** CSV → MySQL raw layer → validation / reject records → product aggregation → JSON export

## Why this project exists

The project demonstrates core data-engineering practices on a small, understandable dataset:

- Separate raw, clean, reject, and aggregate layers
- Explicit validation rules and reject reasons
- Idempotent full-refresh batch runs
- Environment-based configuration—no credentials in source code
- Reproducible local MySQL setup with Docker Compose
- Automated validation tests in GitHub Actions

## Architecture

```text
data/orders_dirty.csv
        │
        ▼
raw_orders ──► clean_orders ──► product_aggregation ──► data/output/product_summary.json
        │
        └────────► error_orders (rejected records + reason)
```

## Tech stack

- Python 3.12+
- MySQL 8
- SQL
- Docker Compose
- pytest and GitHub Actions

## Data contract and quality rules

The input CSV must have exactly these columns: `order_id`, `product`, and `amount`.

A record is accepted only when:

1. `order_id` is a three-digit number (100–999)
2. `product` is not blank
3. `amount` is greater than zero

Invalid parsed records are written to `error_orders` with a reason. Rows that cannot be parsed during CSV ingestion are reported with their line number.

## Run locally

### 1. Create a virtual environment and install dependencies

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2. Create the local configuration

```powershell
Copy-Item .env.example .env
```

The committed `.env.example` values work with the included local Docker MySQL service. Do not commit the generated `.env` file.

### 3. Start MySQL

```powershell
docker compose up -d
```

Wait for the MySQL health check to pass, then run the pipeline:

```powershell
python run_pipeline.py
```

The final JSON file is written to `data/output/product_summary.json`.

### 4. Run tests

```powershell
pytest
```

## Idempotency

This is an intentionally simple full-refresh batch pipeline. Each stage truncates and rebuilds its target table, so rerunning the complete pipeline with the same input produces the same result without duplicate output.

## Repository layout

```text
├── data/                 # Sample input data
├── pipelines/            # Ingest, validate, aggregate, and export stages
├── tests/                # Unit tests for quality rules
├── pipeline_utils.py     # Shared DB and validation helpers
├── run_pipeline.py       # Single pipeline entry point
├── docker-compose.yml    # Local MySQL service
└── .github/workflows/    # Continuous integration
```

## Current limitations and next improvements

- The project is local and batch-oriented; it does not yet support incremental loads, orchestration, alerts, or cloud deployment.
- Parsing failures are logged during ingestion; a future version can persist raw malformed CSV rows in a file-level reject table.
- The next portfolio project will extend these concepts into a cloud lakehouse pipeline with orchestration, data observability, and infrastructure as code.
