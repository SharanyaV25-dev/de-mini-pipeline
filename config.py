"""Runtime configuration read from environment variables.

Copy .env.example to .env for local development. Never commit .env.
"""

from os import getenv

from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": getenv("MYSQL_HOST", "localhost"),
    "port": int(getenv("MYSQL_PORT", "3306")),
    "user": getenv("MYSQL_USER", "pipeline_user"),
    "password": getenv("MYSQL_PASSWORD", "pipeline_password"),
    "database": getenv("MYSQL_DATABASE", "orders_pipeline"),
}

INPUT_CSV_PATH = getenv("INPUT_CSV_PATH", "data/orders_dirty.csv")
OUTPUT_JSON_PATH = getenv("OUTPUT_JSON_PATH", "data/output/product_summary.json")
