"""Stage 3: build an analytics-ready product sales summary."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from pipeline_utils import create_tables, database_cursor


def run() -> None:
    with database_cursor() as cursor:
        create_tables(cursor)
        cursor.execute("TRUNCATE TABLE product_aggregation")
        cursor.execute(
            """INSERT INTO product_aggregation (product_name, total_sales_amount)
            SELECT product, SUM(amount)
            FROM clean_orders
            GROUP BY product
            ORDER BY product"""
        )
        cursor.execute("SELECT COUNT(*) FROM product_aggregation")
        summary_count = cursor.fetchone()[0]

    print(f"Aggregation complete: {summary_count} product summaries created.")


if __name__ == "__main__":
    run()
