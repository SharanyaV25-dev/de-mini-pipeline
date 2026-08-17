"""Stage 4: export the product summary to a JSON output contract."""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from config import OUTPUT_JSON_PATH
from pipeline_utils import database_cursor


def run() -> None:
    output_path = Path(OUTPUT_JSON_PATH)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with database_cursor(dictionary=True) as cursor:
        cursor.execute(
            """SELECT product_name, total_sales_amount
            FROM product_aggregation
            ORDER BY product_name"""
        )
        rows = cursor.fetchall()

    with output_path.open("w", encoding="utf-8") as output_file:
        json.dump(rows, output_file, indent=2)

    print(f"Export complete: {len(rows)} records written to {output_path}.")


if __name__ == "__main__":
    run()
