"""Stage 1: load a CSV file into the raw MySQL layer."""

from __future__ import annotations

import csv
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from config import INPUT_CSV_PATH
from pipeline_utils import create_tables, database_cursor

EXPECTED_COLUMNS = {"order_id", "product", "amount"}


def run() -> None:
    input_path = Path(INPUT_CSV_PATH)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    inserted = 0
    rejected = 0
    with database_cursor() as cursor:
        create_tables(cursor)
        cursor.execute("TRUNCATE TABLE raw_orders")

        with input_path.open(newline="", encoding="utf-8") as source:
            reader = csv.DictReader(source)
            if reader.fieldnames is None or set(reader.fieldnames) != EXPECTED_COLUMNS:
                raise ValueError(f"CSV columns must be exactly: {sorted(EXPECTED_COLUMNS)}")

            for line_number, row in enumerate(reader, start=2):
                try:
                    cursor.execute(
                        "INSERT INTO raw_orders (order_id, product, amount) VALUES (%s, %s, %s)",
                        (int(row["order_id"]), row["product"].strip(), int(row["amount"])),
                    )
                    inserted += 1
                except (TypeError, ValueError) as error:
                    rejected += 1
                    print(f"Skipping CSV line {line_number}: {error}")

    print(f"Raw ingestion complete: {inserted} inserted, {rejected} rejected.")


if __name__ == "__main__":
    run()
