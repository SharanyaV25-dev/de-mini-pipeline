"""Stage 2: validate raw orders and split valid and rejected records."""

from __future__ import annotations

import sys
from pathlib import Path

from mysql.connector import IntegrityError

sys.path.append(str(Path(__file__).resolve().parents[1]))

from pipeline_utils import create_tables, database_cursor, validation_error


def run() -> None:
    clean_count = 0
    reject_count = 0

    with database_cursor() as cursor:
        create_tables(cursor)
        cursor.execute("TRUNCATE TABLE clean_orders")
        cursor.execute("TRUNCATE TABLE error_orders")
        cursor.execute("SELECT order_id, product, amount FROM raw_orders")

        for order_id, product, amount in cursor.fetchall():
            normalized_product = product.strip().title() if product else product
            error = validation_error(order_id, normalized_product, amount)

            if error is None:
                try:
                    cursor.execute(
                        "INSERT INTO clean_orders (order_id, product, amount) VALUES (%s, %s, %s)",
                        (order_id, normalized_product, amount),
                    )
                    clean_count += 1
                    continue
                except IntegrityError:
                    error = "DUPLICATE_ORDER_ID"

            cursor.execute(
                """INSERT INTO error_orders (order_id, product, amount, error_reason)
                VALUES (%s, %s, %s, %s)""",
                (order_id, normalized_product, amount, error),
            )
            reject_count += 1

    print(f"Validation complete: {clean_count} clean, {reject_count} rejected.")


if __name__ == "__main__":
    run()
