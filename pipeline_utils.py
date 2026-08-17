"""Shared database and validation helpers for the order pipeline."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import mysql.connector

from config import DB_CONFIG


@contextmanager
def database_cursor(dictionary: bool = False) -> Iterator[mysql.connector.cursor.MySQLCursor]:
    connection = mysql.connector.connect(**DB_CONFIG)
    cursor = connection.cursor(dictionary=dictionary)
    try:
        yield cursor
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()


def create_tables(cursor: mysql.connector.cursor.MySQLCursor) -> None:
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS raw_orders (
            order_id INT NULL,
            product VARCHAR(255) NULL,
            amount INT NULL
        )"""
    )
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS clean_orders (
            order_id INT PRIMARY KEY,
            product VARCHAR(255) NOT NULL,
            amount INT NOT NULL
        )"""
    )
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS error_orders (
            rejected_row_id INT AUTO_INCREMENT PRIMARY KEY,
            order_id INT NULL,
            product VARCHAR(255) NULL,
            amount INT NULL,
            error_reason VARCHAR(100) NOT NULL
        )"""
    )
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS product_aggregation (
            product_name VARCHAR(255) PRIMARY KEY,
            total_sales_amount BIGINT NOT NULL
        )"""
    )


def validation_error(order_id: int | None, product: str | None, amount: int | None) -> str | None:
    if order_id is None or not 100 <= order_id <= 999:
        return "INVALID_ORDER_ID"
    if product is None or not product.strip():
        return "MISSING_PRODUCT"
    if amount is None or amount <= 0:
        return "INVALID_AMOUNT"
    return None
