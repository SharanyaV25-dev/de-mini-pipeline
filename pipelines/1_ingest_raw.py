# Stage 1: Ingest raw CSV into raw_orders table
# This layer preserves original data for reprocessing/debugging

from config import DB_CONFIG
import mysql.connector as connection
import csv
conn = connection.connect(**DB_CONFIG)
cur = conn.cursor()
try:
    conn.start_transaction()
    cur.execute("CREATE TABLE IF NOT EXISTS raw_orders(order_id INTEGER,product varchar(15),amount INTEGER);")
    cur.execute("TRUNCATE TABLE raw_orders;")
    conn.commit()
    success_count = 0
    failed_count = 0
    with open("orders_dirty.csv","r") as infile:
        reader = csv.reader(infile)
        header = next(reader)
        expected_cols = 3
        if len(header) != expected_cols:
            raise ValueError(f"Schema mismatch: expected {expected_cols} columns, got {len(header)}")
        conn.start_transaction()
        for row in reader:
            try :
                if len(row) != expected_cols :
                    raise ValueError("Row does not match expected schema")
                ord_id = int(row[0])
                prod = row[1].strip()
                amt = int(row[2])
                sql = "INSERT INTO raw_orders(order_id,product,amount) VALUES (%s,%s,%s)"
                val = (ord_id,prod,amt)
                cur.execute(sql,val)
                success_count+=1
            except Exception as e:
                failed_count+=1
                print(f"Skipping bad row {row} | Reason : {e}")
    infile.close()
    conn.commit()
    conn.close()
    print(f"{success_count} records inserted into raw_orders table!")
    print(f"{failed_count} records skipped due to exception!")
except Exception as e:
    conn.rollback()
    print(f"Facing Error due to exception : {e}")