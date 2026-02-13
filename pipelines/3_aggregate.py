# Stage 3: Aggregation Layer
# Purpose:
# - Aggregate clean transactional data into analytics-ready summary tables
# - Compute total sales amount per product from clean_orders table

# Business Logic:
# - Group by product_name
# - Sum amount_value for each product

from config import DB_CONFIG
import mysql.connector as connection
conn = connection.connect(**DB_CONFIG)
cur = conn.cursor()
try:
   conn.start_transaction()
   cur.execute("CREATE TABLE IF NOT EXISTS product_aggregation(product_name VARCHAR(25) PRIMARY KEY,total_sales_amount INT);")
   cur.execute("TRUNCATE TABLE product_aggregation;")
   conn.commit()
   conn.start_transaction()
   cur.execute("SELECT COUNT(*) FROM clean_orders;")
   clean_count = cur.fetchone()[0]
   if clean_count == 0:
       raise Exception("Clean table is empty. Aborting aggregation.")
   cur.execute("select product,sum(amount) from clean_orders group by product order by product;")
   result_set = cur.fetchall()
   rec_count = 0
   for row in result_set:
      prd_name = row[0]
      amt = row[1]
      sql = "INSERT INTO product_aggregation(product_name,total_sales_amount) VALUES(%s,%s);"
      val = (prd_name,amt)
      cur.execute(sql,val)
      rec_count+=1
   conn.commit()
   conn.close()
   print(f"{rec_count} records into product_aggregation table!")
except Exception as e:
   conn.rollback()
   print(f"Failed due to exception at : {e}")
      
