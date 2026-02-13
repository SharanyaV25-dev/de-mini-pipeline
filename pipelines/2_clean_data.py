# Stage 2: Validate business rules and split into clean vs error tables
# Business rules:
# - order_id must be 3 digits
# - product must be non-empty
# - amount must be positive

from config import DB_CONFIG
import mysql.connector as connection
conn = connection.connect(**DB_CONFIG)
cur = conn.cursor()
try:
   conn.start_transaction()
   cur.execute("CREATE TABLE IF NOT EXISTS clean_orders(order_id INT PRIMARY KEY,product TEXT,amount INT);")
   cur.execute("CREATE TABLE IF NOT EXISTS error_orders(order_id INT PRIMARY KEY,product VARCHAR(15),amount INT, error_reason VARCHAR(50));")
   cur.execute("TRUNCATE TABLE clean_orders;")
   cur.execute("TRUNCATE TABLE error_orders;")
   conn.commit()
   conn.start_transaction()
   cur.execute("SELECT * FROM raw_orders;")
   results = cur.fetchall()
   raw_recs = len(results)
   clean_recs = 0
   error_recs = 0
   for row in results:
       try:
           id = row[0]
           prd = row[1].strip().title()
           amt = row[2] 
           if len(str(id)) != 3:
               reason = "INVALID_ID"
           elif prd is None or prd.strip() == "":
               reason = "MISSING_PRODUCT"
           elif amt <= 0:
               reason = "NEGATIVE_OR_ZERO_AMOUNT"
           else:
               reason = "UNKNOWN_ERROR"           
           if (id>0 and prd is not None and prd.strip()!="" and amt>0):
               sql = "INSERT IGNORE INTO clean_orders(order_id,product,amount) VALUES(%s,%s,%s);"
               val = (id,prd,amt)
               cur.execute(sql,val)
               clean_recs+=1
           else :
               sql = "INSERT IGNORE INTO error_orders(order_id,product,amount,error_reason) VALUES(%s,%s,%s,%s);"
               val = (id,prd,amt,reason)
               cur.execute(sql,val)
               error_recs+=1
       except Exception as e:
           failed_count+=1
           print(f"Skipping bad row {row} | Reason : {e}")     
   conn.commit()
   conn.close()
   print(f"Total no. of raw records : {raw_recs}")
   print(f"{clean_recs} clean records inserted in clean_orders table!")
   print(f"{error_recs} error records inserted in error_orders table!")
except Exception as e:
    conn.rollback()
    print(f"Facing Error due to exception : {e}")      