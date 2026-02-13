# Stage 4: Data Export Layer (JSON)
# Purpose:
# - Export final aggregated data to JSON for downstream consumption
# - This can be used by dashboards, APIs, or reporting tools

# Output Contract:
# - Each JSON record represents one product and its total sales
# - Fields: product_name, total_sales_amount

from config import DB_CONFIG
import mysql.connector as connection
import csv,json
conn = connection.connect(**DB_CONFIG)
cur = conn.cursor(dictionary = True)
conn.start_transaction()
cur.execute("select * from product_aggregation;")
result_set = cur.fetchall()
json_output = json.dumps(result_set, indent=4)
with open("product_aggregation.json","w") as file:
    file.write(json_output)
    print("product_aggregation.json file successfully created!!!")
conn.close()
file.close()