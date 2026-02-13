--Raw Ingestion Table
CREATE TABLE IF NOT EXISTS raw_orders(
    order_id INTEGER,
    product varchar(15),
    amount INTEGER
);

--Clean Table
CREATE TABLE IF NOT EXISTS clean_orders(
    order_id INT PRIMARY KEY,
    product TEXT,
    amount INT
);

--Error Table
CREATE TABLE IF NOT EXISTS error_orders(
    order_id INT PRIMARY KEY,
    product VARCHAR(15),
    amount INT, 
    error_reason VARCHAR(50)
);

--Aggregated Table
CREATE TABLE IF NOT EXISTS product_aggregation(
    product_name VARCHAR(25) PRIMARY KEY,
    total_sales_amount INT
);