-- Reference schema used by the local Python batch pipeline.

CREATE TABLE IF NOT EXISTS raw_orders (
    order_id INT NULL,
    product VARCHAR(255) NULL,
    amount INT NULL
);

CREATE TABLE IF NOT EXISTS clean_orders (
    order_id INT PRIMARY KEY,
    product VARCHAR(255) NOT NULL,
    amount INT NOT NULL
);

CREATE TABLE IF NOT EXISTS error_orders (
    rejected_row_id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT NULL,
    product VARCHAR(255) NULL,
    amount INT NULL,
    error_reason VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS product_aggregation (
    product_name VARCHAR(255) PRIMARY KEY,
    total_sales_amount BIGINT NOT NULL
);
