-- Check raw data
SELECT * FROM raw_orders;

-- Count clean vs error records
SELECT COUNT(*) FROM clean_orders;
SELECT COUNT(*) FROM error_orders;

--Aggregation Check
SELECT product,sum(amount) from clean_orders 
GROUP BY product ORDER BY product;

-- Data quality check
SELECT * FROM error_orders WHERE 
error_reason IS NOT NULL;