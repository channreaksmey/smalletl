-- Run this in MySQL Workbench
CREATE DATABASE IF NOT EXISTS sales_db;

USE sales_db;

CREATE TABLE IF NOT EXISTS daily_sales (
    id INT AUTO_INCREMENT PRIMARY KEY,
    sale_id VARCHAR(50),
    date DATE,
    customer_name VARCHAR(100),
    product VARCHAR(100),
    amount DECIMAL(10,2),
    quantity INT,
    region VARCHAR(50),
    sales_rep VARCHAR(100),
    processed_at TIMESTAMP,
    source_file VARCHAR(50),
    INDEX idx_date (date),
    INDEX idx_product (product)
);

SELECT * FROM daily_sales;