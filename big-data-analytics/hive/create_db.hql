-- ============================================
-- Create retail database
-- Contains customers, orders, products tables
-- ============================================

-- Set environment
SET hive.cli.print.header=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- 1. Create database (if not exists)
CREATE DATABASE IF NOT EXISTS retail_db
COMMENT 'Retail database - customers, orders, products'
LOCATION '/user/hive/warehouse/retail_db'
WITH DBPROPERTIES ('created_by'='admin', 'created_date'='2024-01-15');

-- Use database
USE retail_db;

-- ============================================
-- 2. Create customers table
-- ============================================
CREATE TABLE IF NOT EXISTS customers (
    customer_id INT COMMENT 'Customer ID',
    order_id INT COMMENT 'Order ID',
    order_date STRING COMMENT 'Order date',
    fname STRING COMMENT 'First name',
    lname STRING COMMENT 'Last name',
    state STRING COMMENT 'State',
    zipcode STRING COMMENT 'Zip code',
    country STRING COMMENT 'Country'
)
COMMENT 'Customer information table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- ============================================
-- 3. Create orders table
-- ============================================
CREATE TABLE IF NOT EXISTS orders (
    order_id INT COMMENT 'Order ID',
    customer_id INT COMMENT 'Customer ID',
    total DECIMAL(10,2) COMMENT 'Order total amount'
)
COMMENT 'Order information table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- ============================================
-- 4. Create products table
-- ============================================
CREATE TABLE IF NOT EXISTS products (
    prod_id INT COMMENT 'Product ID',
    brand STRING COMMENT 'Brand',
    name STRING COMMENT 'Product name'
)
COMMENT 'Product information table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- ============================================
-- 5. Show creation results
-- ============================================
SHOW TABLES;
DESCRIBE customers;
DESCRIBE orders;
DESCRIBE products;