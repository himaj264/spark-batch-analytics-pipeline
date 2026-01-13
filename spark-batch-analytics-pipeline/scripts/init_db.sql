-- PostgreSQL Database Initialization Script
-- ==========================================
-- This script creates the necessary tables for the Spark Analytics Pipeline
-- Run this script to initialize the database schema

-- Create schema for analytics
CREATE SCHEMA IF NOT EXISTS analytics;

-- Set search path
SET search_path TO analytics, public;

-- =====================================================
-- Main Tables (Created by Spark, but schema defined here)
-- =====================================================

-- Sales Transactions Table
-- This table stores the transformed sales data
CREATE TABLE IF NOT EXISTS sales_transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(255),
    category VARCHAR(100),
    quantity INTEGER,
    unit_price DECIMAL(10, 2),
    transaction_date DATE,
    store_id VARCHAR(50),
    region VARCHAR(50),
    payment_method VARCHAR(50),
    total_amount DECIMAL(12, 2),
    year INTEGER,
    month INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    price_tier VARCHAR(20),
    transaction_size VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- Aggregation Tables
-- =====================================================

-- Sales by Category
CREATE TABLE IF NOT EXISTS agg_sales_by_category (
    category VARCHAR(100) PRIMARY KEY,
    total_transactions BIGINT,
    total_units_sold BIGINT,
    total_revenue DECIMAL(15, 2),
    avg_transaction_value DECIMAL(10, 2),
    avg_unit_price DECIMAL(10, 2),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sales by Region
CREATE TABLE IF NOT EXISTS agg_sales_by_region (
    region VARCHAR(50) PRIMARY KEY,
    total_transactions BIGINT,
    total_units_sold BIGINT,
    total_revenue DECIMAL(15, 2),
    avg_transaction_value DECIMAL(10, 2),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sales by Payment Method
CREATE TABLE IF NOT EXISTS agg_sales_by_payment (
    payment_method VARCHAR(50) PRIMARY KEY,
    total_transactions BIGINT,
    total_revenue DECIMAL(15, 2),
    avg_transaction_value DECIMAL(10, 2),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Daily Sales Summary
CREATE TABLE IF NOT EXISTS agg_daily_sales (
    transaction_date DATE PRIMARY KEY,
    day_name VARCHAR(20),
    total_transactions BIGINT,
    total_units_sold BIGINT,
    daily_revenue DECIMAL(15, 2),
    avg_transaction_value DECIMAL(10, 2),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Customer Summary
CREATE TABLE IF NOT EXISTS agg_customer_summary (
    customer_id VARCHAR(50) PRIMARY KEY,
    total_purchases BIGINT,
    total_items_bought BIGINT,
    total_spent DECIMAL(15, 2),
    avg_purchase_value DECIMAL(10, 2),
    first_purchase DATE,
    last_purchase DATE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Product Performance
CREATE TABLE IF NOT EXISTS agg_product_performance (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(100),
    times_purchased BIGINT,
    total_units_sold BIGINT,
    total_revenue DECIMAL(15, 2),
    avg_selling_price DECIMAL(10, 2),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Store Performance
CREATE TABLE IF NOT EXISTS agg_store_performance (
    store_id VARCHAR(50) PRIMARY KEY,
    region VARCHAR(50),
    total_transactions BIGINT,
    total_revenue DECIMAL(15, 2),
    avg_transaction_value DECIMAL(10, 2),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- Create Indexes for Better Query Performance
-- =====================================================

CREATE INDEX IF NOT EXISTS idx_transactions_date ON sales_transactions(transaction_date);
CREATE INDEX IF NOT EXISTS idx_transactions_customer ON sales_transactions(customer_id);
CREATE INDEX IF NOT EXISTS idx_transactions_product ON sales_transactions(product_id);
CREATE INDEX IF NOT EXISTS idx_transactions_category ON sales_transactions(category);
CREATE INDEX IF NOT EXISTS idx_transactions_region ON sales_transactions(region);

-- =====================================================
-- Sample Queries for Validation
-- =====================================================

-- You can run these queries after the ETL pipeline completes:

-- Get total revenue by category
-- SELECT category, total_revenue FROM agg_sales_by_category ORDER BY total_revenue DESC;

-- Get top customers
-- SELECT customer_id, total_spent FROM agg_customer_summary ORDER BY total_spent DESC LIMIT 10;

-- Get daily revenue trend
-- SELECT transaction_date, daily_revenue FROM agg_daily_sales ORDER BY transaction_date;

-- =====================================================
-- Grant Permissions (if needed)
-- =====================================================

-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO analytics_user;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO analytics_user;

COMMENT ON TABLE sales_transactions IS 'Transformed sales transaction data from Spark ETL pipeline';
COMMENT ON TABLE agg_sales_by_category IS 'Aggregated sales metrics grouped by product category';
COMMENT ON TABLE agg_sales_by_region IS 'Aggregated sales metrics grouped by geographic region';
COMMENT ON TABLE agg_daily_sales IS 'Daily sales summary with revenue and transaction counts';
