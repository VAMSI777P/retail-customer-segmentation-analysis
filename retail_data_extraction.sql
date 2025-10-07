-- Sales Performance and Customer Segmentation Analysis
-- SQL Script for Data Extraction from Retail Database

-- =============================================================
-- RETAIL CHAIN DATABASE QUERIES
-- =============================================================

-- Step 1: Verify database structure and data availability
-- Check table sizes and data quality

SELECT 'customers' as table_name, COUNT(*) as record_count FROM customers
UNION ALL
SELECT 'products', COUNT(*) FROM products  
UNION ALL
SELECT 'sales_transactions', COUNT(*) FROM sales_transactions;

-- Step 2: Data quality checks
-- Check for missing values and data inconsistencies

-- Missing customer information
SELECT 
    COUNT(*) as total_customers,
    SUM(CASE WHEN customer_age IS NULL THEN 1 ELSE 0 END) as missing_age,
    SUM(CASE WHEN gender IS NULL THEN 1 ELSE 0 END) as missing_gender,
    SUM(CASE WHEN location IS NULL THEN 1 ELSE 0 END) as missing_location
FROM customers;

-- Missing product information  
SELECT 
    COUNT(*) as total_products,
    SUM(CASE WHEN product_name IS NULL THEN 1 ELSE 0 END) as missing_name,
    SUM(CASE WHEN product_category IS NULL THEN 1 ELSE 0 END) as missing_category,
    SUM(CASE WHEN unit_price IS NULL OR unit_price <= 0 THEN 1 ELSE 0 END) as invalid_price
FROM products;

-- Step 3: Main data extraction query
-- Extract comprehensive sales data for analysis

SELECT 
    -- Transaction information
    s.transaction_id,
    s.customer_id,
    s.product_id,
    s.quantity,
    s.unit_price,
    s.total_amount,
    s.transaction_date,

    -- Customer demographics
    c.customer_age,
    c.gender,
    c.location,
    c.join_date,

    -- Product information
    p.product_name,
    p.product_category,

    -- Derived fields
    EXTRACT(YEAR FROM s.transaction_date) as transaction_year,
    EXTRACT(MONTH FROM s.transaction_date) as transaction_month,
    EXTRACT(DAY FROM s.transaction_date) as transaction_day,
    EXTRACT(DOW FROM s.transaction_date) as day_of_week,

    -- Customer tenure at time of purchase
    s.transaction_date - c.join_date as customer_tenure_days

FROM sales_transactions s
JOIN customers c ON s.customer_id = c.customer_id
JOIN products p ON s.product_id = p.product_id

-- Filter for relevant date range
WHERE s.transaction_date >= '2023-01-01'
  AND s.transaction_date <= '2024-12-31'

-- Ensure data quality
  AND s.total_amount > 0
  AND s.quantity > 0
  AND c.customer_age BETWEEN 18 AND 100

ORDER BY s.transaction_date DESC, s.customer_id;

-- Step 4: Customer summary statistics
-- Aggregate customer behavior metrics

SELECT 
    customer_id,

    -- Purchase behavior
    COUNT(*) as total_transactions,
    SUM(total_amount) as total_spent,
    AVG(total_amount) as avg_order_value,
    MAX(total_amount) as max_order_value,
    MIN(total_amount) as min_order_value,

    -- Product diversity
    COUNT(DISTINCT product_id) as unique_products_purchased,
    COUNT(DISTINCT product_category) as unique_categories_purchased,

    -- Timing analysis
    MIN(transaction_date) as first_purchase_date,
    MAX(transaction_date) as last_purchase_date,
    MAX(transaction_date) - MIN(transaction_date) as customer_lifespan_days,

    -- Purchase frequency
    COUNT(*) / GREATEST(1, EXTRACT(DAYS FROM (MAX(transaction_date) - MIN(transaction_date)))) as purchases_per_day

FROM sales_transactions s
WHERE transaction_date >= '2023-01-01'
  AND transaction_date <= '2024-12-31'
  AND total_amount > 0

GROUP BY customer_id
HAVING COUNT(*) >= 1  -- At least one transaction
ORDER BY total_spent DESC;

-- Step 5: Product performance analysis
-- Analyze product and category performance

SELECT 
    p.product_category,
    p.product_id,
    p.product_name,
    p.unit_price,

    -- Sales metrics
    COUNT(*) as times_sold,
    SUM(s.quantity) as total_quantity_sold,
    SUM(s.total_amount) as total_revenue,
    AVG(s.total_amount) as avg_transaction_value,

    -- Customer reach
    COUNT(DISTINCT s.customer_id) as unique_customers,

    -- Profitability (assuming 40% margin)
    SUM(s.total_amount) * 0.40 as estimated_profit

FROM products p
JOIN sales_transactions s ON p.product_id = s.product_id
WHERE s.transaction_date >= '2023-01-01'
  AND s.transaction_date <= '2024-12-31'

GROUP BY p.product_category, p.product_id, p.product_name, p.unit_price
ORDER BY total_revenue DESC;

-- Step 6: Time-based analysis
-- Monthly and seasonal trends

SELECT 
    EXTRACT(YEAR FROM transaction_date) as year,
    EXTRACT(MONTH FROM transaction_date) as month,

    -- Volume metrics
    COUNT(*) as transaction_count,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(DISTINCT product_id) as unique_products_sold,

    -- Revenue metrics  
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_transaction_value,
    SUM(quantity) as total_items_sold,

    -- Customer behavior
    SUM(total_amount) / COUNT(DISTINCT customer_id) as revenue_per_customer,
    COUNT(*) / COUNT(DISTINCT customer_id) as transactions_per_customer

FROM sales_transactions
WHERE transaction_date >= '2023-01-01'
  AND transaction_date <= '2024-12-31'
  AND total_amount > 0

GROUP BY EXTRACT(YEAR FROM transaction_date), EXTRACT(MONTH FROM transaction_date)
ORDER BY year, month;

-- Step 7: Customer segmentation prep query
-- RFM analysis preparation

WITH customer_rfm AS (
    SELECT 
        s.customer_id,
        c.customer_age,
        c.gender,
        c.location,
        c.join_date,

        -- Recency (days since last purchase)
        CURRENT_DATE - MAX(s.transaction_date) as recency_days,

        -- Frequency (number of transactions)
        COUNT(*) as frequency,

        -- Monetary (total amount spent)
        SUM(s.total_amount) as monetary_value,

        -- Additional metrics
        AVG(s.total_amount) as avg_order_value,
        MAX(s.transaction_date) as last_purchase_date,
        MIN(s.transaction_date) as first_purchase_date

    FROM sales_transactions s
    JOIN customers c ON s.customer_id = c.customer_id
    WHERE s.transaction_date >= '2023-01-01'
      AND s.transaction_date <= '2024-12-31'
      AND s.total_amount > 0

    GROUP BY s.customer_id, c.customer_age, c.gender, c.location, c.join_date
)

SELECT 
    customer_id,
    customer_age,
    gender, 
    location,
    join_date,
    recency_days,
    frequency,
    monetary_value,
    avg_order_value,
    last_purchase_date,
    first_purchase_date,

    -- RFM scoring (quintiles)
    NTILE(5) OVER (ORDER BY recency_days DESC) as recency_score,  -- Lower recency = higher score
    NTILE(5) OVER (ORDER BY frequency) as frequency_score,
    NTILE(5) OVER (ORDER BY monetary_value) as monetary_score

FROM customer_rfm
ORDER BY monetary_value DESC;

-- Step 8: Geographic analysis
-- Sales performance by location

SELECT 
    c.location,

    -- Customer metrics
    COUNT(DISTINCT c.customer_id) as customer_count,
    AVG(c.customer_age) as avg_customer_age,

    -- Sales metrics
    COUNT(*) as total_transactions,
    SUM(s.total_amount) as total_revenue,
    AVG(s.total_amount) as avg_transaction_value,
    SUM(s.total_amount) / COUNT(DISTINCT c.customer_id) as revenue_per_customer,

    -- Market share
    SUM(s.total_amount) / (SELECT SUM(total_amount) FROM sales_transactions WHERE transaction_date >= '2023-01-01') * 100 as market_share_percent

FROM customers c
JOIN sales_transactions s ON c.customer_id = s.customer_id
WHERE s.transaction_date >= '2023-01-01'
  AND s.transaction_date <= '2024-12-31'
  AND s.total_amount > 0

GROUP BY c.location
ORDER BY total_revenue DESC;

-- Step 9: Cohort analysis preparation
-- Customer retention analysis by join month

SELECT 
    DATE_TRUNC('month', c.join_date) as cohort_month,
    DATE_TRUNC('month', s.transaction_date) as transaction_month,

    COUNT(DISTINCT s.customer_id) as active_customers,
    SUM(s.total_amount) as cohort_revenue

FROM customers c
JOIN sales_transactions s ON c.customer_id = s.customer_id
WHERE s.transaction_date >= '2023-01-01'
  AND s.transaction_date <= '2024-12-31'
  AND c.join_date >= '2020-01-01'

GROUP BY DATE_TRUNC('month', c.join_date), DATE_TRUNC('month', s.transaction_date)
ORDER BY cohort_month, transaction_month;

-- =============================================================
-- END OF SQL EXTRACTION SCRIPT
-- =============================================================

-- Usage Instructions:
-- 1. Run these queries against your retail database
-- 2. Export results to CSV files for Python analysis
-- 3. Use the main extraction query (Step 3) for comprehensive analysis
-- 4. Use specialized queries (Steps 4-9) for specific insights
-- 5. Ensure proper indexing on join columns for performance
