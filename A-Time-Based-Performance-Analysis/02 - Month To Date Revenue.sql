USE WAREHOUSE COMPUTE_WH;
USE SCHEMA ECOM_DB.ECOM_DW;

-- Task 2: Measure Month-to-Date performance to evaluate intra-month trends.
WITH sumdaily AS (
    SELECT 
        full_date, 
        day, 
        month, 
        year, 
        SUM(net_amount) AS daily_revenue
    FROM fact_order_line f
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY full_date, day, month, year
)
SELECT 
    day, 
    month, 
    year,
    SUM(daily_revenue) OVER (PARTITION BY year, month ORDER BY full_date) AS mtd_revenue
FROM sumdaily;