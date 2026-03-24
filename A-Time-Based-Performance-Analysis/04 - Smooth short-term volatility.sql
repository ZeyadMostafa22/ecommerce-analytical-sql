USE WAREHOUSE COMPUTE_WH;
USE SCHEMA ECOM_DB.ECOM_DW;

WITH daily_sales AS (
    SELECT 
        d.full_date,
        SUM(f.net_amount) AS daily_revenue
    FROM fact_order_line f
    JOIN dim_date d 
        ON f.date_key = d.date_key
    GROUP BY d.full_date
)
SELECT 
    full_date,
    daily_revenue,
    AVG(daily_revenue) OVER (
        ORDER BY full_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS moving_avg_7
FROM daily_sales
ORDER BY full_date;
