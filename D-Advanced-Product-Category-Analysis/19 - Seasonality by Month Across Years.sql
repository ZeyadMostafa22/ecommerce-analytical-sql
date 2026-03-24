USE WAREHOUSE COMPUTE_WH;
USE SCHEMA ECOM_DB.ECOM_DW;

WITH monthly_sales AS (
    SELECT 
        d.year,
        d.month,
        d.month_name,
        SUM(f.net_amount) AS revenue
    FROM fact_order_line f
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY d.year, d.month, d.month_name
)
SELECT 
    year,
    month,
    month_name,
    revenue,
    LAG(revenue) OVER (
        PARTITION BY month
        ORDER BY year
    ) AS prev_year_revenue,
    ROUND(
        (revenue - LAG(revenue) OVER (
            PARTITION BY month ORDER BY year
        )) / NULLIF(LAG(revenue) OVER (
            PARTITION BY month ORDER BY year
        ), 0) * 100, 2
    ) AS yoy_growth_pct
FROM monthly_sales
ORDER BY month, year;
