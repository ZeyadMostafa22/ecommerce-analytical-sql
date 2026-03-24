USE WAREHOUSE COMPUTE_WH;
USE SCHEMA ECOM_DB.ECOM_DW;

WITH customer_revenue AS (
    SELECT 
        c.customer_id,
        c.customer_segment,
        SUM(f.net_amount) AS total_revenue
    FROM fact_order_line f
    JOIN dim_customer c 
        ON f.customer_key = c.customer_key
    GROUP BY 
        c.customer_id,
        c.customer_segment
),
ranked_customers AS (
    SELECT 
        customer_id,
        customer_segment,
        total_revenue,
        PERCENT_RANK() OVER (
            ORDER BY total_revenue DESC
        ) AS pr
    FROM customer_revenue
)
SELECT 
    customer_id,
    customer_segment,
    total_revenue,
    pr
FROM ranked_customers
WHERE pr <= 0.25
ORDER BY total_revenue DESC;