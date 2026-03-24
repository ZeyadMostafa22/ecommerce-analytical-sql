USE WAREHOUSE COMPUTE_WH;
USE SCHEMA ECOM_DB.ECOM_DW;

-- Task 13: Measure time intervals between consecutive purchases for each customer.
WITH order_dates AS (
    SELECT 
        f.customer_key,
        c.customer_id,
        f.order_id,
        d.full_date AS order_date 
    FROM fact_order_line f
    JOIN dim_date     d ON f.date_key     = d.date_key
    JOIN dim_customer c ON f.customer_key = c.customer_key
    GROUP BY f.customer_key, c.customer_id, f.order_id
)
SELECT 
    customer_id,
    order_id,
    order_date                                                          AS current_purchase_date,
    LAG(order_date) OVER (PARTITION BY customer_key ORDER BY order_date) AS prev_purchase_date,
    DATEDIFF(
        order_date,
        LAG(order_date) OVER (PARTITION BY customer_key ORDER BY order_date)
    )                                                                   AS days_between_purchases
FROM order_dates
ORDER BY customer_id, order_date;