-- Identify products experiencing sustained decline across consecutive periods.
WITH monthly_sales AS (
    SELECT
        fact_order_line.product_key,
        product_name,
        year * 100 + month          AS period,   -- e.g. 202501, 202502
        SUM(net_amount)             AS revenue
    FROM fact_order_line
    JOIN dim_date ON fact_order_line.date_key = dim_date.date_key
    JOIN dim_product ON fact_order_line.product_key = dim_product.product_key
    GROUP BY fact_order_line.product_key, product_name,year, month
),
with_lag AS (
    SELECT
        product_key,
        product_name,
        period,
        revenue,
        LAG(revenue) OVER (PARTITION BY product_key ORDER BY period) AS prev_revenue
    FROM monthly_sales
),
decline_flag AS (
    SELECT
        product_key,
        product_name,
        period,
        revenue,
        CASE WHEN revenue < prev_revenue THEN 1 ELSE 0 END AS is_decline
    FROM with_lag
),
consecutive AS (
    SELECT
        product_key,
        product_name,
        SUM(is_decline) OVER (
            PARTITION BY product_key
            ORDER BY period
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        )                           AS declines_in_last_3
    FROM decline_flag
)
SELECT DISTINCT product_key, product_name
FROM consecutive
WHERE declines_in_last_3 = 3;   -- declined in all 3 consecutive periods