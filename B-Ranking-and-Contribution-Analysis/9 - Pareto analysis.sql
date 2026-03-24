WITH product_revenue AS (
    SELECT 
        p.product_id,
        p.product_name,
        SUM(f.net_amount) AS total_revenue
    FROM fact_order_line f
    JOIN dim_product p 
        ON f.product_key = p.product_key
    GROUP BY 
        p.product_id,
        p.product_name
),
pareto AS (
    SELECT 
        product_id,
        product_name,
        total_revenue,
        SUM(total_revenue) OVER () AS grand_total,
        SUM(total_revenue) OVER (
            ORDER BY total_revenue DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_revenue
    FROM product_revenue
)
SELECT 
    product_id,
    product_name,
    total_revenue,
    grand_total,
    cumulative_revenue,
    ROUND(total_revenue / grand_total * 100, 2)        AS percent_of_total,
    ROUND(cumulative_revenue / grand_total * 100, 2)   AS cumulative_percent
FROM pareto
ORDER BY total_revenue DESC;