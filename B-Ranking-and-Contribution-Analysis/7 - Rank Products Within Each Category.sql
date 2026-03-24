WITH product_revenue AS (
    SELECT 
        p.product_id,
        p.product_name,
        c.category_name,
        SUM(f.net_amount) AS total_revenue
    FROM fact_order_line f
    JOIN dim_product p   ON f.product_key   = p.product_key
    JOIN dim_category c  ON f.category_key  = c.category_key
    GROUP BY 
        p.product_id,
        p.product_name,
        c.category_name
)
SELECT 
    product_id,
    product_name,
    category_name,
    total_revenue,
    RANK() OVER (
        PARTITION BY category_name
        ORDER BY total_revenue DESC
    ) AS rank_product
FROM product_revenue
ORDER BY category_name;