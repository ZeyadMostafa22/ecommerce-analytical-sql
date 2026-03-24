USE WAREHOUSE COMPUTE_WH;
USE SCHEMA ECOM_DB.ECOM_DW;

-- Task 8: Determine each product's contribution to its category's total revenue.
WITH revenue_base AS (
    SELECT 
        f.category_key, 
        f.product_key, 
        SUM(f.net_amount) AS prod_rev,
        SUM(SUM(f.net_amount)) OVER (PARTITION BY f.category_key) AS categ_rev
    FROM fact_order_line f
    GROUP BY f.category_key, f.product_key
)
SELECT 
    c.category_name,
    p.product_name,
    rb.prod_rev,
    rb.categ_rev,
    ROUND((rb.prod_rev / rb.categ_rev) * 100, 2) AS contribution_pct
FROM revenue_base rb
JOIN dim_category c ON rb.category_key = c.category_key
JOIN dim_product  p ON rb.product_key  = p.product_key
ORDER BY c.category_name, contribution_pct DESC;