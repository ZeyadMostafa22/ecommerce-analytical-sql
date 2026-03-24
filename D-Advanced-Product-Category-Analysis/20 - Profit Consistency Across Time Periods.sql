-- Task 20: Evaluate profit consistency across time periods.
WITH monthly_profit AS (
    SELECT 
        f.product_key,
        p.product_name,
        d.year,
        d.month,
        SUM(f.profit_amount) AS monthly_profit
    FROM fact_order_line f
    JOIN dim_date    d ON f.date_key    = d.date_key
    JOIN dim_product p ON f.product_key = p.product_key
    GROUP BY f.product_key, p.product_name, d.year, d.month
),
consistency_metrics AS (
    SELECT 
        product_key,
        product_name,
        AVG(monthly_profit)    AS avg_monthly_profit,
        STDDEV(monthly_profit) AS profit_std_dev
    FROM monthly_profit
    GROUP BY product_key, product_name
)
SELECT 
    product_key,
    product_name,
    ROUND(avg_monthly_profit, 2) AS avg_monthly_profit,
    ROUND(profit_std_dev,     2) AS profit_std_dev,
    ROUND(profit_std_dev / NULLIF(avg_monthly_profit, 0), 4) AS coeff_of_variation,
    CASE 
        WHEN avg_monthly_profit  = 0                                              THEN 'No Profit Data'
        WHEN profit_std_dev / NULLIF(avg_monthly_profit, 0) < 0.15               THEN 'Highly Consistent'
        WHEN profit_std_dev / NULLIF(avg_monthly_profit, 0) < 0.50               THEN 'Stable Earner'
        ELSE                                                                           'High Volatility'
    END AS consistency_tier
FROM consistency_metrics
ORDER BY coeff_of_variation;