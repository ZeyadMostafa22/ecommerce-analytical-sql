-- Task 6: Acceleration or Deceleration
WITH monthly_revenue AS (
    -- Step 1: Monthly Revenue
    SELECT 
        d.year, 
        d.month, 
        SUM(f.net_amount) AS revenue
    FROM fact_order_line f
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY d.year, d.month
),
growth_rate AS (
    -- Step 2: Growth Rate 
    SELECT 
        year, 
        month, 
        revenue, 
        LAG(revenue) OVER(ORDER BY year, month) AS prev_rev,
        -- (Current - Prev) / Prev
        (revenue - LAG(revenue) OVER(ORDER BY year, month)) / 
            NULLIF(LAG(revenue) OVER(ORDER BY year, month), 0) AS growth
    FROM monthly_revenue
)
-- Step 3: Acceleration Logic 
SELECT 
    year, 
    month, 
    growth, 
    LAG(growth) OVER(ORDER BY year, month) AS prev_growth,
    CASE 
        WHEN growth > LAG(growth) OVER(ORDER BY year, month) THEN 'Accelerating'
        WHEN growth < LAG(growth) OVER(ORDER BY year, month) THEN 'Decelerating'
        ELSE 'Stable'
    END AS revenue_dynamics
FROM growth_rate;