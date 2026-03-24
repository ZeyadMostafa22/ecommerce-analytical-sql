-- ============================================================
--  E-Commerce Analytical Data Warehouse
--  Phase 4 — KPI Definition Queries
--  Schema: ECOM_DB.ECOM_DW
-- ============================================================

USE SCHEMA ECOM_DB.ECOM_DW;


-- ============================================================
--  PART A — INDIVIDUAL KPI QUERIES
-- ============================================================

-- ------------------------------------------------------------
-- KPI 1 : TOTAL REVENUE
--         Definition : Sum of net_amount across all order lines
--         (net_amount = gross after discount, before cost)
-- ------------------------------------------------------------
SELECT
    SUM(net_amount)                             AS total_revenue
FROM FACT_ORDER_LINE;


-- ------------------------------------------------------------
-- KPI 2 : GROSS PROFIT
--         Definition : Sum of profit_amount across all order lines
--         (profit_amount = net_amount - cost_amount)
-- ------------------------------------------------------------
SELECT
    SUM(profit_amount)                          AS gross_profit
FROM FACT_ORDER_LINE;


-- ------------------------------------------------------------
-- KPI 3 : AVERAGE ORDER VALUE  (AOV)
--         Definition : Total revenue divided by the number of
--         distinct orders.  Uses order_id (our agreed fix).
-- ------------------------------------------------------------
SELECT
    ROUND(
        SUM(net_amount) / COUNT(DISTINCT order_id),
        2
    )                                           AS avg_order_value
FROM FACT_ORDER_LINE;


-- ------------------------------------------------------------
-- KPI 4 : CUSTOMER LIFETIME VALUE  (CLV)
--         Definition : Average total spend per customer across
--         the full date range.
-- ------------------------------------------------------------
WITH customer_spend AS (
    SELECT
        customer_key,
        SUM(net_amount)     AS total_spend
    FROM FACT_ORDER_LINE
    GROUP BY customer_key
)
SELECT
    ROUND(AVG(total_spend), 2)                  AS avg_customer_lifetime_value,
    ROUND(MIN(total_spend), 2)                  AS min_clv,
    ROUND(MAX(total_spend), 2)                  AS max_clv
FROM customer_spend;


-- ------------------------------------------------------------
-- KPI 5 : REPEAT PURCHASE RATE
--         Definition : % of customers who placed more than
--         one distinct order.
-- ------------------------------------------------------------
WITH order_counts AS (
    SELECT
        customer_key,
        COUNT(DISTINCT order_id)    AS num_orders
    FROM FACT_ORDER_LINE
    GROUP BY customer_key
)
SELECT
    COUNT(*)                                            AS total_customers,
    SUM(CASE WHEN num_orders > 1 THEN 1 ELSE 0 END)    AS repeat_customers,
    ROUND(
        SUM(CASE WHEN num_orders > 1 THEN 1 ELSE 0 END)
        * 100.0 / COUNT(*),
        2
    )                                                   AS repeat_purchase_rate_pct
FROM order_counts;


-- ------------------------------------------------------------
-- KPI 6 : PROFIT MARGIN  %
--         Definition : Gross profit as a percentage of revenue.
-- ------------------------------------------------------------
SELECT
    ROUND(
        SUM(profit_amount) * 100.0 / NULLIF(SUM(net_amount), 0),
        2
    )                                           AS profit_margin_pct
FROM FACT_ORDER_LINE;


-- ------------------------------------------------------------
-- KPI 7 : REVENUE GROWTH RATE  (Month-over-Month)
--         Definition : For each month, % change vs prior month.
--         Uses LAG() window function.
-- ------------------------------------------------------------
WITH monthly_revenue AS (
    SELECT
        d.year,
        d.month,
        SUM(f.net_amount)       AS revenue
    FROM FACT_ORDER_LINE f
    JOIN DIM_DATE d ON f.date_key = d.date_key
    GROUP BY d.year, d.month
),
with_lag AS (
    SELECT
        year,
        month,
        revenue,
        LAG(revenue) OVER (ORDER BY year, month)    AS prev_month_revenue
    FROM monthly_revenue
)
SELECT
    year,
    month,
    ROUND(revenue, 2)                               AS revenue,
    ROUND(prev_month_revenue, 2)                    AS prev_month_revenue,
    ROUND(
        (revenue - prev_month_revenue)
        * 100.0 / NULLIF(prev_month_revenue, 0),
        2
    )                                               AS mom_growth_rate_pct
FROM with_lag
ORDER BY year, month;

