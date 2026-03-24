WITH co_purchase AS (
    SELECT 
        f1.product_key AS product_a,
        f2.product_key AS product_b,
        COUNT(*) AS co_purchase_score
    FROM FACT_ORDER_LINE f1
    JOIN FACT_ORDER_LINE f2 
        ON f1.order_id = f2.order_id
        AND f1.product_key <> f2.product_key
    GROUP BY f1.product_key, f2.product_key
),

recency AS (
    SELECT 
        f1.product_key AS product_a,
        f2.product_key AS product_b,
        COUNT(*) AS recency_score
    FROM FACT_ORDER_LINE f1
    JOIN FACT_ORDER_LINE f2 
        ON f1.order_id = f2.order_id
        AND f1.product_key <> f2.product_key
    JOIN DIM_DATE d 
        ON f1.date_key = d.date_key
    WHERE d.full_date >= CURRENT_DATE - INTERVAL '30 DAY'
    GROUP BY f1.product_key, f2.product_key
),

category_score AS (
    SELECT 
        p1.product_key AS product_a,
        p2.product_key AS product_b,
        CASE 
            WHEN p1.subcategory = p2.subcategory THEN 1
            WHEN p1.brand = p2.brand THEN 0.7
            ELSE 0.3
        END AS category_score
    FROM DIM_PRODUCT p1
    JOIN DIM_PRODUCT p2 
        ON p1.product_key <> p2.product_key
),

product_profit AS (
    SELECT 
        product_key,
        AVG(profit_amount / NULLIF(net_amount, 0)) AS profit_score
    FROM FACT_ORDER_LINE
    GROUP BY product_key
),

profit_score AS (
    SELECT 
        cp.product_a,
        cp.product_b,
        pp.profit_score
    FROM co_purchase cp
    JOIN product_profit pp 
        ON cp.product_b = pp.product_key
),

stock_score AS (
    SELECT 
        p1.product_key AS product_a,
        p2.product_key AS product_b,
        CASE 
            WHEN p2.stock_quantity = 0 THEN 0
            WHEN p2.stock_quantity < 50 THEN 0.5
            ELSE 1
        END AS stock_score
    FROM DIM_PRODUCT p1
    JOIN DIM_PRODUCT p2 
        ON p1.product_key <> p2.product_key
),

co_purchase_norm AS (
    SELECT 
        product_a,
        product_b,
        co_purchase_score / MAX(co_purchase_score) OVER (PARTITION BY product_a) AS score
    FROM co_purchase
),

recency_norm AS (
    SELECT 
        product_a,
        product_b,
        recency_score / MAX(recency_score) OVER (PARTITION BY product_a) AS score
    FROM recency
),

final_scores AS (
    SELECT 
        cp.product_a,
        cp.product_b,

        0.3 * cp.score +
        0.2 * COALESCE(r.score, 0) +
        0.1 * c.category_score +
        0.1 * COALESCE(p.profit_score, 0) +
        0.1 * s.stock_score

        AS recommendation_score

    FROM co_purchase_norm cp
    LEFT JOIN recency_norm r 
        ON cp.product_a = r.product_a 
        AND cp.product_b = r.product_b
    LEFT JOIN category_score c 
        ON cp.product_a = c.product_a 
        AND cp.product_b = c.product_b
    LEFT JOIN profit_score p 
        ON cp.product_a = p.product_a 
        AND cp.product_b = p.product_b
    LEFT JOIN stock_score s 
        ON cp.product_a = s.product_a 
        AND cp.product_b = s.product_b
)

SELECT *
FROM (
    SELECT 
        product_a,
        product_b,
        recommendation_score,
        ROW_NUMBER() OVER (
            PARTITION BY product_a 
            ORDER BY recommendation_score DESC
        ) AS rn
    FROM final_scores
) t
WHERE rn <= 4 ORDER BY PRODUCT_A, RN;