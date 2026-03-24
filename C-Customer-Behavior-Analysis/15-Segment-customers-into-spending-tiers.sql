use schema ecom_db.ecom_dw;

-- =============================================================================
-- question 15 | segment customers into spending tiers (e.g., quartiles).
-- =============================================================================

with customer_total_spend as (
    select
        fol.customer_key,
        dc.first_name || ' ' || dc.last_name as customer_name,
        sum(fol.net_amount)                  as total_spend
    from fact_order_line fol
    inner join dim_customer dc on fol.customer_key = dc.customer_key
    group by fol.customer_key, dc.first_name, dc.last_name
),
customer_quartiles as (
    select
        customer_key,
        customer_name,
        round(total_spend, 2)                    as total_spend,
        ntile(4) over (order by total_spend asc) as spending_quartile
    from customer_total_spend
)
select
    customer_key,
    customer_name,
    total_spend,
    spending_quartile,
    case spending_quartile
        when 1 then 'low spender'
        when 2 then 'mid spender'
        when 3 then 'high spender'
        when 4 then 'top spender (vip)'
    end as spending_tier
from customer_quartiles
order by spending_quartile desc, total_spend desc;
