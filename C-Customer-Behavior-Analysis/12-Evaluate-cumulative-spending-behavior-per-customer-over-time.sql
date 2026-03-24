use schema ecom_db.ecom_dw;

-- =============================================================================
-- question 12 | evaluate cumulative spending behavior per customer over time.
-- =============================================================================

with customer_daily_spend as (
    select
        fol.customer_key,
        dc.first_name || ' ' || dc.last_name as customer_name,
        dd.full_date,
        sum(fol.net_amount)                  as daily_spend
    from fact_order_line fol
    inner join dim_date     dd on fol.date_key     = dd.date_key
    inner join dim_customer dc on fol.customer_key = dc.customer_key
    group by fol.customer_key, dc.first_name, dc.last_name, dd.full_date
)
select
    customer_key,
    customer_name,
    full_date,
    daily_spend,
    sum(daily_spend) over (
        partition by customer_key
        order by full_date
        rows between unbounded preceding and current row
    ) as cumulative_spend
from customer_daily_spend
order by customer_key, full_date;
