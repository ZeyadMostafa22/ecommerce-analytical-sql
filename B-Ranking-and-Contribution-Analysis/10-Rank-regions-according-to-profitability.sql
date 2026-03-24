USE WAREHOUSE COMPUTE_WH;
USE SCHEMA ECOM_DB.ECOM_DW;


-- =============================================================================
-- question 10 | rank regions according to profitability.
-- =============================================================================

with region_profit as (
    select
        dc.region,
        sum(fol.profit_amount) as total_profit
    from fact_order_line fol
    inner join dim_customer dc on fol.customer_key = dc.customer_key
    group by dc.region
)
select
    rank() over (order by total_profit desc) as profit_rank,
    region,
    round(total_profit, 2)                   as total_profit
from region_profit
order by profit_rank;