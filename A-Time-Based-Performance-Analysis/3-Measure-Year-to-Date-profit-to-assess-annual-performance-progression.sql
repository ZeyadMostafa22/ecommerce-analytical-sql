use schema ecom_db.ecom_dw;

-- =============================================================================
-- question 3 | measure year-to-date profit to assess annual performance
--               progression.
-- =============================================================================

with daily_profit as (
    select
        dd.year,
        dd.month,
        dd.full_date,
        sum(fol.profit_amount) as daily_profit
    from fact_order_line fol
    inner join dim_date dd on fol.date_key = dd.date_key
    group by dd.year, dd.month, dd.full_date
)
select
    year,
    month,
    full_date,
    daily_profit,
    sum(daily_profit) over (
        partition by year
        order by full_date
        rows between unbounded preceding and current row
    ) as ytd_profit
from daily_profit
order by full_date;
