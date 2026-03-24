USE WAREHOUSE COMPUTE_WH;
USE SCHEMA ECOM_DB.ECOM_DW;


-- =============================================================================
-- question 18 | detect trending categories based on recent performance
--               compared to historical behavior.
-- =============================================================================

with date_bounds as (
    select
        max(full_date)                       as max_date,
        dateadd('month', -6, max(full_date)) as cutoff_date
    from dim_date
),
category_periods as (
    select
        dc.category_key,
        dc.category_name,
        sum(case
                when dd.full_date >  db.cutoff_date then fol.net_amount
                else 0
            end) as recent_revenue,
        sum(case
                when dd.full_date <= db.cutoff_date then fol.net_amount
                else 0
            end) as historical_revenue
    from fact_order_line  fol
    inner join dim_date     dd on fol.date_key     = dd.date_key
    inner join dim_category dc on fol.category_key = dc.category_key
    cross join date_bounds  db
    group by dc.category_key, dc.category_name
)
select
    category_name,
    round(recent_revenue,     2)                                                    as recent_revenue,
    round(historical_revenue, 2)                                                    as historical_revenue,
    round(
        (recent_revenue - historical_revenue) / nullif(historical_revenue, 0) * 100
    , 2)                                                                            as growth_rate_pct,
    rank() over (
        order by
            (recent_revenue - historical_revenue) / nullif(historical_revenue, 0)
        desc nulls last
    )                                                                               as trend_rank,
    case
        when historical_revenue = 0               then 'new category'
        when recent_revenue  >  historical_revenue then 'trending up'
        when recent_revenue  <  historical_revenue then 'trending down'
        else                                            'stable'
    end                                                                             as trend_status
from category_periods
order by trend_rank;