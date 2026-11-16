-- Sales and labor so far in the month against the budget for the same share of the month, so a month in progress
-- isn't compared with a whole month's budget.
with actual as (
    select store_key, {{ month_key('business_date') }} as fiscal_month, sum(net_sales) as net_sales,
           sum(labor_hours) as labor_hours, count(*) as days_with_data
    from {{ ref('store_day_sales_labor') }}
    group by store_key, {{ month_key('business_date') }}
),

month_days as (
    select fiscal_month, count(*) as days_in_month from {{ ref('dim_date') }} group by fiscal_month
)

select
    b.store_key,
    b.fiscal_month,
    coalesce(a.days_with_data, 0) as days_with_data,
    m.days_in_month,
    round(coalesce(a.net_sales, 0), 2) as net_sales,
    b.budget_net_sales,
    round(b.budget_net_sales * coalesce(a.days_with_data, 0) / m.days_in_month, 2) as budget_net_sales_to_date,
    round({{ safe_ratio('a.net_sales', 'b.budget_net_sales * a.days_with_data / m.days_in_month') }}, 4)
        as sales_vs_budget_pct,
    round(coalesce(a.labor_hours, 0), 1) as labor_hours,
    b.budget_labor_hours,
    round({{ safe_ratio('a.labor_hours', 'b.budget_labor_hours * a.days_with_data / m.days_in_month') }}, 4)
        as labor_hours_vs_budget_pct,
    b.budget_version
from {{ ref('fct_store_budget_month') }} as b
join month_days as m on m.fiscal_month = b.fiscal_month
left join actual as a on a.store_key = b.store_key and a.fiscal_month = b.fiscal_month
