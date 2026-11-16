with bounds as (select first_loaded_date, last_complete_date from {{ ref('load_status') }}),

sales as (
    select store_key, business_date, sum(net_sales) as net_sales,
           count(distinct case when not is_voided then transaction_id end) as transactions
    from {{ ref('fct_pos_lines') }}
    group by store_key, business_date
),

labor as (
    select store_key, business_date, sum(paid_hours) as labor_hours
    from {{ ref('fct_labor_shifts') }}
    group by store_key, business_date
),

days as (
    select store_key, business_date from sales
    union distinct
    select store_key, business_date from labor
)

select
    d.store_key,
    st.store_name,
    d.business_date,
    round(coalesce(s.net_sales, 0), 2) as net_sales,
    coalesce(s.transactions, 0) as transactions,
    round(coalesce(l.labor_hours, 0), 2) as labor_hours,
    round(lc.labor_cost, 2) as labor_cost,
    round({{ safe_ratio('s.net_sales', 'l.labor_hours') }}, 2) as sales_per_labor_hour,
    round({{ safe_ratio('lc.labor_cost', 's.net_sales') }}, 4) as labor_cost_pct,
    coalesce(r.status = 'matched', false) as reconciled
from days as d
cross join bounds as b
join {{ ref('dim_store') }} as st on st.store_key = d.store_key
left join sales as s on s.store_key = d.store_key and s.business_date = d.business_date
left join labor as l on l.store_key = d.store_key and l.business_date = d.business_date
left join {{ ref('labor_cost_store_day') }} as lc on lc.store_key = d.store_key and lc.business_date = d.business_date
left join {{ ref('key_reconciliation_daily') }} as r on r.store_key = d.store_key and r.business_date = d.business_date
where d.business_date between b.first_loaded_date and b.last_complete_date
