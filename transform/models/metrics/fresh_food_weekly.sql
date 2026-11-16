with bounds as (select first_loaded_date, last_complete_date from {{ ref('load_status') }}),

sold as (
    select p.store_key, {{ week_start('p.business_date') }} as week_start, p.item_key,
           sum(case when p.is_voided then 0 else p.quantity end) as units_sold
    from {{ ref('fct_pos_lines') }} as p
    cross join bounds as b
    where p.business_date between b.first_loaded_date and b.last_complete_date
    group by p.store_key, {{ week_start('p.business_date') }}, p.item_key
),

received as (
    select r.store_key, {{ week_start('r.delivery_date') }} as week_start, r.item_key, sum(r.units) as units_received
    from {{ ref('fct_purchase_lines') }} as r
    cross join bounds as b
    where r.delivery_date between b.first_loaded_date and b.last_complete_date
    group by r.store_key, {{ week_start('r.delivery_date') }}, r.item_key
)

select
    r.store_key,
    r.week_start,
    i.item_key,
    i.item_name,
    r.units_received,
    coalesce(s.units_sold, 0) as units_sold,
    round({{ safe_ratio('coalesce(s.units_sold, 0)', 'r.units_received') }}, 4) as sell_through_pct,
    -- Friday's delivery covers the weekend, so a week is only comparable once all seven days are loaded.
    {{ dbt.dateadd('day', 6, 'r.week_start') }} <= (select last_complete_date from bounds)
        and r.week_start >= (select first_loaded_date from bounds) as is_complete_week
from received as r
join {{ ref('dim_item') }} as i on i.item_key = r.item_key and i.is_fresh
left join sold as s on s.store_key = r.store_key and s.week_start = r.week_start and s.item_key = r.item_key
