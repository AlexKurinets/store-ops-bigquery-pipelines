-- Every open store-day expects one opening checklist; a missed checklist counts against completion.
with bounds as (select first_loaded_date, last_complete_date from {{ ref('load_status') }}),

open_days as (
    select store_key, business_date
    from {{ ref('store_day_sales_labor') }}
    where transactions > 0
),

submitted as (
    select store_key, business_date,
           count(distinct case when checklist = 'opening' then submission_id end) as opening_checklists,
           sum(case when result = 'fail' then 1 else 0 end) as items_failed,
           count(*) as items_answered
    from {{ ref('fct_checklist_items') }}
    group by store_key, business_date
)

select
    o.store_key,
    {{ week_start('o.business_date') }} as week_start,
    count(*) as open_days,
    sum(case when coalesce(s.opening_checklists, 0) > 0 then 1 else 0 end) as opening_checklists_done,
    round({{ safe_ratio('sum(case when coalesce(s.opening_checklists, 0) > 0 then 1 else 0 end)', 'count(*)') }}, 4)
        as completion_pct,
    sum(coalesce(s.items_failed, 0)) as items_failed,
    round(1 - {{ safe_ratio('sum(coalesce(s.items_failed, 0))', 'sum(coalesce(s.items_answered, 0))') }}, 4)
        as pass_rate_pct,
    {{ dbt.dateadd('day', 6, week_start('o.business_date')) }} <= max(b.last_complete_date)
        and {{ week_start('o.business_date') }} >= min(b.first_loaded_date) as is_complete_week
from open_days as o
left join submitted as s on s.store_key = o.store_key and s.business_date = o.business_date
cross join bounds as b
where o.business_date between b.first_loaded_date and b.last_complete_date
group by o.store_key, {{ week_start('o.business_date') }}
