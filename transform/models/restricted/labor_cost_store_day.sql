-- Pay rates never leave the restricted datasets. Only this store-day total does, into the metrics layer.
with rates as (
    select employee_key, pay_rate,
           row_number() over (partition by employee_key order by _batch_date desc, _loaded_at desc) as version_rank
    from {{ source('raw_restricted', 'timekeeping_employees') }}
)

select
    sh.store_key,
    sh.business_date,
    sum(sh.paid_hours * r.pay_rate) as labor_cost,
    sum(case when r.pay_rate is null then sh.paid_hours else 0 end) as hours_without_rate
from {{ ref('fct_labor_shifts') }} as sh
left join rates as r on r.employee_key = sh.employee_key and r.version_rank = 1
group by sh.store_key, sh.business_date
