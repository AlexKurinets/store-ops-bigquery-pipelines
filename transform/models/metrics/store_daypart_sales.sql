select
    p.store_key,
    p.business_date,
    p.daypart,
    d.daypart_order,
    round(sum(p.net_sales), 2) as net_sales,
    count(distinct case when not p.is_voided then p.transaction_id end) as transactions
from {{ ref('fct_pos_lines') }} as p
join (select distinct daypart, daypart_order from {{ ref('dayparts') }}) as d on d.daypart = p.daypart
cross join {{ ref('load_status') }} as b
where p.business_date between b.first_loaded_date and b.last_complete_date
group by p.store_key, p.business_date, p.daypart, d.daypart_order
