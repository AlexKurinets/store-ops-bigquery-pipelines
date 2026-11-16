select
    p.store_key,
    p.business_date,
    p.daypart,
    i.item_key,
    i.item_name,
    sum(case when p.is_voided then 0 else p.quantity end) as units_sold
from {{ ref('fct_pos_lines') }} as p
join {{ ref('dim_item') }} as i on i.item_key = p.item_key and i.is_fresh
cross join {{ ref('load_status') }} as b
where p.business_date between b.first_loaded_date and b.last_complete_date
group by p.store_key, p.business_date, p.daypart, i.item_key, i.item_name
