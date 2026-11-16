select
    x.store_key,
    c.count_date,
    i.item_key,
    c.item_number,
    c.units_on_hand
from {{ ref('stg_distributor_inventory_counts') }} as c
join {{ ref('store_xref') }} as x on x.system = 'distributor' and x.source_code = c.ship_to
left join {{ ref('items') }} as i on i.distributor_item_number = c.item_number
