select
    p.invoice_no,
    p.line_no,
    x.store_key,
    p.delivery_date,
    i.item_key,
    p.item_number,
    p.cases,
    p.units_per_case,
    p.units,
    p.case_cost,
    p.extended_cost,
    p.source_file
from {{ ref('stg_distributor_invoice_lines') }} as p
join {{ ref('store_xref') }} as x
    on x.system = 'distributor' and x.source_code = p.ship_to
    and p.delivery_date between x.valid_from and coalesce(x.valid_to, {{ end_of_time() }})
left join {{ ref('items') }} as i on i.distributor_item_number = p.item_number
