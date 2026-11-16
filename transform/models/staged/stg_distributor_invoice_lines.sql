with ranked as (
    select
        *,
        row_number() over (partition by invoice_no, line_no order by _loaded_at desc) as version_rank
    from {{ source('raw', 'distributor_invoice_lines') }}
)

select
    invoice_no, line_no, ship_to, delivery_date, item_number, description, cases, units_per_case,
    cases * units_per_case as units, case_cost, extended_cost, source_file
from ranked
where version_rank = 1
