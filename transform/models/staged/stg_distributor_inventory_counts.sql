with ranked as (
    select
        *,
        row_number() over (partition by ship_to, count_date, item_number order by _loaded_at desc) as version_rank
    from {{ source('raw', 'distributor_inventory_counts') }}
)

select ship_to, count_date, item_number, units_on_hand, source_file
from ranked
where version_rank = 1
