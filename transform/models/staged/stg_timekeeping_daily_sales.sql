with ranked as (
    select
        *,
        row_number() over (
            partition by location_code, business_date order by imported_at desc, _loaded_at desc
        ) as version_rank
    from {{ source('raw', 'timekeeping_daily_sales') }}
)

select location_code, business_date, net_sales, imported_at
from ranked
where version_rank = 1
