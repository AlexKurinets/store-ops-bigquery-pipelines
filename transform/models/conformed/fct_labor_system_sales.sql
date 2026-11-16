select
    x.store_key,
    ds.business_date,
    ds.net_sales as labor_system_net_sales,
    ds.imported_at
from {{ ref('stg_timekeeping_daily_sales') }} as ds
join {{ ref('store_xref') }} as x
    on x.system = 'timekeeping' and x.source_code = ds.location_code and x.is_reportable
