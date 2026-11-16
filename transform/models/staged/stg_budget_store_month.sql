-- Finance re-uploads a workbook to correct it. The newest object generation for a month replaces the older one.
-- Generations are opaque numbers (Cloud Storage uses microsecond timestamps), so versions are ranked per month.
with generations as (
    select
        fiscal_month,
        source_generation,
        dense_rank() over (partition by fiscal_month order by source_generation) as budget_version,
        dense_rank() over (partition by fiscal_month order by source_generation desc) as recency
    from (select distinct fiscal_month, source_generation from {{ source('raw', 'budget_store_month') }}) as g
)

select
    b.fiscal_month,
    b.store_name,
    {{ normalize_name('b.store_name') }} as store_name_normalized,
    b.budget_net_sales,
    b.budget_labor_hours,
    b.budget_labor_cost,
    b.source_object,
    b.source_generation,
    g.budget_version
from {{ source('raw', 'budget_store_month') }} as b
join generations as g
    on g.fiscal_month = b.fiscal_month and g.source_generation = b.source_generation
where g.recency = 1
