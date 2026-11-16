select
    x.store_key,
    b.fiscal_month,
    b.budget_net_sales,
    b.budget_labor_hours,
    b.budget_labor_cost,
    b.store_name as budget_store_name,
    b.source_generation,
    b.budget_version
from {{ ref('stg_budget_store_month') }} as b
join {{ ref('store_xref') }} as x on x.system = 'budget' and x.source_code = b.store_name_normalized
