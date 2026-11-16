-- Fuzzy name matching once paired the wrong two stores. Every budget row must map through an explicit alias.
select fiscal_month, store_name
from {{ ref('stg_budget_store_month') }} as b
left join {{ ref('store_xref') }} as x on x.system = 'budget' and x.source_code = b.store_name_normalized
where x.store_key is null
