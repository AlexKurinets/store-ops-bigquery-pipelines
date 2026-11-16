select store_key, business_date, hours_without_rate
from {{ ref('labor_cost_store_day') }}
where hours_without_rate > 0
