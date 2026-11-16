select item_key, plu, distributor_item_number, item_name, category, is_fresh, units_per_case, list_price
from {{ ref('items') }}
