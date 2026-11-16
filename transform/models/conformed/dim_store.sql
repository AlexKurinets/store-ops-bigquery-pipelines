select
    s.store_key,
    s.store_name,
    s.state,
    s.timezone,
    s.day_start_hour,
    s.is_24h,
    s.opened_on,
    max(case when x.system = 'pos' and x.valid_to is null then x.source_code end) as current_pos_number,
    max(case when x.system = 'timekeeping' then x.source_code end) as timekeeping_location_code,
    max(case when x.system = 'distributor' then x.source_code end) as distributor_ship_to,
    max(case when x.system = 'audits' then x.source_code end) as audit_site_id
from {{ ref('stores') }} as s
left join {{ ref('store_xref') }} as x on x.store_key = s.store_key
group by s.store_key, s.store_name, s.state, s.timezone, s.day_start_hour, s.is_24h, s.opened_on
