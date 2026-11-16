-- The training room has shifts in the timekeeping system but is not a store, so its crosswalk row is marked
-- not reportable and its shifts are left out here (they are not "unmapped").
select
    sh.shift_id,
    x.store_key,
    {{ business_date('sh.clock_in', 's.timezone', 's.day_start_hour') }} as business_date,
    sh.employee_key,
    sh.job_code,
    sh.clock_in,
    sh.clock_out,
    sh.break_minutes,
    {{ hours_between('sh.clock_in', 'sh.clock_out') }} - sh.break_minutes / 60.0 as paid_hours
from {{ ref('stg_timekeeping_shifts') }} as sh
join {{ ref('store_xref') }} as x
    on x.system = 'timekeeping' and x.source_code = sh.location_code and x.is_reportable
join {{ ref('stores') }} as s on s.store_key = x.store_key
