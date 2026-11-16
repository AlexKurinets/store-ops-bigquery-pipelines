-- No names, IDs or pay here: the key is an HMAC of the employee ID, the same one the POS operator ID hashes to.
select
    e.employee_key,
    x.store_key as home_store_key,
    e.job_code,
    e.hire_date,
    e.termination_date,
    e.termination_date is null as is_active
from {{ ref('stg_timekeeping_employees') }} as e
left join {{ ref('store_xref') }} as x
    on x.system = 'timekeeping' and x.source_code = e.home_location_code
