-- Employees arrive as a full snapshot on every run; the latest snapshot wins.
with ranked as (
    select
        *,
        row_number() over (partition by employee_key order by _batch_date desc, _loaded_at desc) as version_rank
    from {{ source('raw', 'timekeeping_employees') }}
)

select employee_key, home_location_code, job_code, hire_date, termination_date
from ranked
where version_rank = 1
