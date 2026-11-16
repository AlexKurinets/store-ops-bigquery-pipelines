with ranked as (
    select
        *,
        row_number() over (partition by shift_id order by modified_at desc, _loaded_at desc) as version_rank
    from {{ source('raw', 'timekeeping_shifts') }}
)

select shift_id, employee_key, location_code, job_code, clock_in, clock_out, break_minutes, _batch_date
from ranked
where version_rank = 1 and clock_out is not null
