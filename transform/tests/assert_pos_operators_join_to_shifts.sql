-- Hashed employee keys must still join: at least 99% of POS lines have an operator who was clocked in at the
-- same store when the ticket closed.
with lines as (
    select p.transaction_id, p.line_number,
           max(case when sh.shift_id is not null then 1 else 0 end) as on_shift
    from {{ ref('fct_pos_lines') }} as p
    left join {{ ref('fct_labor_shifts') }} as sh
        on sh.employee_key = p.operator_key and sh.store_key = p.store_key
        and p.closed_at between sh.clock_in and sh.clock_out
    group by p.transaction_id, p.line_number
)
select count(*) as lines, sum(on_shift) as on_shift
from lines
having sum(on_shift) < 0.99 * count(*)
