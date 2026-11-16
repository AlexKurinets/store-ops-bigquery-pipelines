-- POS against the timekeeping system's own copy of daily sales, per store and business date.
-- A presence-only check (both systems have a row) passes even when the date rule is wrong, so this compares totals.
-- When they differ, the POS total as of the labor system's import time says whether a later change explains it.
with bounds as (
    select first_loaded_date, least(last_complete_date, last_reconcilable_date) as last_date
    from {{ ref('load_status') }}
),

pos as (
    select store_key, business_date, sum(net_sales) as pos_net_sales
    from {{ ref('fct_pos_lines') }}
    group by store_key, business_date
),

labor as (
    select store_key, business_date, labor_system_net_sales, imported_at
    from {{ ref('fct_labor_system_sales') }}
),

pos_at_import as (
    select
        p.store_key,
        p.business_date,
        sum(case when p.received_at <= l.imported_at and (p.voided_at is null or p.voided_at > l.imported_at)
                 then p.extended_price else 0 end) as pos_net_sales_at_import
    from {{ ref('fct_pos_lines') }} as p
    join labor as l on l.store_key = p.store_key and l.business_date = p.business_date
    group by p.store_key, p.business_date
),

pos_calendar as (
    -- For comparison only: the same POS lines bucketed by local calendar date, the rule that failed.
    select p.store_key, {{ local_date('p.closed_at', 's.timezone') }} as calendar_date, sum(p.net_sales) as pos_net_sales
    from {{ ref('fct_pos_lines') }} as p
    join {{ ref('stores') }} as s on s.store_key = p.store_key
    group by p.store_key, {{ local_date('p.closed_at', 's.timezone') }}
),

joined as (
    select
        coalesce(pos.store_key, labor.store_key) as store_key,
        coalesce(pos.business_date, labor.business_date) as business_date,
        pos.pos_net_sales,
        labor.labor_system_net_sales,
        labor.imported_at as labor_imported_at,
        pai.pos_net_sales_at_import,
        cal.pos_net_sales as pos_net_sales_calendar_date
    from pos
    full outer join labor on labor.store_key = pos.store_key and labor.business_date = pos.business_date
    left join pos_at_import as pai
        on pai.store_key = coalesce(pos.store_key, labor.store_key)
        and pai.business_date = coalesce(pos.business_date, labor.business_date)
    left join pos_calendar as cal
        on cal.store_key = coalesce(pos.store_key, labor.store_key)
        and cal.calendar_date = coalesce(pos.business_date, labor.business_date)
),

scored as (
    select
        j.*,
        greatest({{ var('recon_tolerance_pct') }} * abs(coalesce(j.labor_system_net_sales, 0)),
                 {{ var('recon_tolerance_abs') }}) as tolerance
    from joined as j
    cross join bounds as b
    where j.business_date between b.first_loaded_date and b.last_date
)

select
    store_key,
    business_date,
    round(pos_net_sales, 2) as pos_net_sales,
    round(labor_system_net_sales, 2) as labor_system_net_sales,
    round(pos_net_sales - labor_system_net_sales, 2) as difference,
    round(pos_net_sales_at_import, 2) as pos_net_sales_at_import,
    labor_imported_at,
    case
        when pos_net_sales is not null and labor_system_net_sales is not null
             and abs(pos_net_sales - labor_system_net_sales) <= tolerance then 'matched'
        else 'mismatch'
    end as status,
    case
        when labor_system_net_sales is null then 'missing_in_labor_system'
        when pos_net_sales is null then 'missing_in_pos'
        when abs(pos_net_sales - labor_system_net_sales) <= tolerance then null
        when abs(pos_net_sales_at_import - labor_system_net_sales) <= tolerance then 'pos_changed_after_labor_import'
        else 'unexplained'
    end as reason,
    coalesce(abs(pos_net_sales_calendar_date - labor_system_net_sales) <= tolerance, false) as matched_calendar_date,
    round(pos_net_sales_calendar_date, 2) as pos_net_sales_calendar_date
from scored
