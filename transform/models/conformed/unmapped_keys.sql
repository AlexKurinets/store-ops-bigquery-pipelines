-- Source codes that no crosswalk row maps. Their rows are kept out of the facts and listed here, with when they
-- were first and last seen, until someone adds the crosswalk row. Nothing is silently dropped.
with seen as (
    select 'pos' as system, store_number as source_code, cast(closed_at as date) as seen_on
    from {{ ref('stg_pos_lines') }}
    union all
    select 'timekeeping', location_code, cast(clock_in as date) from {{ ref('stg_timekeeping_shifts') }}
    union all
    select 'timekeeping', location_code, business_date from {{ ref('stg_timekeeping_daily_sales') }}
    union all
    select 'distributor', ship_to, delivery_date from {{ ref('stg_distributor_invoice_lines') }}
    union all
    select 'audits', site_id, cast(submitted_at as date) from {{ ref('stg_audit_checklist_items') }}
    union all
    select 'budget', store_name_normalized, cast(null as date) from {{ ref('stg_budget_store_month') }}
)

select
    s.system,
    s.source_code,
    count(*) as row_count,
    min(s.seen_on) as first_seen,
    max(s.seen_on) as last_seen
from seen as s
left join {{ ref('store_xref') }} as x on x.system = s.system and x.source_code = s.source_code
where x.source_code is null
group by s.system, s.source_code
