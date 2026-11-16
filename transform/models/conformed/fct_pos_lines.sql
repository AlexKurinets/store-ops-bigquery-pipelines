{{ config(
    materialized='incremental',
    incremental_strategy=day_replace_strategy(),
    unique_key='business_date',
    partition_by={'field': 'business_date', 'data_type': 'date'},
    cluster_by=['store_key'],
    on_schema_change='append_new_columns'
) }}

-- A POS store number maps to a store only for the dates it was valid, and the business date depends on that
-- store's time zone, so candidates are joined first and the validity check uses each candidate's business date.
with lines as (
    select *
    from {{ ref('stg_pos_lines') }}
    {% if is_incremental() %}
    where _batch_date >= {{ dbt.dateadd('day', -var('lookback_days'), "(select max(business_date) from " ~ this ~ ")") }}
    {% endif %}
),

candidates as (
    select
        l.*,
        x.store_key,
        x.valid_from,
        x.valid_to,
        {{ business_date('l.closed_at', 's.timezone', 's.day_start_hour') }} as business_date,
        {{ local_hour('l.closed_at', 's.timezone') }} as local_hour
    from lines as l
    join {{ ref('store_xref') }} as x on x.system = 'pos' and x.source_code = l.store_number
    join {{ ref('stores') }} as s on s.store_key = x.store_key
)

select
    c.transaction_id,
    c.line_number,
    c.store_key,
    c.business_date,
    c.local_hour,
    d.daypart,
    c.closed_at,
    c.received_at,
    c.voided_at,
    c.voided_at is not null as is_voided,
    c.operator_key,
    i.item_key,
    c.plu,
    c.quantity,
    c.extended_price,
    case when c.voided_at is null then c.extended_price else 0 end as net_sales,
    c.tender_type,
    c._batch_date
from candidates as c
left join {{ ref('items') }} as i on i.plu = c.plu
left join {{ ref('dayparts') }} as d on d.local_hour = c.local_hour
where c.business_date between c.valid_from and coalesce(c.valid_to, {{ end_of_time() }})
{% if is_incremental() %}
  and c.business_date >= {{ dbt.dateadd('day', -var('lookback_days'), "(select max(business_date) from " ~ this ~ ")") }}
{% endif %}
