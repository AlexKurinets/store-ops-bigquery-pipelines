-- Each POS run reloads three business days, so a line appears in several batches. Keep its latest version,
-- which carries a void posted after close.
with ranked as (
    select
        *,
        row_number() over (
            partition by transaction_id, line_number
            order by updated_at desc, _loaded_at desc
        ) as version_rank
    from {{ source('raw', 'pos_transaction_lines') }}
)

select
    transaction_id,
    line_number,
    store_number,
    register_id,
    operator_key,
    closed_at,
    received_at,
    voided_at,
    tender_type,
    plu,
    description,
    quantity,
    unit_price,
    extended_price,
    _batch_date,
    _loaded_at
from ranked
where version_rank = 1
