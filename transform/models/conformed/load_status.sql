-- Load boundaries the metrics respect.
--   first_loaded_date    the first batch date in raw POS.
--   last_complete_date   the last business date POS and labor have both fully loaded. A run for date D also reads
--                        into D+1's early hours, so anything after the latest batch date is partial.
--   last_reconcilable    the last date the timekeeping system's own daily sales are in. Its import runs at 05:00
--                        local and the timekeeping job at 06:15 CT, so this is normally the latest day; if an
--                        import runs late, the reconciliation stops at the last date it covers.
select
    (select min(_batch_date) from {{ source('raw', 'pos_transaction_lines') }}) as first_loaded_date,
    least(
        (select max(_batch_date) from {{ source('raw', 'pos_transaction_lines') }}),
        (select max(_batch_date) from {{ source('raw', 'timekeeping_shifts') }})
    ) as last_complete_date,
    (select max(business_date) from {{ ref('stg_timekeeping_daily_sales') }}) as last_reconcilable_date
