-- The latest freshness check per source, so an agent can see whether the data it is about to use is current.
-- A view, because the checks run after the build; a table would hold the previous check.
{{ config(materialized='view') }}

with ranked as (
    select *, row_number() over (partition by source order by checked_at desc) as check_rank
    from {{ source('ops', 'freshness_checks') }}
)

select source, status, sla, expected_batch, latest_batch, landed_at, due_at, checked_at
from ranked
where check_rank = 1
