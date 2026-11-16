with ranked as (
    select
        *,
        row_number() over (partition by submission_id, item_code order by _loaded_at desc) as version_rank
    from {{ source('raw', 'audit_checklist_items') }}
)

select submission_id, site_id, checklist, submitted_at, item_code, question, result, temperature_f, comment
from ranked
where version_rank = 1
