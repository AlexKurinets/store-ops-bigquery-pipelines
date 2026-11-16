select
    a.submission_id,
    x.store_key,
    {{ business_date('a.submitted_at', 's.timezone', 's.day_start_hour') }} as business_date,
    a.checklist,
    a.submitted_at,
    a.item_code,
    a.question,
    a.result,
    a.temperature_f,
    a.comment
from {{ ref('stg_audit_checklist_items') }} as a
join {{ ref('store_xref') }} as x on x.system = 'audits' and x.source_code = a.site_id
join {{ ref('stores') }} as s on s.store_key = x.store_key
