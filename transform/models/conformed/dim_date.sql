with days as ({{ date_spine('2026-01-01', '2027-12-31') }})

select
    date_day,
    {{ iso_weekday('date_day') }} as iso_weekday,
    {{ week_start('date_day') }} as fiscal_week_start,
    {{ month_key('date_day') }} as fiscal_month,
    {{ iso_weekday('date_day') }} in (6, 7) as is_weekend
from days
