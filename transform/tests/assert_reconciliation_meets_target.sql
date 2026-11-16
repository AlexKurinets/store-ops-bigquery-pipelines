-- Definition of done: store and date keys reconcile between POS and labor at 99% or better.
select count(*) as store_days,
       sum(case when status = 'matched' then 1 else 0 end) as matched
from {{ ref('key_reconciliation_daily') }}
having sum(case when status = 'matched' then 1 else 0 end) < {{ var('recon_target') }} * count(*)
