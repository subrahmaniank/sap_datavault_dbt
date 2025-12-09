-- Test: Verify insert-only pattern - no duplicate hash keys with same load_date in hubs
-- Data Vault 2.0 requires insert-only, so same hash key should only appear once
select 
    hk_customer_h,
    count(*) as duplicate_count
from {{ ref('hub_customer') }}
group by hk_customer_h
having count(*) > 1

