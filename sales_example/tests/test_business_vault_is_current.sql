-- Test: Verify exactly one is_current = true per parent key in Business Vault
-- Each customer should have exactly one current version
select 
    hk_customer_h,
    sum(case when is_current = true then 1 else 0 end) as current_count
from {{ ref('bv_sat_sap_customer_kna1') }}
group by hk_customer_h
having current_count != 1

