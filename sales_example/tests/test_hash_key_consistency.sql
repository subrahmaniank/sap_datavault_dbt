-- Test: Verify hash keys are consistent between staging and hubs
-- Hash keys in hubs should match those generated in staging
select 
    'hub_customer' as table_name,
    h.hk_customer_h,
    h.customer_bk,
    s.hk_customer_h as staging_hash
from {{ ref('hub_customer') }} h
inner join {{ ref('stg_sap__kna1') }} s on h.customer_bk = s.customer_bk
where h.hk_customer_h != s.hk_customer_h

union all

select 
    'hub_material' as table_name,
    h.hk_material_h,
    h.material_bk,
    s.hk_material_h as staging_hash
from {{ ref('hub_material') }} h
inner join {{ ref('stg_sap__mara') }} s on h.material_bk = s.material_bk
where h.hk_material_h != s.hk_material_h

