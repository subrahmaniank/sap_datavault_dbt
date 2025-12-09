-- Test Coverage Report
-- Shows which models have tests and which don't

with models as (
    select
        'model' as resource_type,
        name as resource_name,
        original_file_path,
        tags
    from {{ ref('_record_counts') }}  -- Using our existing model as a proxy
    union all
    select 'model', 'stg_sap__kna1', 'models/staging/sap/stg_sap__kna1.sql', array_construct('staging')
    union all
    select 'model', 'stg_sap__mara', 'models/staging/sap/stg_sap__mara.sql', array_construct('staging')
    union all
    select 'model', 'stg_sap__vbak', 'models/staging/sap/stg_sap__vbak.sql', array_construct('staging')
    union all
    select 'model', 'stg_sap__vbap', 'models/staging/sap/stg_sap__vbap.sql', array_construct('staging')
    union all
    select 'model', 'stg_sap__vbpa', 'models/staging/sap/stg_sap__vbpa.sql', array_construct('staging')
    union all
    select 'model', 'hub_customer', 'models/raw_vault/hub/hub_customer.sql', array_construct('raw_vault', 'hub')
    union all
    select 'model', 'hub_material', 'models/raw_vault/hub/hub_material.sql', array_construct('raw_vault', 'hub')
    union all
    select 'model', 'hub_order', 'models/raw_vault/hub/hub_order.sql', array_construct('raw_vault', 'hub')
    union all
    select 'model', 'hub_order_item', 'models/raw_vault/hub/hub_order_item.sql', array_construct('raw_vault', 'hub')
    union all
    select 'model', 'link_order_customer', 'models/raw_vault/link/link_order_customer.sql', array_construct('raw_vault', 'link')
    union all
    select 'model', 'link_order_item', 'models/raw_vault/link/link_order_item.sql', array_construct('raw_vault', 'link')
    union all
    select 'model', 'link_order_material', 'models/raw_vault/link/link_order_material.sql', array_construct('raw_vault', 'link')
    union all
    select 'model', 'sat_sap_customer_kna1', 'models/raw_vault/satellite/sat_sap_customer_kna1.sql', array_construct('raw_vault', 'satellite')
    union all
    select 'model', 'sat_sap_material_mara', 'models/raw_vault/satellite/sat_sap_material_mara.sql', array_construct('raw_vault', 'satellite')
    union all
    select 'model', 'sat_sap_order_header_vbak', 'models/raw_vault/satellite/sat_sap_order_header_vbak.sql', array_construct('raw_vault', 'satellite')
    union all
    select 'model', 'sat_sap_order_item_vbap', 'models/raw_vault/satellite/sat_sap_order_item_vbap.sql', array_construct('raw_vault', 'satellite')
    union all
    select 'model', 'bv_sat_sap_customer_kna1', 'models/business_vault/bv_sat_sap_customer_kna1.sql', array_construct('business_vault')
    union all
    select 'model', 'bv_sat_sap_material_mara', 'models/business_vault/bv_sat_sap_material_mara.sql', array_construct('business_vault')
    union all
    select 'model', 'bv_sat_sap_order_header_vbak', 'models/business_vault/bv_sat_sap_order_header_vbak.sql', array_construct('business_vault')
    union all
    select 'model', 'bv_sat_sap_order_item_vbap', 'models/business_vault/bv_sat_sap_order_item_vbap.sql', array_construct('business_vault')
    union all
    select 'model', 'pit_customer_daily', 'models/business_vault/pit_customer_daily.sql', array_construct('business_vault')
    union all
    select 'model', 'bridge_sales', 'models/business_vault/bridge_sales.sql', array_construct('business_vault')
),

test_counts as (
    -- This would need to query the dbt manifest, but for now we'll use a simplified approach
    select
        'hub_customer' as model_name, 6 as test_count
    union all select 'hub_material', 5
    union all select 'hub_order', 5
    union all select 'hub_order_item', 5
    union all select 'link_order_customer', 8
    union all select 'link_order_item', 6
    union all select 'link_order_material', 6
    union all select 'sat_sap_customer_kna1', 6
    union all select 'sat_sap_material_mara', 4
    union all select 'sat_sap_order_header_vbak', 3
    union all select 'sat_sap_order_item_vbap', 3
    union all select 'stg_sap__kna1', 5
    union all select 'stg_sap__mara', 3
    union all select 'stg_sap__vbak', 2
    union all select 'stg_sap__vbap', 2
    union all select 'stg_sap__vbpa', 1
    union all select 'bv_sat_sap_customer_kna1', 5
    union all select 'bv_sat_sap_material_mara', 2
    union all select 'bv_sat_sap_order_header_vbak', 1
    union all select 'bv_sat_sap_order_item_vbap', 1
    union all select 'pit_customer_daily', 3
    union all select 'bridge_sales', 5
)

select
    m.resource_name,
    m.original_file_path,
    array_to_string(m.tags, ', ') as tags,
    coalesce(t.test_count, 0) as test_count,
    case
        when coalesce(t.test_count, 0) = 0 then '❌ No Tests'
        when coalesce(t.test_count, 0) < 3 then '⚠️ Low Coverage'
        when coalesce(t.test_count, 0) < 5 then '✅ Good Coverage'
        else '✅✅ Excellent Coverage'
    end as coverage_status
from models m
left join test_counts t on m.resource_name = t.model_name
order by 
    case 
        when coalesce(t.test_count, 0) = 0 then 1
        when coalesce(t.test_count, 0) < 3 then 2
        when coalesce(t.test_count, 0) < 5 then 3
        else 4
    end,
    m.resource_name

