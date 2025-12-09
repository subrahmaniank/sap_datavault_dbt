-- Test: Verify all satellites reference valid hubs
-- All parent keys in satellites must exist in their respective hubs
select 
    'sat_sap_customer_kna1' as satellite_name,
    sat.hk_customer_h
from {{ ref('sat_sap_customer_kna1') }} sat
left join {{ ref('hub_customer') }} hub on sat.hk_customer_h = hub.hk_customer_h
where hub.hk_customer_h is null

union all

select 
    'sat_sap_material_mara' as satellite_name,
    sat.hk_material_h
from {{ ref('sat_sap_material_mara') }} sat
left join {{ ref('hub_material') }} hub on sat.hk_material_h = hub.hk_material_h
where hub.hk_material_h is null

union all

select 
    'sat_sap_order_header_vbak' as satellite_name,
    sat.hk_order_h
from {{ ref('sat_sap_order_header_vbak') }} sat
left join {{ ref('hub_order') }} hub on sat.hk_order_h = hub.hk_order_h
where hub.hk_order_h is null

union all

select 
    'sat_sap_order_item_vbap' as satellite_name,
    sat.hk_order_item_h
from {{ ref('sat_sap_order_item_vbap') }} sat
left join {{ ref('hub_order_item') }} hub on sat.hk_order_item_h = hub.hk_order_item_h
where hub.hk_order_item_h is null

