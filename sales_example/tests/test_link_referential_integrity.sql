-- Test: Verify all links reference valid hubs
-- All foreign keys in links must exist in their respective hubs
select 
    'link_order_customer' as link_name,
    loc.hk_order_h,
    loc.hk_customer_h
from {{ ref('link_order_customer') }} loc
left join {{ ref('hub_order') }} ho on loc.hk_order_h = ho.hk_order_h
left join {{ ref('hub_customer') }} hc on loc.hk_customer_h = hc.hk_customer_h
where ho.hk_order_h is null or hc.hk_customer_h is null

union all

select 
    'link_order_item' as link_name,
    loi.hk_order_h,
    loi.hk_order_item_h
from {{ ref('link_order_item') }} loi
left join {{ ref('hub_order') }} ho on loi.hk_order_h = ho.hk_order_h
left join {{ ref('hub_order_item') }} hoi on loi.hk_order_item_h = hoi.hk_order_item_h
where ho.hk_order_h is null or hoi.hk_order_item_h is null

union all

select 
    'link_order_material' as link_name,
    lom.hk_order_item_h,
    lom.hk_material_h
from {{ ref('link_order_material') }} lom
left join {{ ref('hub_order_item') }} hoi on lom.hk_order_item_h = hoi.hk_order_item_h
left join {{ ref('hub_material') }} hm on lom.hk_material_h = hm.hk_material_h
where hoi.hk_order_item_h is null or hm.hk_material_h is null

