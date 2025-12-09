-- Test: Verify bridge table has complete data
-- All order items should have corresponding customer and material data
select 
    bs.sales_line_hk,
    bs.sales_document,
    bs.customer_number,
    bs.material_number
from {{ ref('bridge_sales') }} bs
where bs.customer_name is null
   or bs.material_type is null
   or bs.order_date is null

