-- Record counts for Raw Vault and Business Vault
{{ config(
    materialized = 'view',
    tags = ['business_vault', 'reporting']
) }}

-- Raw Vault - Hubs
select 'Raw Vault - Hubs' as layer, 'hub_customer' as table_name, count(*) as record_count
from {{ ref('hub_customer') }}
union all
select 'Raw Vault - Hubs', 'hub_material', count(*) from {{ ref('hub_material') }}
union all
select 'Raw Vault - Hubs', 'hub_order', count(*) from {{ ref('hub_order') }}
union all
select 'Raw Vault - Hubs', 'hub_order_item', count(*) from {{ ref('hub_order_item') }}

union all

-- Raw Vault - Links
select 'Raw Vault - Links', 'link_order_customer', count(*) from {{ ref('link_order_customer') }}
union all
select 'Raw Vault - Links', 'link_order_item', count(*) from {{ ref('link_order_item') }}
union all
select 'Raw Vault - Links', 'link_order_material', count(*) from {{ ref('link_order_material') }}

union all

-- Raw Vault - Satellites
select 'Raw Vault - Satellites', 'sat_sap_customer_kna1', count(*) from {{ ref('sat_sap_customer_kna1') }}
union all
select 'Raw Vault - Satellites', 'sat_sap_material_mara', count(*) from {{ ref('sat_sap_material_mara') }}
union all
select 'Raw Vault - Satellites', 'sat_sap_order_header_vbak', count(*) from {{ ref('sat_sap_order_header_vbak') }}
union all
select 'Raw Vault - Satellites', 'sat_sap_order_item_vbap', count(*) from {{ ref('sat_sap_order_item_vbap') }}

union all

-- Business Vault - Views (with effective dates)
select 'Business Vault - Views', 'bv_sat_sap_customer_kna1', count(*) from {{ ref('bv_sat_sap_customer_kna1') }}
union all
select 'Business Vault - Views', 'bv_sat_sap_material_mara', count(*) from {{ ref('bv_sat_sap_material_mara') }}
union all
select 'Business Vault - Views', 'bv_sat_sap_order_header_vbak', count(*) from {{ ref('bv_sat_sap_order_header_vbak') }}
union all
select 'Business Vault - Views', 'bv_sat_sap_order_item_vbap', count(*) from {{ ref('bv_sat_sap_order_item_vbap') }}

union all

-- Business Vault - Tables
select 'Business Vault - Tables', 'pit_customer_daily', count(*) from {{ ref('pit_customer_daily') }}
union all
select 'Business Vault - Tables', 'bridge_sales', count(*) from {{ ref('bridge_sales') }}

order by layer, table_name

