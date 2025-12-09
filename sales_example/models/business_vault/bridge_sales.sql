-- models/business_vault/bridge/bridge_sales.sql
{{ config(
    materialized = 'table',
    alias = 'bridge_sales',
    tags = ['business_vault', 'bridge', 'sales', 'bi_accelerator', 'critical'],
    post_hook = [
        "{{ log('BRIDGE_SALES – denormalized sales rows ready for BI', info=True) }}"
    ]
) }}

-- Use Business Vault satellite models which include is_current flags
-- Filter to latest versions using is_current flag (computed in Business Vault)
with latest_order_items as (
    select *
    from {{ ref('bv_sat_sap_order_item_vbap') }}
    where is_current = true
),

latest_order_headers as (
    select *
    from {{ ref('bv_sat_sap_order_header_vbak') }}
    where is_current = true
),

latest_customers as (
    select *
    from {{ ref('bv_sat_sap_customer_kna1') }}
    where is_current = true
),

latest_materials as (
    select *
    from {{ ref('bv_sat_sap_material_mara') }}
    where is_current = true
)

select
    -- Primary keys (for direct joins in BI tools)
    oi.hk_order_item_h          as sales_line_hk,
    loi.hk_order_h              as sales_header_hk,
    c.hk_customer_h             as customer_hk,
    m.hk_material_h             as material_hk,

    -- Business keys (human readable)
    ho.order_bk                 as sales_document,
    hoi.order_item_bk           as sales_document_item,
    hc.customer_bk              as customer_number,
    hm.material_bk              as material_number,

    -- Dates
    hdr.order_date              as order_date,
    hdr.document_date          as document_date,

    -- Customer attributes (current version)
    c.customer_name,
    c.city                      as customer_city,
    c.country_code              as customer_country,
    c.region_code               as customer_region,

    -- Material attributes (current version)
    m.material_type,
    m.material_type_description,
    m.material_group,
    m.base_unit_of_measure      as material_uom,
    m.weight_category,

    -- Line item facts
    oi.quantity,
    oi.unit_of_measure          as item_uom,
    oi.net_value_item           as net_amount_local,
    oi.currency_code            as local_currency,
    oi.unit_price               as unit_price_local,
    oi.plant,
    oi.storage_location,
    oi.item_status,

    -- Header-level facts (denormalized)
    hdr.net_value_header        as net_amount_header_local,
    hdr.currency_code           as header_currency,
    hdr.sales_organization,
    hdr.distribution_channel,
    hdr.division,
    hdr.sales_office,
    hdr.sales_group,
    hdr.order_type,
    hdr.order_type_description,

    -- Calculated KPIs (ready for dashboards)
    oi.quantity * oi.unit_price as gross_amount_local,
    hdr.net_value_header        as total_order_value_local,

    -- Metadata
    oi.record_source            as item_record_source,
    hdr.record_source           as header_record_source,
    greatest(oi.load_date, hdr.load_date, c.load_date, m.load_date) as bridge_load_date,
    current_timestamp()         as bridge_created_at

from latest_order_items oi
join {{ ref('hub_order_item') }}              hoi  on oi.hk_order_item_h = hoi.hk_order_item_h
join {{ ref('link_order_item') }}             loi  on oi.hk_order_item_h = loi.hk_order_item_h
join {{ ref('hub_order') }}                   ho   on loi.hk_order_h = ho.hk_order_h
join latest_order_headers                      hdr  on loi.hk_order_h = hdr.hk_order_h
join {{ ref('link_order_customer') }}         loc  on loi.hk_order_h = loc.hk_order_h
join {{ ref('hub_customer') }}                hc   on loc.hk_customer_h = hc.hk_customer_h
join latest_customers                          c    on loc.hk_customer_h = c.hk_customer_h
join {{ ref('link_order_material') }}         lom  on oi.hk_order_item_h = lom.hk_order_item_h
join {{ ref('hub_material') }}                hm   on lom.hk_material_h = hm.hk_material_h
join latest_materials                          m    on lom.hk_material_h = m.hk_material_h

order by ho.order_bk, hoi.order_item_bk
