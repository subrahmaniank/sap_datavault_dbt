-- models/business_vault/bv_sat_sap_order_item_vbap.sql
-- Business Vault view that adds effective dates to Raw Vault satellite
-- This computes effective_from, effective_to, and is_current from load_date
{{ config(
    materialized = 'view',
    alias = 'bv_sat_sap_order_item_vbap',
    tags = ['business_vault', 'satellite', 'order_item', 'effective_dates']
) }}

select
    -- All columns from Raw Vault satellite
    hk_order_item_h,
    quantity,
    unit_of_measure,
    net_value_item,
    currency_code,
    plant,
    storage_location,
    unit_price,
    item_status,
    hashdiff,
    record_source,
    load_date,
    
    -- Computed effective dates (Business Vault logic)
    -- effective_from: when this version became active (same as load_date)
    load_date as effective_from,
    
    -- effective_to: when the next version became active (or far future if current)
    coalesce(
        lead(load_date) over (
            partition by hk_order_item_h
            order by load_date
        ),
        '9999-12-31'::timestamp_ntz
    ) as effective_to,
    
    -- is_current: true if this is the latest version for this order item
    lead(load_date) over (
        partition by hk_order_item_h
        order by load_date
    ) is null as is_current

from {{ ref('sat_sap_order_item_vbap') }}

order by hk_order_item_h, load_date

