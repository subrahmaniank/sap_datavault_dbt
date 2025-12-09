-- models/business_vault/bv_sat_sap_order_header_vbak.sql
-- Business Vault view that adds effective dates to Raw Vault satellite
-- This computes effective_from, effective_to, and is_current from load_date
{{ config(
    materialized = 'view',
    alias = 'bv_sat_sap_order_header_vbak',
    tags = ['business_vault', 'satellite', 'order_header', 'effective_dates']
) }}

select
    -- All columns from Raw Vault satellite
    hk_order_h,
    order_date,
    document_date,
    net_value_header,
    currency_code,
    sales_organization,
    distribution_channel,
    division,
    sales_office,
    sales_group,
    order_type,
    order_type_description,
    hashdiff,
    record_source,
    load_date,
    
    -- Computed effective dates (Business Vault logic)
    -- effective_from: when this version became active (same as load_date)
    load_date as effective_from,
    
    -- effective_to: when the next version became active (or far future if current)
    coalesce(
        lead(load_date) over (
            partition by hk_order_h
            order by load_date
        ),
        '9999-12-31'::timestamp_ntz
    ) as effective_to,
    
    -- is_current: true if this is the latest version for this order
    lead(load_date) over (
        partition by hk_order_h
        order by load_date
    ) is null as is_current

from {{ ref('sat_sap_order_header_vbak') }}

order by hk_order_h, load_date

