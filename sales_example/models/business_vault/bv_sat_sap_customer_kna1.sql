-- models/business_vault/bv_sat_sap_customer_kna1.sql
-- Business Vault view that adds effective dates to Raw Vault satellite
-- This computes effective_from, effective_to, and is_current from load_date
{{ config(
    materialized = 'view',
    alias = 'bv_sat_sap_customer_kna1',
    tags = ['business_vault', 'satellite', 'customer', 'effective_dates']
) }}

select
    -- All columns from Raw Vault satellite
    hk_customer_h,
    customer_name,
    city,
    country_code,
    region_code,
    postal_code,
    street_address,
    phone_number,
    customer_account_group,
    hashdiff,
    record_source,
    load_date,
    
    -- Computed effective dates (Business Vault logic)
    -- effective_from: when this version became active (same as load_date)
    load_date as effective_from,
    
    -- effective_to: when the next version became active (or far future if current)
    coalesce(
        lead(load_date) over (
            partition by hk_customer_h
            order by load_date
        ),
        '9999-12-31'::timestamp_ntz
    ) as effective_to,
    
    -- is_current: true if this is the latest version for this customer
    lead(load_date) over (
        partition by hk_customer_h
        order by load_date
    ) is null as is_current

from {{ ref('sat_sap_customer_kna1') }}

order by hk_customer_h, load_date

