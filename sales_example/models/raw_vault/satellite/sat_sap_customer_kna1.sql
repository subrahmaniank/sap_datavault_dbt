-- models/raw_vault/sat/sat_sap_customer_kna1.sql
{{ config(
    materialized = 'incremental',
    alias = 'sat_sap_customer_kna1',
    unique_key = ['hk_customer_h', 'load_date'],
    incremental_strategy = 'merge',
    tags = ['raw_vault', 'satellite', 'customer', 'slowly_changing'],
    post_hook = [
        "{{ log('SAT_SAP_CUSTOMER_KNA1 – customer versions loaded', info=True) }}"
    ]
) }}

with staged as (
    select
        -- Driving key
        hk_customer_h,

        -- Descriptive attributes (exactly as cleaned in staging)
        customer_name,
        city,
        country_code,
        region_code,
        postal_code,
        street_address,
        phone_number,
        customer_account_group,

        -- Hashdiff: detects any change in the payload
        {{ generate_hash_diff([
            'customer_name',
            'city',
            'country_code',
            'region_code',
            'postal_code',
            'street_address',
            'phone_number',
            'customer_account_group'
        ]) }} as hashdiff,

        -- Metadata
        record_source,
        load_date
    from {{ ref('stg_sap__kna1') }}
    where hk_customer_h is not null
),

-- Pure Data Vault 2.0 insert-only pattern: only insert new records when hashdiff changes
new_versions as (
    select
        s.hk_customer_h,
        s.customer_name,
        s.city,
        s.country_code,
        s.region_code,
        s.postal_code,
        s.street_address,
        s.phone_number,
        s.customer_account_group,
        s.hashdiff,
        s.record_source,
        s.load_date
    from staged s
    {% if is_incremental() %}
    -- Only insert if this is a new parent key OR hashdiff has changed
    left join (
        select 
            hk_customer_h,
            hashdiff
        from {{ this }}
        qualify row_number() over (
            partition by hk_customer_h
            order by load_date desc
        ) = 1
    ) latest
      on s.hk_customer_h = latest.hk_customer_h
    where latest.hk_customer_h is null  -- New customer
       or s.hashdiff != latest.hashdiff  -- Changed attributes
    {% endif %}
)

select
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
    load_date
from new_versions
order by hk_customer_h, load_date
