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
        load_date,
        load_date as effective_from,
        '9999-12-31'::timestamp_ntz as effective_to,
        true as is_current
    from {{ ref('stg_sap__kna1') }}
    where hk_customer_h is not null
),

-- Only insert new versions when something actually changed
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
        s.load_date,
        s.effective_from,
        s.effective_to,
        s.is_current
    from staged s
    {% if is_incremental() %}
    left join {{ this }} t
      on s.hk_customer_h = t.hk_customer_h
     and t.is_current = true
    where t.hk_customer_h is null
       or s.hashdiff != t.hashdiff
    {% endif %}
),

-- Close previous version if a new one arrives for the same customer
final as (
    select
        nv.hk_customer_h,
        nv.customer_name,
        nv.city,
        nv.country_code,
        nv.region_code,
        nv.postal_code,
        nv.street_address,
        nv.phone_number,
        nv.customer_account_group,
        nv.hashdiff,
        nv.record_source,
        nv.load_date,
        nv.effective_from,
        nv.effective_to,
        nv.is_current
    from new_versions nv
    {% if is_incremental() %}
    union all
    -- Close old versions when a newer one exists
    select
        t.hk_customer_h,
        t.customer_name,
        t.city,
        t.country_code,
        t.region_code,
        t.postal_code,
        t.street_address,
        t.phone_number,
        t.customer_account_group,
        t.hashdiff,
        t.record_source,
        t.load_date,
        t.effective_from,
        nv.load_date as effective_to,
        false as is_current
    from {{ this }} t
    join new_versions nv
      on t.hk_customer_h = nv.hk_customer_h
    where t.is_current = true
      and nv.load_date > t.load_date
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
    load_date,
    effective_from,
    effective_to,
    is_current
from final
order by hk_customer_h, load_date
