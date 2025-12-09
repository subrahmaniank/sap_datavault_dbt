-- models/raw_vault/sat/sat_sap_order_header_vbak.sql
{{ config(
    materialized = 'incremental',
    alias = 'sat_sap_order_header_vbak',
    unique_key = ['hk_order_h', 'load_date'],
    incremental_strategy = 'merge',
    tags = ['raw_vault', 'satellite', 'order_header', 'slowly_changing'],
    post_hook = [
        "{{ log('SAT_SAP_ORDER_HEADER_VBAK – order header versions loaded', info=True) }}"
    ]
) }}

with staged as (
    select
        -- Driving key from staging
        hk_order_h,

        -- All descriptive attributes from VBAK (cleaned in staging)
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

        -- Hashdiff – detects ANY change in the header payload
        {{ generate_hash_diff([
            'order_date',
            'document_date',
            'net_value_header',
            'currency_code',
            'sales_organization',
            'distribution_channel',
            'division',
            'sales_office',
            'sales_group',
            'order_type',
            'order_type_description'
        ]) }} as hashdiff,

        -- Metadata
        record_source,
        load_date
    from {{ ref('stg_sap__vbak') }}
    where hk_order_h is not null
),

-- Pure Data Vault 2.0 insert-only pattern: only insert new records when hashdiff changes
new_versions as (
    select
        s.hk_order_h,
        s.order_date,
        s.document_date,
        s.net_value_header,
        s.currency_code,
        s.sales_organization,
        s.distribution_channel,
        s.division,
        s.sales_office,
        s.sales_group,
        s.order_type,
        s.order_type_description,
        s.hashdiff,
        s.record_source,
        s.load_date
    from staged s
    {% if is_incremental() %}
    -- Only insert if this is a new parent key OR hashdiff has changed
    left join (
        select 
            hk_order_h,
            hashdiff
        from {{ this }}
        qualify row_number() over (
            partition by hk_order_h
            order by load_date desc
        ) = 1
    ) latest
      on s.hk_order_h = latest.hk_order_h
    where latest.hk_order_h is null  -- New order
       or s.hashdiff != latest.hashdiff  -- Changed attributes
    {% endif %}
)

select
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
    load_date
from new_versions
order by hk_order_h, load_date
