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
        load_date,
        load_date as effective_from,
        '9999-12-31'::timestamp_ntz as effective_to,
        true as is_current
    from {{ ref('stg_sap__vbak') }}
    where hk_order_h is not null
),

-- Detect real changes (new orders or actual updates)
new_versions as (
    select
        s.*
    from staged s
    {% if is_incremental() %}
    left join {{ this }} t
      on s.hk_order_h = t.hk_order_h
     and t.is_current = true
    where t.hk_order_h is null
       or s.hashdiff != t.hashdiff
    {% endif %}
),

final as (
    -- 1. Insert the new version(s)
    select
        nv.hk_order_h,
        nv.order_date,
        nv.document_date,
        nv.net_value_header,
        nv.currency_code,
        nv.sales_organization,
        nv.distribution_channel,
        nv.division,
        nv.sales_office,
        nv.sales_group,
        nv.order_type,
        nv.order_type_description,
        nv.hashdiff,
        nv.record_source,
        nv.load_date,
        nv.effective_from,
        nv.effective_to,
        nv.is_current
    from new_versions nv
    {% if is_incremental() %}
    union all

    -- 2. Close the previous current version when a new one appears
    select
        t.hk_order_h,
        t.order_date,
        t.document_date,
        t.net_value_header,
        t.currency_code,
        t.sales_organization,
        t.distribution_channel,
        t.division,
        t.sales_office,
        t.sales_group,
        t.order_type,
        t.order_type_description,
        t.hashdiff,
        t.record_source,
        t.load_date,
        t.effective_from,
        nv.load_date as effective_to,
        false as is_current
    from {{ this }} t
    join new_versions nv
      on t.hk_order_h = nv.hk_order_h
    where t.is_current = true
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
    load_date,
    effective_from,
    effective_to,
    is_current
from final
order by hk_order_h, load_date
