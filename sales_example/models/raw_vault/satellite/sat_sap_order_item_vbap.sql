-- models/raw_vault/sat/sat_sap_order_item_vbap.sql
{{ config(
    materialized = 'incremental',
    alias = 'sat_sap_order_item_vbap',
    unique_key = ['hk_order_item_h', 'load_date'],
    incremental_strategy = 'merge',
    tags = ['raw_vault', 'satellite', 'order_item', 'high_volume'],
    post_hook = [
        "{{ log('SAT_SAP_ORDER_ITEM_VBAP – item versions loaded', info=True) }}"
    ]
) }}

with staged as (
    select
        -- Driving key (pre-computed in staging)
        hk_order_item_h,

        -- All line-item descriptive attributes
        quantity,
        unit_of_measure,
        net_value_item,
        currency_code,
        plant,
        storage_location,
        unit_price,
        item_status,

        -- Hashdiff – detects ANY change in the line item
        {{ generate_hash_diff([
            'quantity',
            'unit_of_measure',
            'net_value_item',
            'currency_code',
            'plant',
            'storage_location',
            'unit_price',
            'item_status'
        ]) }} as hashdiff,

        -- Metadata
        record_source,
        load_date,
        load_date as effective_from,
        '9999-12-31'::timestamp_ntz as effective_to,
        true as is_current

    from {{ ref('stg_sap__vbap') }}
    where hk_order_item_h is not null
),

new_versions as (
    select s.*
    from staged s
    {% if is_incremental() %}
    left join {{ this }} t
      on s.hk_order_item_h = t.hk_order_item_h
     and t.is_current = true
    where t.hk_order_item_h is null
       or s.hashdiff != t.hashdiff
    {% endif %}
),

final as (
    -- Insert new versions
    select
        nv.hk_order_item_h,
        nv.quantity,
        nv.unit_of_measure,
        nv.net_value_item,
        nv.currency_code,
        nv.plant,
        nv.storage_location,
        nv.unit_price,
        nv.item_status,
        nv.hashdiff,
        nv.record_source,
        nv.load_date,
        nv.effective_from,
        nv.effective_to,
        nv.is_current
    from new_versions nv
    {% if is_incremental() %}
    union all

    -- Close previous current version when a newer one arrives
    select
        t.hk_order_item_h,
        t.quantity,
        t.unit_of_measure,
        t.net_value_item,
        t.currency_code,
        t.plant,
        t.storage_location,
        t.unit_price,
        t.item_status,
        t.hashdiff,
        t.record_source,
        t.load_date,
        t.effective_from,
        nv.load_date as effective_to,
        false as is_current
    from {{ this }} t
    join new_versions nv
      on t.hk_order_item_h = nv.hk_order_item_h
    where t.is_current = true
    {% endif %}
)

select
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
    effective_from,
    effective_to,
    is_current
from final
order by hk_order_item_h, load_date
