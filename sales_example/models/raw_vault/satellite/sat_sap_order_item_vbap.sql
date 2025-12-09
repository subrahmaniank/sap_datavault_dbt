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
        load_date

    from {{ ref('stg_sap__vbap') }}
    where hk_order_item_h is not null
),

-- Pure Data Vault 2.0 insert-only pattern: only insert new records when hashdiff changes
new_versions as (
    select
        s.hk_order_item_h,
        s.quantity,
        s.unit_of_measure,
        s.net_value_item,
        s.currency_code,
        s.plant,
        s.storage_location,
        s.unit_price,
        s.item_status,
        s.hashdiff,
        s.record_source,
        s.load_date
    from staged s
    {% if is_incremental() %}
    -- Only insert if this is a new parent key OR hashdiff has changed
    left join (
        select 
            hk_order_item_h,
            hashdiff
        from {{ this }}
        qualify row_number() over (
            partition by hk_order_item_h
            order by load_date desc
        ) = 1
    ) latest
      on s.hk_order_item_h = latest.hk_order_item_h
    where latest.hk_order_item_h is null  -- New order item
       or s.hashdiff != latest.hashdiff  -- Changed attributes
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
    load_date
from new_versions
order by hk_order_item_h, load_date
