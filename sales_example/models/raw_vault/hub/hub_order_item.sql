-- models/raw_vault/hub/hub_order_item.sql
{{ config(
    materialized = 'incremental',
    alias = 'hub_order_item',
    unique_key = 'hk_order_item_h',
    incremental_strategy = 'merge',
    tags = ['raw_vault', 'hub', 'sales_order_item', 'critical'],
    post_hook = [
        "{{ log('HUB_ORDER_ITEM loaded', info=True) }}"
    ]
) }}

with source_data as (
    select distinct
        hk_order_item_h,
        order_item_bk,
        record_source,
        load_date
    from {{ ref('stg_sap__vbap') }}
    where order_item_bk is not null
      and trim(order_item_bk) != ''
),

-- True Data Vault insert-only: only brand-new order items
new_records as (
    select
        sd.hk_order_item_h,
        sd.order_item_bk,
        sd.record_source,
        sd.load_date
    from source_data sd
    {% if is_incremental() %}
    left join {{ this }} t
      on sd.hk_order_item_h = t.hk_order_item_h
    where t.hk_order_item_h is null
    {% else %}
    -- Initial load: deduplicate by hash key, keep earliest load_date
    qualify row_number() over (
        partition by hk_order_item_h
        order by load_date asc
    ) = 1
    {% endif %}
),

final as (
    select
        hk_order_item_h,
        order_item_bk,
        record_source,
        load_date
    from new_records
)

select * from final
