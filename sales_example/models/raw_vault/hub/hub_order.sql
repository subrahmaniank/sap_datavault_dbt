-- models/raw_vault/hub/hub_order.sql
{{ config(
    materialized = 'incremental',
    alias = 'hub_order',
    unique_key = 'hk_order_h',
    incremental_strategy = 'merge',
    tags = ['raw_vault', 'hub', 'sales_order'],
    post_hook = [
        "{{ log('HUB_ORDER inserted', info=True) }}"
    ]
) }}

with source_data as (
    select distinct
        hk_order_h,
        order_bk,
        record_source,
        load_date
    from {{ ref('stg_sap__vbak') }}
    where order_bk is not null
),

-- Insert-only: only brand-new orders (by hash key)
new_records as (
    select
        sd.hk_order_h,
        sd.order_bk,
        sd.record_source,
        sd.load_date
    from source_data sd
    {% if is_incremental() %}
    where not exists (
        select 1
        from {{ this }} t
        where t.hk_order_h = sd.hk_order_h
    )
    {% else %}
    -- Initial load: deduplicate by hash key, keep earliest load_date
    qualify row_number() over (
        partition by hk_order_h
        order by load_date asc
    ) = 1
    {% endif %}
),

final as (
    select
        hk_order_h,
        order_bk,
        record_source,
        load_date
    from new_records
)

select * from final
