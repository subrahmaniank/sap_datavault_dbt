-- models/raw_vault/link/link_order_material.sql
{{ config(
    materialized = 'incremental',
    alias = 'link_order_material',
    unique_key = 'hk_link_order_material_l',
    incremental_strategy = 'merge',
    tags = ['raw_vault', 'link', 'sales', 'critical'],
    post_hook = [
        "{{ log('LINK_ORDER_MATERIAL – item-material relationships loaded', info=True) }}"
    ]
) }}

with source_relationships as (
    select distinct
        -- Pre-computed hub keys from staging
        i.hk_order_item_h,
        m.hk_material_h,

        -- Composite link hash key from business keys (Data Vault 2.0 best practice)
        {{ generate_hash_key(['i.order_item_bk', 'm.material_bk']) }}
            as hk_link_order_material_l,

        -- Metadata
        i.record_source,
        i.load_date
    from {{ ref('stg_sap__vbap') }} i
    join {{ ref('stg_sap__mara') }} m
      on i.material_bk = m.material_bk
    where i.order_item_bk is not null
      and m.material_bk is not null
),

new_links as (
    select
        sr.hk_link_order_material_l,
        sr.hk_order_item_h,
        sr.hk_material_h,
        sr.record_source,
        sr.load_date
    from source_relationships sr
    {% if is_incremental() %}
    where not exists (
        select 1
        from {{ this }} t
        where t.hk_link_order_material_l = sr.hk_link_order_material_l
    )
    {% endif %}
),

final as (
    select
        hk_link_order_material_l,
        hk_order_item_h,
        hk_material_h,
        record_source,
        load_date
    from new_links
)

select * from final
