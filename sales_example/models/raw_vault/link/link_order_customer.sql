-- models/raw_vault/link/link_order_customer.sql
{{ config(
    materialized = 'incremental',
    alias = 'link_order_customer',
    unique_key = 'hk_link_order_customer_l',
    incremental_strategy = 'merge',
    tags = ['raw_vault', 'link', 'sales', 'critical'],
    post_hook = [
        "{{ log('LINK_ORDER_CUSTOMER – order-customer relationships loaded', info=True) }}"
    ]
) }}

with source_relationships as (
    select distinct
        -- Pre-computed hub keys from staging
        v.hk_order_h,
        c.hk_customer_h,

        -- Composite link hash key from business keys (Data Vault 2.0 best practice)
        {{ generate_hash_key(['v.order_bk', 'c.customer_bk']) }}
            as hk_link_order_customer_l,

        -- Metadata
        v.record_source,
        v.load_date
    from {{ ref('stg_sap__vbak') }} v
    join {{ ref('stg_sap__kna1') }} c
      on v.customer_bk = c.customer_bk
    where v.order_bk is not null
      and c.customer_bk is not null
),

new_links as (
    select
        sr.hk_link_order_customer_l,
        sr.hk_order_h,
        sr.hk_customer_h,
        sr.record_source,
        sr.load_date
    from source_relationships sr
    {% if is_incremental() %}
    where not exists (
        select 1
        from {{ this }} t
        where t.hk_link_order_customer_l = sr.hk_link_order_customer_l
    )
    {% endif %}
),

final as (
    select
        hk_link_order_customer_l,
        hk_order_h,
        hk_customer_h,
        record_source,
        load_date
    from new_links
)

select * from final
