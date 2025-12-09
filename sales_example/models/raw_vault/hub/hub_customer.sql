-- models/raw_vault/hub/hub_customer.sql
{{ config(
    materialized = 'incremental',
    alias = 'hub_customer',
    unique_key = 'hk_customer_h',
    incremental_strategy = 'merge',
    tags = ['raw_vault', 'hub', 'customer'],
    post_hook = [
        "{{ log('HUB_CUSTOMER loaded', info=True) }}"
    ]
) }}

with source_data as (
    select
        hk_customer_h,
        customer_bk,
        record_source,
        load_date
    from {{ ref('stg_sap__kna1') }}
    where customer_bk is not null
),

-- Ensure we only insert new business keys (true Data Vault insert-only)
new_records as (
    select
        sd.hk_customer_h,
        sd.customer_bk,
        sd.record_source,
        sd.load_date
    from source_data sd
    {% if is_incremental() %}
    left join {{ this }} t
      on sd.hk_customer_h = t.hk_customer_h
    where t.hk_customer_h is null
    {% else %}
    -- Initial load: deduplicate by hash key, keep earliest load_date
    qualify row_number() over (
        partition by hk_customer_h
        order by load_date asc
    ) = 1
    {% endif %}
),

-- Final selection
final as (
    select
        hk_customer_h,
        customer_bk,
        record_source,
        load_date
    from new_records
)

select * from final
