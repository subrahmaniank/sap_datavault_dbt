-- models/raw_vault/hub/hub_material.sql
{{ config(
    materialized = 'incremental',
    alias = 'hub_material',
    unique_key = 'hk_material_h',
    incremental_strategy = 'merge',
    tags = ['raw_vault', 'hub', 'material', 'product'],
    post_hook = [
        "{{ log('HUB_MATERIAL – loaded', info=True) }}"
    ]
) }}

with source_data as (
    select distinct
        hk_material_h,
        material_bk,
        record_source,
        load_date
    from {{ ref('stg_sap__mara') }}
    where material_bk is not null
      and trim(material_bk) != ''
),

-- Pure insert-only: only new materials ever get added
new_records as (
    select
        sd.hk_material_h,
        sd.material_bk,
        sd.record_source,
        sd.load_date
    from source_data sd
    {% if is_incremental() %}
    where not exists (
        select 1
        from {{ this }} t
        where t.hk_material_h = sd.hk_material_h
    )
    {% endif %}
),

final as (
    select
        hk_material_h,
        material_bk,
        record_source,
        load_date
    from new_records
)

select * from final
