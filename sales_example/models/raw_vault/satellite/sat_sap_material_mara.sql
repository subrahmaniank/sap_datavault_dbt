-- models/raw_vault/sat/sat_sap_material_mara.sql
{{ config(
    materialized = 'incremental',
    alias = 'sat_sap_material_mara',
    unique_key = ['hk_material_h', 'load_date'],
    incremental_strategy = 'merge',
    tags = ['raw_vault', 'satellite', 'material', 'master_data'],
    post_hook = [
        "{{ log('SAT_SAP_MATERIAL_MARA – material versions loaded', info=True) }}"
    ]
) }}

with staged as (
    select
        -- Driving key
        hk_material_h,

        -- All material master attributes (cleaned in staging)
        material_group,
        material_type,
        material_type_description,
        base_unit_of_measure,
        gross_weight,
        net_weight,
        volume,
        volume_uom,
        creation_date,
        last_change_date,
        maintenance_status,
        weight_category,

        -- Hashdiff – detects any change in material master
        {{ generate_hash_diff([
            'material_group',
            'material_type',
            'material_type_description',
            'base_unit_of_measure',
            'gross_weight',
            'net_weight',
            'volume',
            'volume_uom',
            'creation_date',
            'last_change_date',
            'maintenance_status',
            'weight_category'
        ]) }} as hashdiff,

        -- Metadata
        record_source,
        load_date,
        load_date as effective_from,
        '9999-12-31'::timestamp_ntz as effective_to,
        true as is_current

    from {{ ref('stg_sap__mara') }}
    where hk_material_h is not null
),

new_versions as (
    select s.*
    from staged s
    {% if is_incremental() %}
    left join {{ this }} t
      on s.hk_material_h = t.hk_material_h
     and t.is_current = true
    where t.hk_material_h is null
       or s.hashdiff != t.hashdiff
    {% endif %}
),

final as (
    -- 1. Insert brand-new or changed material versions
    select
        nv.hk_material_h,
        nv.material_group,
        nv.material_type,
        nv.material_type_description,
        nv.base_unit_of_measure,
        nv.gross_weight,
        nv.net_weight,
        nv.volume,
        nv.volume_uom,
        nv.creation_date,
        nv.last_change_date,
        nv.maintenance_status,
        nv.weight_category,
        nv.hashdiff,
        nv.record_source,
        nv.load_date,
        nv.effective_from,
        nv.effective_to,
        nv.is_current
    from new_versions nv
    {% if is_incremental() %}
    union all

    -- 2. Close previous version when a newer one arrives
    select
        t.hk_material_h,
        t.material_group,
        t.material_type,
        t.material_type_description,
        t.base_unit_of_measure,
        t.gross_weight,
        t.net_weight,
        t.volume,
        t.volume_uom,
        t.creation_date,
        t.last_change_date,
        t.maintenance_status,
        t.weight_category,
        t.hashdiff,
        t.record_source,
        t.load_date,
        t.effective_from,
        nv.load_date as effective_to,
        false as is_current
    from {{ this }} t
    join new_versions nv
      on t.hk_material_h = nv.hk_material_h
    where t.is_current = true
    {% endif %}
)

select
    hk_material_h,
    material_group,
    material_type,
    material_type_description,
    base_unit_of_measure,
    gross_weight,
    net_weight,
    volume,
    volume_uom,
    creation_date,
    last_change_date,
    maintenance_status,
    weight_category,
    hashdiff,
    record_source,
    load_date,
    effective_from,
    effective_to,
    is_current
from final
order by hk_material_h, load_date
