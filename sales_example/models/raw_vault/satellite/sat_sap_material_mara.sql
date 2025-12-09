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
        load_date

    from {{ ref('stg_sap__mara') }}
    where hk_material_h is not null
),

-- Pure Data Vault 2.0 insert-only pattern: only insert new records when hashdiff changes
new_versions as (
    select
        s.hk_material_h,
        s.material_group,
        s.material_type,
        s.material_type_description,
        s.base_unit_of_measure,
        s.gross_weight,
        s.net_weight,
        s.volume,
        s.volume_uom,
        s.creation_date,
        s.last_change_date,
        s.maintenance_status,
        s.weight_category,
        s.hashdiff,
        s.record_source,
        s.load_date
    from staged s
    {% if is_incremental() %}
    -- Only insert if this is a new parent key OR hashdiff has changed
    left join (
        select 
            hk_material_h,
            hashdiff
        from {{ this }}
        qualify row_number() over (
            partition by hk_material_h
            order by load_date desc
        ) = 1
    ) latest
      on s.hk_material_h = latest.hk_material_h
    where latest.hk_material_h is null  -- New material
       or s.hashdiff != latest.hashdiff  -- Changed attributes
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
    load_date
from new_versions
order by hk_material_h, load_date
