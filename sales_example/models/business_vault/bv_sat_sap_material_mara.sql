-- models/business_vault/bv_sat_sap_material_mara.sql
-- Business Vault view that adds effective dates to Raw Vault satellite
-- This computes effective_from, effective_to, and is_current from load_date
{{ config(
    materialized = 'view',
    alias = 'bv_sat_sap_material_mara',
    tags = ['business_vault', 'satellite', 'material', 'effective_dates']
) }}

select
    -- All columns from Raw Vault satellite
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
    
    -- Computed effective dates (Business Vault logic)
    -- effective_from: when this version became active (same as load_date)
    load_date as effective_from,
    
    -- effective_to: when the next version became active (or far future if current)
    coalesce(
        lead(load_date) over (
            partition by hk_material_h
            order by load_date
        ),
        '9999-12-31'::timestamp_ntz
    ) as effective_to,
    
    -- is_current: true if this is the latest version for this material
    lead(load_date) over (
        partition by hk_material_h
        order by load_date
    ) is null as is_current

from {{ ref('sat_sap_material_mara') }}

order by hk_material_h, load_date

