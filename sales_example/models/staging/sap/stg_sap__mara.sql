-- models/staging/sap/stg_sap__mara.sql
{{ config(
    materialized = 'view',
    alias = 'stg_sap__mara',
    tags = ['sap', 'material', 'product', 'staging'],
    pre_hook = "{{ dbt_utils.log_info('Staging MARA started') }}"
) }}

select
    -- Business Key – 18-digit material number with leading zeros (SAP standard)
    lpad(trim(matnr), 18, '0')                              as material_bk,

    -- Hashed Hub Key – used by hub_material and all links
    {{ generate_hash_key(['lpad(trim(matnr), 18, \'0\')']) }}
                                                            as hk_material_h,

    -- Core material master attributes
    trim(upper(matkl))                                      as material_group,
    trim(upper(mtart))                                      as material_type,
    trim(upper(meins))                                      as base_unit_of_measure,
    brgew::decimal(13,3)                                    as gross_weight,
    ntgew::decimal(13,3)                                    as net_weight,
    volum::decimal(13,3)                                    as volume,
    volum                                                   as volume_uom,
    ersda::date                                             as creation_date,
    laeda::date                                             as last_change_date,
    vpsta                                                   as maintenance_status,

    -- Human-readable descriptions (derived)
    case mtart
        when 'FERT' then 'Finished Good'
        when 'HAWA' then 'Trading Good'
        when 'ROH'  then 'Raw Material'
        when 'DIEN' then 'Service'
        when 'VERP' then 'Packaging'
        else upper(mtart)
    end                                                     as material_type_description,

    case
        when brgew > 1000 then 'Heavy'
        when brgew > 100  then 'Medium'
        when brgew > 0    then 'Light'
        else 'Weightless'
    end                                                     as weight_category,

    -- Metadata
    record_source                                           as record_source,
    {{ get_load_date() }}                                   as load_date,

    -- For effectivity satellites / PIT tables
    load_date::timestamp_ntz                                   as effective_from,
    '9999-12-31'::timestamp_ntz                             as effective_to,
    true                                                    as is_current

from {{ source('seeds', 'seed_sap_mara') }}

where matnr is not null
  and trim(matnr) != ''
  and length(trim(matnr)) <= 18

-- Ensure only the latest version of each material (in case of future incremental loads)
qualify row_number() over (
    partition by matnr
    order by load_date desc, laeda desc
) = 1
