-- models/staging/sap/stg_sap__vbap.sql
{{ config(
    materialized = 'view',
    alias = 'stg_sap__vbap',
    tags = ['sap', 'sales_order_item', 'staging'],
    pre_hook = "{{ dbt_utils.log_info('Staging VBAP started') }}"
) }}

select
    -- Business Keys -------------------------------------------------
    lpad(trim(vbeln), 10, '0')                              as order_bk,
    lpad(trim(posnr), 6, '0')                               as item_number_bk,

    -- Composite business key for hub_order_item
    lpad(trim(vbeln), 10, '0') || '_' || lpad(trim(posnr), 6, '0')
                                                            as order_item_bk,

    -- Hashed Hub Key (used directly in hub_order_item)
    {{ generate_hash_key([
        'lpad(trim(vbeln), 10, \'0\')',
        'lpad(trim(posnr), 6, \'0\')'
    ]) }}                                                   as hk_order_item_h,

    -- Foreign hashed keys (ready for links)
    {{ generate_hash_key(['lpad(trim(vbeln), 10, \'0\')']) }}
                                                            as hk_order_h,
    {{ generate_hash_key(['lpad(trim(matnr), 18, \'0\')']) }}
                                                            as hk_material_h,

    -- Material business key (preserves leading zeros)
    lpad(trim(matnr), 18, '0')                              as material_bk,

    -- Core item fields ---------------------------------------------
    kwmeng::decimal(18,3)                                   as quantity,
    vrkme::varchar(3)                                       as unit_of_measure,
    netwr::decimal(18,4)                                    as net_value_item,
    waerk::varchar(5)                                       as currency_code,
    werks::varchar(4)                                       as plant,
    lgort::varchar(4)                                       as storage_location,

    -- Calculated fields for immediate analytics
    round(netwr::decimal(18,4) / nullif(kwmeng, 0), 4)       as unit_price,
    case
        when kwmeng > 0 then 'Delivered'
        when kwmeng = 0 then 'Cancelled'
        else 'Open'
    end                                                     as item_status,

    -- Metadata ------------------------------------------------------
    record_source                                           as record_source,
    {{ get_load_date() }}                                   as load_date,

    -- For effectivity satellites and PIT tables
    {{ get_load_date() }}                                   as effective_from,
    '9999-12-31'::timestamp_ntz                             as effective_to,
    true                                                    as is_current

from {{ source('seeds', 'seed_sap_vbap') }}

where vbeln is not null
  and trim(vbeln) != ''
  and posnr is not null
  and trim(posnr) != ''
  and matnr is not null
  and kwmeng >= 0
  and netwr >= 0

-- Deduplicate just in case the same item appears twice in a future load
qualify row_number() over (
    partition by vbeln, posnr
    order by load_date desc
) = 1
