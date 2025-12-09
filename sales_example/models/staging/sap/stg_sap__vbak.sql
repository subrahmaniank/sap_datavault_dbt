-- models/staging/sap/stg_sap__vbak.sql
{{ config(
    materialized = 'view',
    alias = 'stg_sap__vbak',
    tags = ['sap', 'sales_order', 'staging'],
    pre_hook = "{{ dbt_utils.log_info('Staging VBAK started') }}"
) }}

select
    -- Business Keys -------------------------------------------------
    lpad(trim(vbeln), 10, '0')                              as order_bk,

    -- Hashed Hub Key (used directly in hub_order)
    {{ generate_hash_key(['lpad(trim(vbeln), 10, \'0\')']) }} as hk_order_h,

    -- Customer link (for link_order_customer)
    lpad(trim(kunnr), 10, '0')                              as customer_bk,

    -- Core header fields --------------------------------------------
    erdat::date                                             as order_date,
    audat::date                                             as document_date,
    netwr::decimal(18,4)                                    as net_value_header,
    waerk::varchar(5)                                       as currency_code,
    vkorg::varchar(4)                                       as sales_organization,
    vtweg::varchar(2)                                       as distribution_channel,
    spart::varchar(2)                                       as division,
    vkbur::varchar(4)                                       as sales_office,
    vkgrp::varchar(3)                                       as sales_group,
    auart::varchar(4)                                       as order_type,

    -- Derived / cleaned fields --------------------------------------
    case
        when auart in ('TA', 'OR') then 'Standard Order'
        when auart = 'ZRE'         then 'Return Order'
        when auart = 'ZCR'         then 'Credit Memo Request'
        else 'Other'
    end                                                     as order_type_description,

    -- Metadata ------------------------------------------------------
    record_source                                           as record_source,
    {{ get_load_date() }}                                   as load_date

from {{ source('seeds', 'seed_sap_vbak') }}

where vbeln is not null
  and trim(vbeln) != ''
  and netwr >= 0

qualify row_number() over (partition by vbeln order by load_date desc) = 1
