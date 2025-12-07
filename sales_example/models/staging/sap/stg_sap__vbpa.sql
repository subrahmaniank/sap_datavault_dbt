-- models/staging/sap/stg_sap__vbpa.sql
{{ config(
    materialized = 'view',
    alias = 'stg_sap__vbpa',
    tags = ['sap', 'partner', 'sales_order', 'staging'],
    pre_hook = "{{ dbt_utils.log_info('Staging VBPA started') }}"
) }}

select
    -- Primary business keys
    lpad(trim(vbeln), 10, '0')                              as order_bk,
    upper(trim(parvw))                                      as partner_function_code,

    -- Secondary business keys (customer or vendor numbers)
    case
        when upper(trim(parvw)) in ('AG','RE','RG','WE','SP','SH')
            then lpad(trim(kunnr), 10, '0')
        else null
    end                                                     as customer_bk,

    case
        when upper(trim(parvw)) in ('LF','VE')
            then lpad(trim(lifnr), 10, '0')
        else null
    end                                                     as vendor_bk,

    -- Pre-computed hashed keys for instant linking
    {{ generate_hash_key(['lpad(trim(vbeln), 10, \'0\')']) }}
                                                            as hk_order_h,

    case when customer_bk is not null
        then {{ generate_hash_key(['customer_bk']) }}
        else null
    end                                                     as hk_customer_h,

    case when vendor_bk is not null
        then {{ generate_hash_key(['vendor_bk']) }}
        else null
    end                                                     as hk_vendor_h,

    -- Human-readable partner role description
    case upper(trim(parvw))
        when 'AG' then 'Sold-to Party'
        when 'RE' then 'Bill-to Party'
        when 'RG' then 'Payer'
        when 'WE' then 'Ship-to Party'
        when 'SP' then 'Sales Employee'
        when 'SH' then 'Ship-to Party (alternative)'
        when 'LF' then 'Vendor'
        when 'VE' then 'Sales Representative'
        else 'Other (' || upper(trim(parvw)) || ')'
    end                                                     as partner_role,

    -- Additional fields (rarely used but kept for completeness)
    pernr::varchar(8)                                       as personnel_number,
    adrnr::varchar(10)                                      as address_number,

    -- Metadata
    record_source                                           as record_source,
    load_date::timestamp_ntz                                as load_date,
    load_date::timestamp_ntz                                as effective_from,
    '9999-12-31'::timestamp_ntz                             as effective_to,
    true                                                    as is_current

from {{ source('seeds', 'seed_sap_vbpa') }}

where vbeln is not null
  and trim(vbeln) != ''
  and parvw is not null
  and trim(parvw) != ''

-- Keep only one row per order + partner function (latest load wins)
qualify row_number() over (
    partition by vbeln, parvw
    order by load_date desc
) = 1
