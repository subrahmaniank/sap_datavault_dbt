-- models/staging/sap/stg_sap__kna1.sql
{{ config(
    materialized = 'view',
    tags = ['sap', 'customer', 'staging']
) }}

select
    lpad(trim(kunnr), 10, '0')                              as customer_bk,

    {{ generate_hash_key(['lpad(trim(kunnr), 10, \'0\')']) }} as hk_customer_h,

    -- Cleaned descriptive fields
    nullif(trim(upper(name1)), '')                          as customer_name,
    nullif(trim(upper(ort01)), '')                          as city,
    nullif(trim(upper(land1)), '')                          as country_code,
    nullif(trim(upper(regio)), '')                          as region_code,
    nullif(trim(pstlz), '')                                 as postal_code,
    nullif(trim(stras), '')                                 as street_address,
    nullif(trim(telf1), '')                                 as phone_number,
    nullif(trim(ktok), '')                                as customer_account_group,

    -- Load metadata
    record_source                                           as record_source,
    load_date::timestamp_ntz                                as load_date

from {{ source('seeds', 'seed_sap_kna1') }}

where kunnr is not null
  and length(trim(kunnr)) <= 10
