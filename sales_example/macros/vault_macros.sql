{#
===============================================================================
    DATA VAULT 2.0 MACROS
===============================================================================
    These macros generate Hub, Link, and Satellite models following
    Data Vault 2.0 standards.

    Author: Auto-generated
    Date: 2025
    Version: 2.0
===============================================================================
#}

{#
    Macro: hub
    Description: Generates a Hub model (stores unique business keys)

    Parameters:
        - hub_name: Name of the hub (e.g., 'customer', 'order')
        - hash_key_column: Name of the hash key column in source (e.g., 'hk_customer_h')
        - business_key_columns: List of business key column names
        - source_model: Name of the source staging model

    Example:
        {{ hub(
            hub_name='customer',
            hash_key_column='hk_customer_h',
            business_key_columns=['customer_bk'],
            source_model='stg_sap__kna1'
        ) }}

    Notes:
        - Hash key should be pre-computed in staging layer
        - Only unique hash keys are stored (natural deduplication)
        - Incremental loads only insert new hash keys
#}
{% macro hub(hub_name, hash_key_column, business_key_columns, source_model) %}
{{ config(
    materialized='incremental',
    unique_key=hash_key_column,
    on_schema_change='fail'
) }}

with source_data as (
    select
        {{ hash_key_column }},
        {{ business_key_columns | join(',\n        ') }},
        record_source,
        load_date
    from {{ ref(source_model) }}
    where {{ hash_key_column }} is not null
        {% for bk in business_key_columns %}
        and {{ bk }} is not null
        {% endfor %}
),

{% if is_incremental() %}
new_records as (
    select s.*
    from source_data s
    left join {{ this }} t
        on s.{{ hash_key_column }} = t.{{ hash_key_column }}
    where t.{{ hash_key_column }} is null
)

select * from new_records
{% else %}
-- Initial load: deduplicate by hash key, keep earliest load_date
deduplicated as (
    select *
    from source_data
    qualify row_number() over (
        partition by {{ hash_key_column }}
        order by load_date asc
    ) = 1
)

select * from deduplicated
{% endif %}

{% endmacro %}


{#
    Macro: link
    Description: Generates a Link model (stores relationships between hubs)

    Parameters:
        - link_name: Name of the link (e.g., 'order_customer')
        - hash_key_column: Name of the link hash key column (e.g., 'hk_order_customer_l')
        - foreign_hash_keys: List of foreign hash key column names from hubs
        - source_model: Name of the source staging model

    Example:
        {{ link(
            link_name='order_customer',
            hash_key_column='hk_order_customer_l',
            foreign_hash_keys=['hk_order_h', 'hk_customer_h'],
            source_model='stg_sap__vbak'
        ) }}

    Notes:
        - Link hash key should be pre-computed from all foreign keys
        - All foreign keys must be not null
        - Incremental loads only insert new link combinations
#}
{% macro link(link_name, hash_key_column, foreign_hash_keys, source_model) %}
{{ config(
    materialized='incremental',
    unique_key=hash_key_column,
    on_schema_change='fail'
) }}

with source_data as (
    select
        {{ hash_key_column }},
        {{ foreign_hash_keys | join(',\n        ') }},
        record_source,
        load_date
    from {{ ref(source_model) }}
    where {{ hash_key_column }} is not null
        {% for fk in foreign_hash_keys %}
        and {{ fk }} is not null
        {% endfor %}
),

{% if is_incremental() %}
new_records as (
    select s.*
    from source_data s
    left join {{ this }} t
        on s.{{ hash_key_column }} = t.{{ hash_key_column }}
    where t.{{ hash_key_column }} is null
)

select * from new_records
{% else %}
-- Initial load: deduplicate by link hash key, keep earliest load_date
deduplicated as (
    select *
    from source_data
    qualify row_number() over (
        partition by {{ hash_key_column }}
        order by load_date asc
    ) = 1
)

select * from deduplicated
{% endif %}

{% endmacro %}


{#
    Macro: sat
    Description: Generates a Satellite model (stores descriptive attributes with history)

    Parameters:
        - sat_name: Name of the satellite (e.g., 'customer')
        - parent_hash_key: Name of parent hub/link hash key column
        - hashdiff_column: Name of the hashdiff column (will be generated)
        - source_model: Name of the source staging model
        - attribute_columns: List of descriptive attribute columns

    Example:
        {{ sat(
            sat_name='customer',
            parent_hash_key='hk_customer_h',
            hashdiff_column='hashdiff',
            source_model='stg_sap__kna1',
            attribute_columns=['customer_name', 'city', 'country_code']
        ) }}

    Notes:
        - Hashdiff is computed from all attribute columns
        - Only records with changed hashdiff are inserted
        - Supports Type 2 SCD (Slowly Changing Dimension)
        - Each parent key can have multiple rows with different hashdiffs
#}
{% macro sat(sat_name, parent_hash_key, hashdiff_column, source_model, attribute_columns) %}
{{ config(
    materialized='incremental',
    unique_key=[parent_hash_key, 'load_date'],
    on_schema_change='append_new_columns'
) }}

with source_data as (
    select
        {{ parent_hash_key }},
        {{ attribute_columns | join(',\n        ') }},
        {{ generate_hash_diff(attribute_columns) }} as {{ hashdiff_column }},
        record_source,
        load_date
    from {{ ref(source_model) }}
    where {{ parent_hash_key }} is not null
),

{% if is_incremental() %}
-- Get the latest record for each parent key from the existing satellite
latest_records as (
    select *
    from {{ this }}
    qualify row_number() over (
        partition by {{ parent_hash_key }}
        order by load_date desc
    ) = 1
),

-- Identify records with changes (new parent key or different hashdiff)
records_to_insert as (
    select
        s.{{ parent_hash_key }},
        s.{{ attribute_columns | join(',\n        s.') }},
        s.{{ hashdiff_column }},
        s.record_source,
        s.load_date
    from source_data s
    left join latest_records l
        on s.{{ parent_hash_key }} = l.{{ parent_hash_key }}
    where l.{{ parent_hash_key }} is null  -- New parent key
       or s.{{ hashdiff_column }} != l.{{ hashdiff_column }}  -- Changed attributes
)

select * from records_to_insert
{% else %}
-- Initial load: deduplicate by parent key and hashdiff, keep earliest load_date
deduplicated as (
    select *
    from source_data
    qualify row_number() over (
        partition by {{ parent_hash_key }}, {{ hashdiff_column }}
        order by load_date asc
    ) = 1
)

select * from deduplicated
{% endif %}

{% endmacro %}


{#
    Macro: pit (Point-in-Time) table
    Description: Generates a Point-in-Time table for efficient querying of satellite history

    Parameters:
        - pit_name: Name of the PIT table
        - hub_name: Name of the parent hub
        - hub_hash_key: Hash key of the hub
        - satellites: List of dictionaries with satellite info
            - name: satellite name
            - hash_key: satellite hash key column

    Example:
        {{ pit(
            pit_name='customer_pit',
            hub_name='hub_customer',
            hub_hash_key='hk_customer_h',
            satellites=[
                {'name': 'sat_customer', 'hash_key': 'hk_customer_h'},
                {'name': 'sat_customer_address', 'hash_key': 'hk_customer_h'}
            ]
        ) }}

    Notes:
        - Creates a snapshot for each point in time where data changed
        - Enables efficient temporal queries without window functions
        - Materializes as table for query performance
#}
{% macro pit(pit_name, hub_name, hub_hash_key, satellites) %}
{{ config(
    materialized='table'
) }}

with hub as (
    select
        {{ hub_hash_key }},
        load_date as hub_load_date
    from {{ ref(hub_name) }}
),

{% for sat in satellites %}
{{ sat.name }}_with_validity as (
    select
        {{ sat.hash_key }},
        load_date,
        lead(load_date) over (
            partition by {{ sat.hash_key }}
            order by load_date
        ) as next_load_date
    from {{ ref(sat.name) }}
),
{% endfor %}

-- Create a spine of all distinct load dates across all tables
spine as (
    select distinct load_date as snapshot_date
    from (
        select hub_load_date as load_date from hub
        {% for sat in satellites %}
        union all
        select load_date from {{ sat.name }}_with_validity
        {% endfor %}
    )
),

-- Join spine with hub and all satellites using point-in-time logic
final as (
    select
        s.snapshot_date,
        h.{{ hub_hash_key }}
        {% for sat in satellites %}
        , {{ sat.name }}.load_date as {{ sat.name }}_load_date
        {% endfor %}
    from spine s
    cross join hub h
    {% for sat in satellites %}
    left join {{ sat.name }}_with_validity as {{ sat.name }}
        on h.{{ hub_hash_key }} = {{ sat.name }}.{{ sat.hash_key }}
        and s.snapshot_date >= {{ sat.name }}.load_date
        and s.snapshot_date < coalesce({{ sat.name }}.next_load_date, '9999-12-31'::timestamp_ntz)
    {% endfor %}
    where s.snapshot_date >= h.hub_load_date
)

select * from final
order by snapshot_date, {{ hub_hash_key }}
{% endmacro %}
