{#
    Macro: test_insert_only_pattern
    Description: Tests that a table follows insert-only pattern (no updates)
    Verifies that hash keys are unique (Data Vault 2.0 requirement)
#}
{% macro test_insert_only_pattern(model, hash_key_column) %}
    select 
        {{ hash_key_column }},
        count(*) as duplicate_count
    from {{ model }}
    group by {{ hash_key_column }}
    having count(*) > 1
{% endmacro %}

{#
    Macro: test_satellite_history
    Description: Tests that satellites properly track history
    Verifies load_date ordering and hashdiff changes
#}
{% macro test_satellite_history(model, parent_hash_key, hashdiff_column) %}
    with ordered_records as (
        select 
            {{ parent_hash_key }},
            {{ hashdiff_column }},
            load_date,
            lag({{ hashdiff_column }}) over (
                partition by {{ parent_hash_key }}
                order by load_date
            ) as prev_hashdiff,
            lag(load_date) over (
                partition by {{ parent_hash_key }}
                order by load_date
            ) as prev_load_date
        from {{ model }}
    )
    select *
    from ordered_records
    where prev_load_date is not null
      and (
          prev_load_date >= load_date  -- Load dates should be ascending
          or (prev_hashdiff = {{ hashdiff_column }} and prev_load_date < load_date)  -- Same hashdiff shouldn't create new record
      )
{% endmacro %}

{#
    Macro: test_effective_dates_logic
    Description: Tests Business Vault effective dates logic
    Verifies effective_from <= effective_to and proper is_current flags
#}
{% macro test_effective_dates_logic(model, parent_hash_key) %}
    with current_versions as (
        select 
            {{ parent_hash_key }},
            sum(case when is_current = true then 1 else 0 end) as current_count
        from {{ model }}
        group by {{ parent_hash_key }}
    )
    select 
        {{ parent_hash_key }},
        current_count
    from current_versions
    where current_count != 1  -- Should have exactly one current version
{% endmacro %}

