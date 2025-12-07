{% macro clean_staging_fields() %}
    , nullif(trim({{ adapter.quote("RECORD_SOURCE") }}),'') as record_source
    , {{ get_load_date() }} as load_date
    , try_to_date({{ adapter.quote("ERDAT") }}) as effective_date
{% endmacro %}
