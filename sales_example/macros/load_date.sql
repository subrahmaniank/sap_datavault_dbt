{% macro get_load_date() %}
    current_timestamp()::timestamp_ntz
{% endmacro %}
