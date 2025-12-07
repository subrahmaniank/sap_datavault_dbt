-- Test: no duplicate business keys in hubs
{% test unique_business_key(model, column_name) %}
    select {{ column_name }}, count(*)
    from {{ model }}
    group by {{ column_name }}
    having count(*) > 1
{% endtest %}

-- Test: hashdiff changes trigger new satellite row
{% test hashdiff_changes(model) %}
    select hashdiff, count(*) as cnt
    from {{ model }}
    where is_active
    group by hashdiff
    having count(*) > 1
{% endtest %}
