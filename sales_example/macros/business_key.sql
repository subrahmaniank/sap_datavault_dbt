{% macro bk(columns) %}
    {{ return(adapter.dispatch('bk', 'my_vault')(columns)) }}
{% endmacro %}

{% macro default__bk(columns) %}
    trim(upper({{ columns | join('||') }}))
{% endmacro %}
