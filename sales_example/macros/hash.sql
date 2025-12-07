{#
    Macro: generate_hash_key
    Description: Generates an MD5 hash key from one or more columns/expressions
    Parameters:
        - columns: List of column names or SQL expressions to hash
    Example:
        {{ generate_hash_key(['customer_id']) }}
        {{ generate_hash_key(['order_id', 'line_item']) }}
    Notes:
        - Columns are concatenated with '||' as separator
        - NULL values are handled gracefully
        - Returns consistent hash for same input values
#}
{% macro generate_hash_key(columns) %}
    md5(
        coalesce(
            cast({{ columns | join(" || '||' || ") }} as string),
            'NULL'
        )
    )
{% endmacro %}

{#
    Macro: generate_hash_diff
    Description: Generates an MD5 hashdiff from multiple columns for satellite records
    Parameters:
        - columns: List of column names to include in the diff
    Example:
        {{ generate_hash_diff(['customer_name', 'address', 'phone']) }}
    Notes:
        - Used to detect changes in satellite attributes
        - NULL values are converted to empty strings for consistency
        - Columns are concatenated with '||' as separator
#}
{% macro generate_hash_diff(columns) %}
    md5(
        concat(
            {% for col in columns %}
            coalesce(cast({{ col }} as string), '')
            {% if not loop.last %}, '||', {% endif %}
            {% endfor %}
        )
    )
{% endmacro %}
