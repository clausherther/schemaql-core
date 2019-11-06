{% macro safe_cast(field, type) %}
  {{ connector_macro('safe_cast', field, type) }}
{% endmacro %}


{% macro default__safe_cast(field, type) %}
    cast({{field}} as {{type}})
{% endmacro %}


{% macro snowflake__safe_cast(field, type) %}
    try_cast({{field}} as {{type}})
{% endmacro %}


{% macro bigquery__safe_cast(field, type) %}
    safe_cast({{field}} as {{type}})
{% endmacro %}
