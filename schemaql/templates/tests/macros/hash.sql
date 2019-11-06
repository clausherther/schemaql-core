{% macro hash(columns) -%}
   {{ connector_macro('hash', columns) }}
{%- endmacro %}

{% macro default__hash(columns) -%}
    md5(cast({{columns}} as {{ type_string() }} ))
{%- endmacro %}

{% macro bigquery__hash(columns) -%}
    to_hex({{default__hash(columns)}})
{%- endmacro %}
