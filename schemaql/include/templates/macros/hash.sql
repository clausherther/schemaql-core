{%- macro row_key(columns) -%}
    concat(
    {% for col in columns -%}
        coalesce(cast({{ col }} as {{ type_string() }}), '')
        {%- if not loop.last %},'|',{% endif -%}
    {% endfor -%}
    )
{%- endmacro -%}
{%- macro hash(columns) -%}
   {{ connector_macro('hash', columns) }}
{%- endmacro %}

{% macro default__hash(columns) -%}
    md5({{ row_key(columns) }})
{%- endmacro %}

{% macro bigquery__hash(columns) -%}
    to_hex({{default__hash(columns)}})
{%- endmacro %}
