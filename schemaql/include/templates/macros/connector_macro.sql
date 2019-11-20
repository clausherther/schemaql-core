{% macro connector_macro_dbt(name) -%}
    {% set original_name = name %}
    {% if '.' in name %}
        {% set package_name, name = name.split(".", 1) %}
    {% else %}
        {% set package_name = none %}
    {% endif %}

    {% if package_name is none %}
        {% set package_context = context() %}
    {% elif package_name in context %}
        {% set package_context = context()[package_name] %}
    {% else %}
        {% set error_msg %}
            In connector_macro: could not find package '{{ package_name }}', called with '{{ original_name }}'
        {% endset %}
    {% endif %}

    {%- set separator = '__' -%}
    {%- set search_name = connector.connector_type + separator + name -%}
    {%- set default_name = 'default' + separator + name -%}
    {%- if search_name in package_context -%}
        {{ package_context[search_name](*varargs, **kwargs) }}
    {%- else -%}
        {{ package_context[default_name](*varargs, **kwargs) }}
    {%- endif -%}
{%- endmacro %}