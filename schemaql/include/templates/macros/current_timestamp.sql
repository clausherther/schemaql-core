{%- macro current_timestamp() -%}
  {{ connector_macro('current_timestamp') }}
{%- endmacro -%}

{%- macro default__current_timestamp() -%}
    current_timestamp::{{type_timestamp()}}
{%- endmacro -%}

{%- macro bigquery__current_timestamp() -%}
    current_timestamp
{%- endmacro -%}


{%- macro current_timestamp_in_utc() -%}
  {{ connector_macro('current_timestamp_in_utc') }}
{%- endmacro -%}

{%- macro default__current_timestamp_in_utc() -%}
    {{current_timestamp()}}
{%- endmacro -%}

{%- macro snowflake__current_timestamp_in_utc() -%}
    convert_timezone('UTC', {{current_timestamp()}})::{{type_timestamp()}}
{%- endmacro -%}

{%- macro postgres__current_timestamp_in_utc() -%}
    (current_timestamp at time zone 'utc')::{{type_timestamp()}}
{%- endmacro -%}