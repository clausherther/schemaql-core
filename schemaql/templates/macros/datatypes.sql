{%- macro type_string() -%}
{{ connector_macro('type_string') }}
{%- endmacro -%}

{%- macro bigquery__type_string() -%}
string
{%- endmacro -%}

{%- macro redshift__type_string() -%}
varchar
{%- endmacro -%}

{%- macro postgres__type_string() -%}
varchar
{%- endmacro -%}

{%- macro snowflake__type_string() -%}
varchar
{%- endmacro -%}



{# timestamp  -------------------------------------------------     #}

{%- macro type_timestamp() -%}
{{ connector_macro('type_timestamp') }}
{%- endmacro -%}

{%- macro default__type_timestamp() -%}
timestamp
{%- endmacro -%}

{%- macro snowflake__type_timestamp() -%}
timestamp_ntz
{%- endmacro -%}


{# float  -------------------------------------------------     #}

{%- macro type_float() -%}
{{ connector_macro('type_float') }}
{%- endmacro -%}

{%- macro default__type_float() -%}
float
{%- endmacro -%}

{%- macro bigquery__type_float() -%}
float64
{%- endmacro -%}


{# bigint  -------------------------------------------------     #}

{%- macro type_bigint() -%}
{{ connector_macro('type_bigint') }}
{%- endmacro -%}

{%- macro default__type_bigint() -%}
bigint
{%- endmacro -%}

{%- macro bigquery__type_bigint() -%}
int64
{%- endmacro -%}

{# int  -------------------------------------------------     #}

{%- macro type_int() -%}
{{ connector_macro('type_int') }}
{%- endmacro -%}

{%- macro default__type_int() -%}
int
{%- endmacro -%}

{%- macro bigquery__type_int() -%}
int64
{%- endmacro -%}
