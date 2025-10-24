-- A sample macro for generating surrogate keys
{% macro generate_surrogate_key(field_list) -%}
    {{ return(adapter.dispatch('generate_surrogate_key', 'barclays_glue_dbt')(field_list)) }}
{%- endmacro %}

{% macro default__generate_surrogate_key(field_list) -%}
    {%- set fields = [] -%}
    {%- for field in field_list -%}
        {%- set _ = fields.append("coalesce(cast(" ~ field ~ " as string), '')") -%}
    {%- endfor -%}
    md5(concat({{ fields | join(', ') }}))
{%- endmacro %}