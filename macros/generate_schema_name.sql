{% macro generate_schema_name(custom_schema_name=none, node=none) -%}
    {%- if node.fqn[0] != project_name -%}
        {%- if target.name == 'dev' -%}
            {{ target.schema }}
        {%- else -%}
            external_packages
        {%- endif -%}
    {%- elif custom_schema_name is not none and node.resource_type in ['seed', 'model'] -%}
        {%- set error_message -%}
            {{ node.resource_type | capitalize }} '{{ node.unique_id }}' has a pre-defined schema.
            Resource file names must follow the pattern '[schema]__[alias].sql'.
        {%- endset -%}
        {{ exceptions.raise_compiler_error(error_message) }}
    {%- elif target.name == 'dev' -%}
        {{ target.schema }}
    {%- else -%}
        {% set node_name = node.name %}
        {% set split_name = node_name.split('__') %}
        {{ split_name[0] | trim }}
    {%- endif -%}
{%- endmacro %}