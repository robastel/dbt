{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
    {%- if node.fqn[0] != project_name -%}
        {%- if target.name == 'dev' -%}
            {{ 'external_packages__' ~ node.name }}
        {%- else -%}
            {{ node.name }}
        {%- endif -%}
    {%- elif custom_alias_name is not none and node.resource_type in ['seed', 'model'] -%}
        {%- set error_message -%}
            {{ node.resource_type | capitalize }} '{{ node.unique_id }}' has a pre-defined alias.
            Resource file names must follow the pattern '[schema]__[alias].sql'.
        {%- endset -%}
        {{ exceptions.raise_compiler_error(error_message) }}
    {%- elif target.name == 'dev' -%}
        {{ node.name }}
    {%- else -%}
        {% set node_name = node.name %}
        {% set split_name = node_name.split('__') %}
        {{ split_name[1] | trim }}
    {%- endif -%}
{%- endmacro %}