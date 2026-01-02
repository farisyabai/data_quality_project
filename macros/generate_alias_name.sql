{% macro generate_alias_name(custom_alias_name=none, node=none) -%}

    {# 1. Target tests that are storing failures #}
    {%- if node is not none and node.resource_type == 'test' -%}
        
        {%- set is_storing_failures = node.config.get('store_failures', False) -%}
        
        {%- if is_storing_failures -%}
            
            {# 
               IMPORTANT: We use modules.datetime to get the current date.
               If you are testing on 2026-01-01, this MUST evaluate to that date.
            #}
            {% set date_suffix = modules.datetime.datetime.now().strftime("%Y_%m_%d") %}
            
            {%- if custom_alias_name -%}
                {{ (custom_alias_name | trim) ~ '_' ~ date_suffix }}
            {%- else -%}
                {{ (node.name | trim) ~ '_' ~ date_suffix }}
            {%- endif -%}

        {%- else -%}
            {{ custom_alias_name if custom_alias_name else node.name }}
        {%- endif -%}

    {# 2. Default logic for regular models #}
    {%- else -%}
        {{ custom_alias_name if custom_alias_name is not none else node.name }}
    {%- endif -%}

{%- endmacro %}