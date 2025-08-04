{% macro insert_row_count() %}
    {%- if target.name == 'prod' -%}
        {%- set accepted_commands = ['run', 'build'] -%}
        {%- if invocation_args_dict.get('which') in accepted_commands -%}
            {{ _create_monitoring_table_if_not_exists() }}
            {{ _append_row_count_to_row_count_monitoring() }}
        {%- endif -%}
    {%- endif -%}
{% endmacro %}

{% macro _create_monitoring_table_if_not_exists() %}
        create table if not exists {{ _get_monitoring_target_schema() }}.row_count_monitoring (
            execution_time timestamp,
            model string,
            invocation_id string,
            package_name string,
            row_count bigint
        );
{% endmacro %}

{% macro _append_row_count_to_row_count_monitoring() %}
        insert into {{ _get_monitoring_target_schema() }}.row_count_monitoring
                select 
                    current_timestamp() as execution_time,
                    '{{ this }}' as model,
                    '{{ invocation_id }}' as invocation_id,
                    '{{ project_name }}' as package_name,
                    count(1) as row_count
                from 
                    {{ this }};
{% endmacro %}

{% macro _get_monitoring_target_schema() %}
    {%- set db_name = 'your_db_name' -%}
    
    {%- if target.name == 'prod' -%}
        {{ return("prod_" ~ db_name) }}
    {%- elif target.name == 'preprod' -%}
        {{ return("preprod_" ~ db_name) }}
    {%- else -%}
       {{ return("dev_" ~ db_name) }}
    {%- endif -%}
{% endmacro %}
