/*
    These macros aim to insert the row count of a table after it has been updated.
    This is especially useful to use as a statistics & alerting table.

    To use this in your dbt project you'd have to:

        1) Add this piece of code to `dbt_project.yml`:
            ```
            on-run-start: 
                - "{{ create_monitoring_table_if_not_exists() }}"
            ```

        2) Add this other piece of code in your `dbt_project.yml` (depending on what models you want to monitor):
            ```
            +post-hook:
                - "{{ insert_row_count() }}"
            ```
*/

{% macro insert_row_count() %}
    {%- if target.name == 'prod' -%}
        {%- set accepted_commands = ['run', 'build'] -%}
        {%- if invocation_args_dict.get('which') in accepted_commands -%}
            insert into {{ _get_monitoring_target_schema() }}.row_count_monitoring
                select 
                    current_timestamp() as execution_time,
                    '{{ this }}' as model,
                    '{{ invocation_id }}' as invocation_id,
                    '{{ project_name }}' as package_name,
                    count(1) as row_count
                from 
                    {{ this }};
        {%- endif -%}
    {%- endif -%}
{% endmacro %}

{% macro create_monitoring_table_if_not_exists() %}
    {%- if target.name == 'prod' -%}
        create table if not exists {{ _get_monitoring_target_schema() }}.row_count_monitoring (
            execution_time timestamp,
            model string,
            invocation_id string,
            package_name string,
            row_count bigint
        );
    {%- endif -%}
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

