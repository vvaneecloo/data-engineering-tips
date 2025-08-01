/*
        This macro aims to override the source dbt macro to automatically add a limit to all
        relations, e.g:

        `{{ source('prod', 'some_table') }}` becomes `select * from prod.some_table limit 1000`
        instead of `select * from prod.some_table`

        To use this in your dbt project you'd have to modify:
            - target.name / profile.name depending on your use case,
            - the object you're comparing to (here 'dev')
            - the limit you want to add (here '1000'),
            - you can also change the limit logic (limit / tablesample) based on your data warehouse
*/

{% macro source(model_name) %}
    {% set relation = builtins.source(model_name) %}

    {% if target.name  == 'dev' %}
        -- override source macro for {{ relation }} as pipeline is launched in dev
        select * from {{ relation }} limit 1000
    {% else %}
        {{ relation }}
    {% endif %}
{% endmacro %}
