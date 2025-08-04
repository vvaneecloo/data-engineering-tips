/*
    This macro aims to override the ref dbt macro to automatically add a limit to all
    relations, e.g:

    `{{ source('prod', 'some_table') }}` becomes `select * from prod.some_table limit 1000`
    instead of `select * from prod.some_table`

    To use this in your dbt project you'd have to modify:
        1) target.name / profile.name depending on your use case,
        2) The target you're comparing to (here 'dev'),
        3) The limit you want to add (here '1000'),
        4) You can also change the limit logic (limit / tablesample) based on your data warehouse.
*/

{% macro ref(model_name) %}
    {% set relation = builtins.ref(model_name) %}

    {% if target.name  == 'dev' %}
        -- override ref macro for {{ relation }} as pipeline is launched in dev
        select * from {{ relation }} limit 1000
    {% else %}
        {{ relation }}
    {% endif %}
{% endmacro %}
