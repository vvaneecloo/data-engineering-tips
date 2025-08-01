/*
        Here you need to change this part of the code to make it work
        with your profile.yml file.

        You'd have to modify:
            - target.name / profile.name depending on your use case,
            - the object you're comparing to (here 'dev')
            - the limit you want to add (here '1000'),
            - you can also change the limit logic (limit / tablesample) based on your data warehouse
*/

{% macro source(model_name) %}
    {% set node = builtins.source(model_name) %}

    {% if target.name  == 'dev' %}
        -- override source macro for {{ node }} as pipeline is launched in dev
        select * from {{ node }} limit 1000
    {% else %}
        {{ node }}
    {% endif %}
{% endmacro %}
