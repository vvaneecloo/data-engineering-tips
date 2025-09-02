/*
    !!! Warning: to use this test, you must have implemented the `insert_row_count` macro / use dbt_artifacts package with a cloud platform
    that gives you the number of updated / inserted rows (BigQuery for instance).

    This test aims to compare the row count of a table after it has been updated to the row count before the update.

    To use this in your dbt project you'd have to:

        1) Add this piece of code to `model.yml` file (you can also add it in the config in sql):
            ```
            models:
              my_model:
                tests:
                  - row_count_lower_than_accepted_value:
                      accepted_value: 0.05
            ```
*/

{% test row_count_lower_than_accepted_value(model, accepted_value='0,05') %}

with
    row_count as (
        select
            1
        from
            {{ _get_monitoring_target_schema() }}.row_count_monitoring
        where
            model = {{ model }}
        qualify 
            1 - lag(row_count, 1) over (order by execution_time) / row_count >= '{{ accepted_value }}'
        order by
            execution_time desc
        limit 1
    )

select * from row_count

{% endtest %}
