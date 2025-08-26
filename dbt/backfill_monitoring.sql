{% macro backfill_monitoring() %}
  {% if is_incremental() %}
        , array_append(
            _metadata,
            struct(
              current_timestamp() as timestamp
              , {{ invocation_id }} as invocation_id
              , {{ run_started_at }} as run_started_at
              , case 
                when {{ is_incremental() }} 
                    then concat(
                      '{{ env_var("DATA_INTERVAL_START", "None in env") }}', 
                      ' - ', 
                      '{{ env_var("DATA_INTERVAL_END", "None in env") }}'
                    )
                    else '{{ env_var("MONDAY_WEEKS_TO_BACKFILL", "None in env") }}'
                end as data_interval
              , '{{ env_var("FULL_REFRESH", "None in env") }}' as is_full_refresh
              , '{{ env_var("DATABRICKS_RUN_ID", "None in env") }}' as databricks_run_id
              , '{{ env_var("DATABRICKS_JOB_ID", "None in env") }}' as databricks_job_id
            )
        ) as _metadata
    {% else %}
        , array(
            struct(
              current_timestamp() as timestamp
              , {{ invocation_id }} as invocation_id
              , {{ run_started_at }} as run_started_at
              , case 
                when {{ is_incremental() }} 
                    then concat(
                      '{{ env_var("DATA_INTERVAL_START", "None in env") }}', 
                      ' - ', 
                      '{{ env_var("DATA_INTERVAL_END", "None in env") }}'
                    )
                    else '{{ env_var("MONDAY_WEEKS_TO_BACKFILL", "None in env") }}'
                end as data_interval
              , '{{ env_var("FULL_REFRESH", "None in env") }}' as is_full_refresh
              , '{{ env_var("DATABRICKS_RUN_ID", "None in env") }}' as databricks_run_id
              , '{{ env_var("DATABRICKS_JOB_ID", "None in env") }}' as databricks_job_id
            )
        )
    {% endif %}
  {% endmacro %}
