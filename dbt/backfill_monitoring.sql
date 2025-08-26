{% macro backfill_monitoring() %}
  {% if is_incremental() %}
        , array_append(
            _updated_at, 
            struct(
              current_timestamp() as timestamp
              {% if is_incremental() %}
                , concat(
                  '{{ env_var("DATA_INTERVAL_START", "None in env") }}', 
                  ' - ', 
                  '{{ env_var("DATA_INTERVAL_END", "None in env") }}'
                ) as data_interval
              {% else %}
                '{{ env_var("MONDAY_WEEKS_TO_BACKFILL", "None in env") }}' as data_interval
              {% endif %}
              , '{{ env_var("FULL_REFRESH", "None in env") }}' as is_full_refresh
              , '{{ env_var("DATABRICKS_RUN_ID", "None in env") }}' as databricks_run_id
              , '{{ env_var("DATABRICKS_JOB_ID", "None in env") }}' as databricks_job_id
            )
        ) as _metadata
    {% else %}
        , array(
            struct(
              current_timestamp() as timestamp
              {% if is_incremental() %}
                , concat(
                  '{{ env_var("DATA_INTERVAL_START", "None in env") }}', 
                  ' - ', 
                  '{{ env_var("DATA_INTERVAL_END", "None in env") }}'
                ) as data_interval
              {% else %}
                '{{ env_var("MONDAY_WEEKS_TO_BACKFILL", "None in env") }}' as data_interval
              {% endif %}
              , '{{ env_var("FULL_REFRESH", "None in env") }}' as is_full_refresh
              , '{{ env_var("DATABRICKS_RUN_ID", "None in env") }}' as databricks_run_id
              , '{{ env_var("DATABRICKS_JOB_ID", "None in env") }}' as databricks_job_id
            )
        ) as _metadata
    {% endif %}
  {% endmacro %}
