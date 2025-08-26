{% macro log_incremental() %}
    {% if is_incremental() %}
        array_append(
            _metadata,
            struct(
              current_timestamp() as timestamp
              , '{{ invocation_id | string }}' as invocation_id
              , '{{ run_started_at | string }}' as run_started_at
              , '{{ env_var("DATA_INTERVAL_START", "None") ~ " - " ~ env_var("DATA_INTERVAL_END", "None") }}' :: string as data_interval
              , '{{ is_incremental()  }}' :: boolean  as is_full_refresh
              , '{{ env_var("DATABRICKS_RUN_ID", "None") }}' :: string as databricks_run_id
              , '{{ env_var("DATABRICKS_JOB_ID", "None") }}' :: string as databricks_job_id
            )
        ) as _metadata
    {% else %}
        array(
            struct(
              current_timestamp() as timestamp
              , '{{ invocation_id | string }}' as invocation_id
              , '{{ run_started_at | string }}' as run_started_at
              , '{{ env_var("BACKFILL_INTERVAL_START", "None") ~ " - " ~ env_var("BACKFILL_INTERVAL_END", "None") }}' :: string as data_interval
              , '{{ is_incremental()  }}' :: boolean  as is_full_refresh
              , '{{ env_var("DATABRICKS_RUN_ID", "None") }}' :: string as databricks_run_id
              , '{{ env_var("DATABRICKS_JOB_ID", "None") }}' :: string as databricks_job_id
            )
        )
    {% endif %}
{% endmacro %}
