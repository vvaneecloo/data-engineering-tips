{% macro log_incremental() %}
    {% set common_vars %}
        current_timestamp() as timestamp
        , "{{ invocation_id | string }}" as invocation_id
        , "{{ run_started_at | string }}" as run_started_at
        , "{{ is_incremental() | string }}" :: boolean  as is_full_refresh
        , "{{ env_var('RUN_ID', 'None') | string }}" :: string as run_id
        , "{{ env_var('JOB_ID', 'None') | string }}" :: string as job_id
    {% endset %}

    {% if is_incremental() %}
        array_append(
            _metadata,
            struct(
              {{ common_vars }}
              , "{{ env_var('DATA_INTERVAL_START', 'None') ~ ' - ' ~ env_var('DATA_INTERVAL_END', 'None') | string }}" :: string as data_interval
            )
        ) as _metadata
    {% else %}
        array(
            struct(
              {{ common_vars }}
              , "{{ env_var('MONDAY_WEEKS_TO_BACKFILL', 'None') | string }}" :: string as data_interval
            )
        ) as _metadata
    {% endif %}
{% endmacro %}
