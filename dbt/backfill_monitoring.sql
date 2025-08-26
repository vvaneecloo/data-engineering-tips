{% macro backfill_monitoring() %}
  {% if is_incremental() %}
      , array_append(array(
            struct(
              current_timestamp() as updated_at,
              '{{ invocation_id }}' as run_id,
              '{{ var("run_type", None) }}' as run_type,
              '{{ var("data_interval_start", "NA") }}' as backfill_start,
              '{{ var("data_interval_end", "NA") }}' as backfill_end
            )
          )
      )
  {% else %}
    , array(
            struct(
              current_timestamp() as updated_at,
              '{{ invocation_id }}' as run_id,
              '{{ var("run_type", None) }}' as run_type,
              '{{ var("data_interval_start", "NA") }}' as backfill_start,
              '{{ var("data_interval_end", "NA") }}' as backfill_end
            )
          )
      )
{% endmacro %}
