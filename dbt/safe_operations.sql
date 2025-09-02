/*
    These helper macros are designed to make aggregations and calculations more resilient when dealing with NULLs and division-by-zero.

    1) safe_sum(column)
       - Wraps a SUM() aggregation in COALESCE to ensure NULLs are treated as 0.
       - Example:
            select {{ safe_sum('amount') }} as total_amount
            from {{ ref('payments') }}

    2) safe_divide(numerator, denominator)
       - Divides two expressions while safely handling division by zero.
       - Returns 0 when the denominator is 0, otherwise performs numerator/denominator.
       - Example:
            select {{ safe_divide('orders', 'customers') }} as avg_orders_per_customer
            from {{ ref('daily_metrics') }}
*/

{% macro safe_sum(column) %}
    sum(coalesce({{ column }}, 0))
{% endmacro %}

{% macro safe_divide(numerator, denominator) %}
    case
        when {{ denominator }} = 0 then 0
        else {{ numerator }} / {{ denominator }}
    end
{% endmacro %}
