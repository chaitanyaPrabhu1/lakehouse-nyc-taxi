{#
  Trip duration in minutes between two timestamps, as a double.

  Centralizing this keeps the silver layer DRY and means the dialect-specific
  date math (Athena/Trino `date_diff`) lives in exactly one place.
#}
{% macro trip_duration_minutes(start_ts, end_ts) %}
    (date_diff('second', {{ start_ts }}, {{ end_ts }}) / 60.0)
{% endmacro %}
