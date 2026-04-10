{% macro audit_columns() %}
  current_timestamp() as etl_load_dts,
  current_timestamp() as etl_update_dts,
  '{{ invocation_id }}' as etl_batch_id,
  '{{ this.name }}' as etl_source_model
{% endmacro %}