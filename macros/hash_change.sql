{% macro hash_change(fields) %}
  {# Generate MD5 hash for change detection across tracked fields #}
  md5(
    {% for field in fields %}
      coalesce(cast({{ field }} as varchar), '')
      {% if not loop.last %} || '|' || {% endif %}
    {% endfor %}
  )
{% endmacro %}