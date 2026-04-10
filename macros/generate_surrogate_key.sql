{% macro generate_surrogate_key(field_list) %}
  {# Generate a surrogate key from one or more business key fields #}
  md5(
    {% for field in field_list %}
      coalesce(cast({{ field }} as varchar), '_null_')
      {% if not loop.last %} || '|' || {% endif %}
    {% endfor %}
  )
{% endmacro %}