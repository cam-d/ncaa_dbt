{% macro col_diff(column_name, prefix = '') %}
   a{{prefix}}_{{column_name}} - b{{prefix}}_{{column_name}} AS {{ column_name }}_diff
{% endmacro %}