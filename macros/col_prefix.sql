{% macro col_prefix(column_name, prefix) %}
   {{column_name}} AS {{prefix}}_{{ column_name }}
{% endmacro %}