{% macro split_string_list(column, alias='element', delimiter=',') %}
    table(split_to_table({{ column }}, '{{ delimiter }}')) {{ alias }}
{% endmacro %}
