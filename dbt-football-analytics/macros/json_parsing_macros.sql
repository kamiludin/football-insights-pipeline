-- macros/json_parsing_macros.sql
{% macro parse_json_columns(table_name, json_columns_with_keys) %}
    {% set output = [] %}

    {% for column, keys in json_columns_with_keys.items() %}
        {% for key in keys %}
            {% set expression = "json_extract_path_text(replace(" ~ table_name ~ "." ~ column ~ ", '''', '\"'), '" ~ key ~ "') AS " ~ column ~ "_" ~ key %}
            {% do output.append(expression) %}
        {% endfor %}
    {% endfor %}

    {{ output | join(',\n    ') }}
{% endmacro %}