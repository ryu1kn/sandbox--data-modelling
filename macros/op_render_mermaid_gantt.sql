{% macro op_render_mermaid_gantt(relation, columns=['columns(*)'], order_by=columns) -%}
    {% set query_gantt_lines %}
    select
        concat_ws(', ', {{ columns | join(', ') }})
            || ' : '
            || concat_ws(', ',
                         'done',
                         valid_from::date,
                         if(valid_to < '2023-01-11', valid_to::date, '2023-01-11')
               ) as validity_period
    from {{ relation }}
    order by {{ order_by | join(', ') }}
    {% endset %}

    {% set lines = run_query(query_gantt_lines) %}

    {% set gantt_section %}
    section {{ relation }}
        {% for l in lines.columns['validity_period'] -%}
        {{ l }}
        {% endfor %}
    {% endset %}

    {{ print(gantt_section | replace('\n    ', '\n')) }}
{% endmacro %}
