{% macro merge_timeline(relation, partition_by, unique_columns) %}
with
    department_tops_w_prev_values as (
        select
            * exclude valid_to,
            lead(valid_to) over w as valid_to,
        from {{ relation }}
        window w as (partition by {{ partition_by }} order by valid_from)

        -- Keep the record if any of unique_columns values are different from
        -- the previous record (when sorted by valid_from
        qualify
            (lag({{ partition_by }}) over w) is distinct from {{ partition_by }}
            {% for column in unique_columns -%}
            or (lag({{ column }}) over w) is distinct from {{ column }}
            {% endfor %}
    )
select * from department_tops_w_prev_values
{% endmacro %}
