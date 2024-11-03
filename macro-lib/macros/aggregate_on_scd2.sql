{% macro aggregate_on_scd2(relation, group_by, aggregate_expressions) %}
{% set new_columns = aggregate_expressions | map(attribute=1) | list %}

with
    records_re_sliced_w_new_timeline as (
        {{ macro_lib.reslice_timeline(relation, partition_by=group_by) }}
    ),
    aggregated as (
        select
            {{ group_by | join(', ') }},
            {% for exp in aggregate_expressions -%}
            {{ exp[0] }} as {{ exp[1] }},
            {% endfor %}
            valid_from,
            valid_to
        from
            records_re_sliced_w_new_timeline
        group by
            {{ group_by | join(', ') }},
            valid_from,
            valid_to
    ),
    final as (
        {{ macro_lib.merge_timeline('aggregated', partition_by=group_by + new_columns) }}
    )
select * from final

{% endmacro %}
