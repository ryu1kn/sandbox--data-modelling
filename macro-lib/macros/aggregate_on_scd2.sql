{% macro aggregate_on_scd2(relation, group_by, transform_scd1, repartition_by) %}
{% set new_columns = aggregate_expressions | map(attribute=1) | list %}

with
    records_re_sliced_w_new_timeline as (
        {{ macro_lib.reslice_timeline(relation, partition_by=group_by) }}
    ),
    aggregated as (
        {{ transform_scd1 }}
    ),
    final as (
        {{ macro_lib.merge_timeline('aggregated', partition_by=group_by + repartition_by) }}
    )
select * from final

{% endmacro %}
