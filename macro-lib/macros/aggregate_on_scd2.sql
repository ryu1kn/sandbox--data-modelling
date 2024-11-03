{#
 # This macro allows you to aggregate / window calculate a table that is in SCD2 format.
 # It works in 3 steps
 #
 # 1. Split validity periods with regards to the columns you want to group by (split_by).
 # 2. Run the provided transformation (e.g. aggregation) on the split validity periods
 # 3. Merge adjacent validity periods when merge_by columns are the same
 #}
{% macro aggregate_on_scd2(relation, split_by, merge_by) %}

with
    records_re_sliced_w_new_timeline as (
        {{ macro_lib.reslice_timeline(relation, partition_by=split_by) }}
    ),
    aggregated as (
        {{ caller('records_re_sliced_w_new_timeline') }}
    ),
    final as (
        {{ macro_lib.merge_timeline('aggregated', partition_by=merge_by) }}
    )
select * from final

{% endmacro %}
