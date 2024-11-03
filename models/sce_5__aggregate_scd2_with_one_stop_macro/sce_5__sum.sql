{% call(split_scd2) macro_lib.aggregate_on_scd2(
    relation=ref('sce_2__table_scd2'),
    split_by=['group_id'],
    merge_by=['group_id', 'summed_val'],
) %}

select
    group_id,
    sum(val) as summed_val,
    valid_from,
    valid_to,
from {{ split_scd2 }}
group by all

{% endcall %}
