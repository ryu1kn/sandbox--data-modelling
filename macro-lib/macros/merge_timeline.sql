{% macro merge_timeline(relation, partition_by) %}
{% set partition_by_str = partition_by | join(', ') %}

with
    periods_w_new_group_flag as (
        select
            *,
            -- Identify the start of a new category of adjacent periods
            if(valid_from = lag(valid_to) over (partition by {{ partition_by_str }} order by valid_from), 0, 1) as is_new_group
        from
            {{ relation }}
    ),
    periods_w_group_id as (
        select
            * exclude is_new_group,
            -- Create a group id based on the cumulative sum of new group
            sum(is_new_group) over (partition by {{ partition_by_str }} order by valid_from) as group_id
        from periods_w_new_group_flag
    )
select
    {{ partition_by_str }},
    min(valid_from) as valid_from,
    max(valid_to)   as valid_to
from periods_w_group_id
group by {{ partition_by_str }}, group_id
{% endmacro %}
