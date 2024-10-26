{% macro merge_timeline(relation, partition_by, unique_columns) %}
with Periods as (
    select
        id,
        val,
        category,
        valid_from,
        valid_to,
        -- Identify the start of a new category of adjacent periods
        case
            when valid_from = LAG(valid_to) over (partition by id, val, category order by valid_from) then 0
            else 1
        end as is_new_category
    from
        {{ relation }}
),
categorys as (
    select
        id,
        val,
        category,
        valid_from,
        valid_to,
        -- Create a category identifier based on the cumulative sum of new categorys
        sum(is_new_category) over (partition by id, val, category order by valid_from) as grp
    from
        Periods
)
select
    id,
    val,
    category,
    min(valid_from) as valid_from,
    max(valid_to)   as valid_to
from categorys
group by id,
    val,
    category,
    grp
order by id,
    valid_from
{% endmacro %}
