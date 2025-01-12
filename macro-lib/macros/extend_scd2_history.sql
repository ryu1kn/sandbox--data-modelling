{% macro extend_scd2_history(
    relation_scd2,
    scd2_begin_date,
    relation_scd1,
    scd1_date,
    primary_key,
    scd1_cols_to_merge=adapter.get_columns_in_relation(relation_scd1) | map(attribute='name')
) %}

with
    raw_history_stretched as (
        select
            scd1.*,
            '{{ scd1_date }}' :: date as valid_from,
            coalesce(scd2.valid_from, '{{ scd2_begin_date }}') as valid_to,
        from {{ relation_scd1 }} as scd1
        left join {{ relation_scd2 }} as scd2 on
            true
            {% for col in primary_key -%}
            and scd1.{{ col }} = scd2.{{ col }}
            {% endfor -%}
            and scd2.valid_from = '{{ scd2_begin_date }}'
        union
        select * from {{ relation_scd2 }}
    ),
    final as (
        {{ macro_lib.merge_timeline('raw_history_stretched', partition_by=scd1_cols_to_merge) }}
    )
select * from final

{% endmacro %}
