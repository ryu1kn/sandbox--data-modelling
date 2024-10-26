{% macro reslice_timeline(relation, partition_by) %}
with
    timeline as (
        select
            {{ partition_by }},
            valid_from,
            coalesce(lead(valid_from) over w, valid_to, '9999-12-31'::date) as valid_to
        from {{ relation }}
        window w as (partition by {{ partition_by }} order by valid_from)

        -- If we simply do `qualify valid_to is distinct from valid_from`
        -- It refers to original `valid_to` value, not the one we calculated.
        -- So we need to repeat the expression like below, or use a new column name.
        -- c.f. https://github.com/duckdb/duckdb/issues/5510
        qualify (lead(valid_from) over w) is distinct from valid_from
    ),
    records_re_sliced_w_new_timeline as (
        select
            orig.* exclude (valid_from, valid_to),
            timeline.valid_from,
            timeline.valid_to,
        from timeline
        left join {{ relation }} as orig
        on timeline.{{ partition_by }} = orig.{{ partition_by }}
            and orig.valid_from <= timeline.valid_from
            and orig.valid_to >= timeline.valid_to
    )
select * from records_re_sliced_w_new_timeline
{% endmacro %}
