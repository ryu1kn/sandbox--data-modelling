with
    timeline as (
        select
            group_id,
            valid_from,
            coalesce(lead(valid_from) over w, '9999-12-31'::date) as valid_to
        from {{ ref('sce_2__table_scd2') }}
        window w as (partition by group_id order by valid_from)

        -- If we simply do `qualify valid_to is distinct from valid_from`
        -- It refers to original `valid_to` value, not the one we calculated.
        -- So we need to repeat the expression like below, or use a new column name.
        -- c.f. https://github.com/duckdb/duckdb/issues/5510
        qualify (lead(valid_from) over w) is distinct from valid_from
    ),
    records_re_sliced_w_new_timeline as (
        select orig.val, timeline.*,
        from timeline
        left join {{ ref('sce_2__table_scd2') }} as orig
        on timeline.group_id = orig.group_id
            and orig.valid_from <= timeline.valid_from
            and orig.valid_to >= timeline.valid_to
    ),
    final as (
        select
            group_id,
            sum(val) as summed_val,
            valid_from,
            valid_to,
        from records_re_sliced_w_new_timeline
        group by all
        order by group_id, valid_from
    )
select * from final
