with
    scd2_table1 as (select * from {{ ref('sce_1__table1_scd2') }}),
    scd2_table2 as (select * from {{ ref('sce_1__table2_scd2') }}),
    scd2_table3 as (select * from {{ ref('sce_1__table3_scd2') }}),

    unified_timeline as ( -- using union to deal with duplicates values instead of union all
        select pk, valid_from from scd2_table1 union
        select pk, valid_from from scd2_table2 union
        select pk, valid_from from scd2_table3
    ),
    unified_timeline_recalculate_valid_to as (
        select
            pk,
            valid_from,
            coalesce(lead(valid_from) over(partition by pk order by valid_from), '9999-12-31'::timestamp) as valid_to,
            valid_to = '9999-12-31'::timestamp as is_current
        from unified_timeline
    ),
    joined as (
        select
            timeline.pk,
            scd2_table1.dim1,
            scd2_table2.dim2,
            scd2_table3.dim3,
            coalesce(timeline.valid_from, '1900-01-01'::timestamp) as valid_from,
            coalesce(timeline.valid_to, '9999-12-31'::timestamp) as valid_to
        from unified_timeline_recalculate_valid_to as timeline
        left join scd2_table1
            on timeline.pk = scd2_table1.pk
            and scd2_table1.valid_from <= timeline.valid_from
            and scd2_table1.valid_to >= timeline.valid_to
        left join scd2_table2
            on timeline.pk = scd2_table2.pk
            and scd2_table2.valid_from <= timeline.valid_from
            and scd2_table2.valid_to >= timeline.valid_to
        left join scd2_table3
            on timeline.pk = scd2_table1.pk
            and scd2_table3.valid_from <= timeline.valid_from
            and scd2_table3.valid_to >= timeline.valid_to

    )
select * from joined
-- where valid_from != valid_to -- As we already have a distinct timeline (using union), this condition is no longer needed
order by PK, valid_from, valid_to, dim1, dim2, dim3
