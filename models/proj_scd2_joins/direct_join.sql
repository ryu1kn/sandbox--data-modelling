with
    scd2_table1 as (select * from {{ ref('scd2_table1') }}),
    scd2_table2 as (select * from {{ ref('scd2_table2') }}),
    scd2_table3 as (select * from {{ ref('scd2_table3') }}),

    prep1 as (
        select
            t1.pk,
            dim1,
            dim2,
            greatest(t1.valid_from, coalesce(t2.valid_from, '1900-01-01'::timestamp)) as valid_from,
            coalesce(lead(greatest(t1.valid_from, coalesce(t2.valid_from, '1900-01-01'::timestamp)))
                     over (partition by t1.pk
                order by greatest(t1.valid_from, coalesce(t2.valid_from, '1900-01-01'::timestamp))),
                     '9999-12-31'::timestamp) as valid_to
        from scd2_table1 t1
        join scd2_table2 t2
            on t1.pk = t2.pk
            and t1.valid_from < t2.valid_to
            and t1.valid_to > t2.valid_from
    ),
    prep2 as (
        select
            t1.pk,
            t1.dim1,
            t1.dim2,
            t2.dim3,
            greatest(t1.valid_from, coalesce(t2.valid_from, '1900-01-01'::timestamp)) as valid_from,
            coalesce(lead(greatest(t1.valid_from, coalesce(t2.valid_from, '1900-01-01'::timestamp)))
                     over (partition by t1.pk
                order by greatest(t1.valid_from, coalesce(t2.valid_from, '1900-01-01'::timestamp))),
                     '9999-12-31'::timestamp) as valid_to
        from prep1 as t1
        join scd2_table3 as t2
            -- join scd2_table3_continuous_intervals as t2
            -- full outer join scd2_table3 as t2
            -- full outer join scd2_table3_continuous_intervals as t2
            on t1.pk = t2.pk
            and t1.valid_from < t2.valid_to
            and t1.valid_to > t2.valid_from
    )
select
    *
from prep2
where valid_from != valid_to -- important: dedup
order by PK, valid_from, valid_to, dim1, dim2, dim3
