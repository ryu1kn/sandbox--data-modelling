with
    records_re_sliced_w_new_timeline as (
        {{ macro_lib.reslice_timeline(ref('sce_2__table_scd2'), 'group_id') }}
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