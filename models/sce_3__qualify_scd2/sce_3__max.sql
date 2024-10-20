with
    timeline as (
        select
            department,
            valid_from,
            coalesce(lead(valid_from) over w, '9999-12-31'::date) as valid_to
        from {{ ref('sce_3__employees_scd2') }}
        window w as (partition by department order by valid_from)
        qualify (lead(valid_from) over w) is distinct from valid_from
    ),
    records_re_sliced_w_new_timeline as (
        select timeline.*, orig.* exclude (department, valid_from, valid_to)
        from timeline
        left join {{ ref('sce_3__employees_scd2') }} as orig
        on timeline.department = orig.department
            and orig.valid_from <= timeline.valid_from
            and orig.valid_to >= timeline.valid_to
    ),

    -- Here, we apply the `qualify` to find the top employee per department
    department_tops as (
        select *
        from records_re_sliced_w_new_timeline
        qualify row_number() over(partition by department, valid_from order by num_feedback_provided desc, employee_id) = 1
    ),

    -- Rest is to clean up (merge) SCD2 validity ranges
    department_tops_w_prev_values as (
        select *
        from department_tops
        window w as (partition by department order by valid_from)
        qualify
            (lag(employee_id) over w) is distinct from employee_id
            or (lag(department) over w) is distinct from department
            or (lag(num_feedback_provided) over w) is distinct from num_feedback_provided
    ),
    final as (
        select
            employee_id,
            department,
            num_feedback_provided,
            valid_from,
            lead(valid_from) over (partition by department order by valid_from) as valid_to
        from department_tops_w_prev_values
    )
select * from final
order by department, valid_from, employee_id
