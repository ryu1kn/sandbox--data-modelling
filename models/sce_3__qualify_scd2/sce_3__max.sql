with
    timeline as (
        select
            department,
            valid_from,
            coalesce(lead(valid_from) over(partition by department order by valid_from), '9999-12-31'::date) as valid_to
        from {{ ref('sce_3__employees_scd2') }}
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
        select
            *,
            lag(employee_id) over(partition by department order by valid_from) as prev_employee_id,
            lag(department) over(partition by department order by valid_from) as prev_department,
            lag(num_feedback_provided) over(partition by department order by valid_from) as prev_num_feedback_provided,
        from department_tops
    ),
    department_tops_wo_dup_records as (
        select *
        from department_tops_w_prev_values
        where employee_id is distinct from prev_employee_id
            or department is distinct from prev_department
            or num_feedback_provided is distinct from prev_num_feedback_provided
    ),
    final as (
        select
            employee_id,
            department,
            num_feedback_provided,
            valid_from,
            lead(valid_from) over(partition by department order by valid_from) as valid_to
        from department_tops_wo_dup_records
    )
select * from final
order by department, valid_from, employee_id
