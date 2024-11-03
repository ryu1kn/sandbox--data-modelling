{% call(split_scd2) macro_lib.aggregate_on_scd2(
    relation=ref('sce_3__employees_scd2'),
    split_by=['department'],
    merge_by=['department', 'employee_id', 'num_feedback_provided'],
) %}

select *
from {{ split_scd2 }}
qualify row_number() over(partition by department, valid_from order by num_feedback_provided desc, employee_id) = 1

{% endcall %}
