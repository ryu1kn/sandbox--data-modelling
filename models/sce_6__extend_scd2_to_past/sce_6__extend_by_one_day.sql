{{ macro_lib.extend_scd2_history(
    relation_scd2=ref('sce_6__employees_scd2'),
    relation_scd1=ref('sce_6__employees_scd1'),
    primary_key=['employee_id'],
    scd1_date='2023-01-01',
    scd2_begin_date='2023-01-02',
) }}
