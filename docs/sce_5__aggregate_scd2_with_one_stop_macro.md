# Scenario 5: Aggregate SCD2 with one macro

## Problem

Do the same problem as in [Scenario 2](./sce_2__aggregate_scd2.md) and [Scenario 3](./sce_3__qualify_scd2.md)
but use just one macro to simplify the solution except the aggregation part.

## Setup

```sh
dbt build --select +path:models/sce_5__aggregate_scd2_with_one_stop_macro
```

```mermaid
gantt
    dateFormat  YYYY-MM-DD
    title       Aggregate SCD2 data (with one-stop macro)

    section sce_2__table_scd2
        id_1,a,(1) : done, 2023-01-01, 2023-01-03
        id_1,a,(3) : done, 2023-01-03, 2023-01-05
        id_1,a,(2) : done, 2023-01-05, 2023-01-07
        id_1,a,(4) : done, 2023-01-07, 2023-01-09
        id_1,a,(5) : done, 2023-01-09, 2023-01-11
        id_2,a,(1) : done, 2023-01-02, 2023-01-05
        id_2,a,(2) : done, 2023-01-05, 2023-01-06
        id_2,a,(3) : done, 2023-01-06, 2023-01-11
        id_3,b,(1) : done, 2023-01-03, 2023-01-05
        id_3,b,(2) : done, 2023-01-05, 2023-01-11

    section sce_5__sum
        a,(1) : done, 2023-01-01, 2023-01-02
        a,(2) : done, 2023-01-02, 2023-01-03
        a,(4) : done, 2023-01-03, 2023-01-06
        a,(5) : done, 2023-01-06, 2023-01-07
        a,(7) : done, 2023-01-07, 2023-01-09
        a,(8) : active, 2023-01-09, 2023-01-11
        b,(1) : done, 2023-01-03, 2023-01-05
        b,(2) : active, 2023-01-05, 2023-01-11
```

```mermaid
gantt
    dateFormat  YYYY-MM-DD
    title       Qualify SCD2 data (with one-stop macro)

    section sce_3__employees_scd2
        e1,Marketing,(1) : done, 2023-01-01, 2023-01-04
        e1,Marketing,(2) : done, 2023-01-04, 2023-01-07
        e1,Marketing,(3) : done, 2023-01-07, 2023-01-09
        e1,Marketing,(5) : done, 2023-01-09, 2023-01-11
        e2,Marketing,(1) : done, 2023-01-02, 2023-01-05
        e2,Marketing,(3) : done, 2023-01-05, 2023-01-06
        e2,Marketing,(4) : done, 2023-01-06, 2023-01-11
        e3,Sales,(1) : done, 2023-01-03, 2023-01-05
        e3,Sales,(3) : done, 2023-01-05, 2023-01-11

    section sce_5__max
        e1,Marketing,(1) : done, 2023-01-01, 2023-01-04
        e1,Marketing,(2) : done, 2023-01-04, 2023-01-05
        e2,Marketing,(3) : done, 2023-01-05, 2023-01-06
        e2,Marketing,(4) : done, 2023-01-06, 2023-01-09
        e1,Marketing,(5) : done, 2023-01-09, 2023-01-11
        e3,Sales,(1) : done, 2023-01-03, 2023-01-05
        e3,Sales,(3) : done, 2023-01-05, 2023-01-11
```

## Generating mermaid Gantt chart sections

```sql
select
    concat_ws(',', group_id, '(' || summed_val || ')')
        || ' : '
        || concat_ws(', ',
                     if(valid_to < '2023-01-11', 'done', 'active'),
                     valid_from::date,
                     if(valid_to < '2023-01-11', valid_to::date, '2023-01-11')
           )
from sce_5__sum
order by group_id, valid_from;
```

```sql
select
    concat_ws(',', 'e' || employee_id, department, '(' || num_feedback_provided || ')')
        || ' : '
        || concat_ws(', ',
                     'done',
                     valid_from::date,
                     if(valid_to < '2023-01-11', valid_to::date, '2023-01-11')
           )
from sce_5__max
order by department, valid_from, employee_id;
```
