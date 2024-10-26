# Scenario 4: Aggregate SCD2 with macros

## Problem

Do the same problem as in [Scenario 2](./sce_2__aggregate_scd2.md), but use macros to simplify the solution.

## Setup

```sh
dbt build --select +sce_4__aggregate_scd2_with_macros
```

```mermaid
gantt
    dateFormat  YYYY-MM-DD
    title       Aggregate SCD2 data (with macro)

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
        id_3,b,(1) : done, 2023-01-05, 2023-01-11

    section sce_2__sum
        a,(1) : done, 2023-01-01, 2023-01-02
        a,(2) : done, 2023-01-02, 2023-01-03
        a,(4) : done, 2023-01-03, 2023-01-05
        a,(4) : done, 2023-01-05, 2023-01-06
        a,(5) : done, 2023-01-06, 2023-01-07
        a,(7) : done, 2023-01-07, 2023-01-09
        a,(8) : done, 2023-01-09, 2023-01-11
        b,(1) : done, 2023-01-03, 2023-01-05
        b,(1) : done, 2023-01-05, 2023-01-11

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
from sce_2__sum
order by group_id, valid_from;
```
