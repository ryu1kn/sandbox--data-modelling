# SCD2 joins

https://infinitelambda.com/multitable-scd2-joins/

## Setup

```sh
dbt build
```

```mermaid
gantt
    dateFormat  YYYY-MM-DD
    title       Dimensions SCD2

    section scd2_table1
        dim1-a : done,   des1_1, 2023-01-01, 2023-01-03
        dim1-b : done,   des1_2, 2023-01-03, 2023-01-04
        dim1-c : done,   des1_3, 2023-01-04, 2023-01-07
        dim1-d : done,   des1_4, 2023-01-07, 2023-01-09
        dim1-e : active, des1_5, 2023-01-09, 2023-01-11

    section scd2_table2
        dim2-a : done,   des2_1, 2022-12-31, 2023-01-02
        dim2-b : done,   des2_2, 2023-01-02, 2023-01-04
        dim2-c : done,   des2_3, 2023-01-04, 2023-01-06
        dim2-d : active, des2_4, 2023-01-06, 2023-01-11

    section scd2_table3
        dim3-a : done,   des3_1, 2023-01-01, 2023-01-03
        dim3-b : done,   des3_2, 2023-01-05, 2023-01-07
        dim3-c : active, des3_3, 2023-01-07, 2023-01-11

    section scd2_direct_joins
        dim1-a,dim2-a,dim3-a : done, 2023-01-01, 2023-01-02
        dim1-a,dim2-b,dim3-a : done, 2023-01-02, 2023-01-05
        dim1-c,dim2-c,dim3-b : done, 2023-01-05, 2023-01-06
        dim1-c,dim2-d,dim3-b : done, 2023-01-06, 2023-01-07
        dim1-d,dim2-d,dim3-c : done, 2023-01-07, 2023-01-09
        dim1-e,dim2-d,dim3-c : active, 2023-01-09, 2023-01-11

    section scd2_unified_timeline
        dim2-a : done, 2022-12-31, 2023-01-01
        dim1-a,dim2-a,dim3-a : done, 2023-01-01, 2023-01-02
        dim1-a,dim2-b,dim3-a : done, 2023-01-02, 2023-01-03
        dim1-b,dim2-b : done, 2023-01-03, 2023-01-04
        dim1-c,dim2-c : done, 2023-01-04, 2023-01-05
        dim1-c,dim2-c,dim3-b : done, 2023-01-05, 2023-01-06
        dim1-c,dim2-d,dim3-b : done, 2023-01-06, 2023-01-07
        dim1-d,dim2-d,dim3-c : done, 2023-01-07, 2023-01-09
        dim1-e,dim2-d,dim3-c : active, 2023-01-09, 2023-01-11
```

## Generating mermaid Gantt chart sections

```sql
select
    concat_ws(',', dim1, dim2, dim3)
        || ' : '
        || concat_ws(', ',
                     if(valid_to < '2023-01-11', 'done', 'active'),
                     valid_from::date,
                     if(valid_to < '2023-01-11', valid_to::date, '2023-01-11')
           )
from unified_timeline;
```
