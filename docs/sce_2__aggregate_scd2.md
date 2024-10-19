# Scenario 2: Aggregate SCD2

## Problem

Suppose you have an SCD2 table that has the value of each ID over time.
Each ID has its associated group.

Write a query that returns the summed value for each group, over time.

If the table was SCD1, then it would simply look like:

```sql
select group_id, sum(val) from sce_2__table group by all;
```

### Observation

We need to re-slice each record with regard to the grouping column, and then do the aggregation.

## Setup

```sh
dbt build --select +sce_2__aggregate_scd2
```

```mermaid
gantt
    dateFormat  YYYY-MM-DD
    title       Aggregate SCD2 data

    section sce_2__table_scd2
        id1,a,(1) : done, 2023-01-01, 2023-01-03
        id1,a,(3) : done, 2023-01-03, 2023-01-04
        id1,a,(2) : done, 2023-01-04, 2023-01-07
        id1,a,(4) : done, 2023-01-07, 2023-01-09
        id1,a,(5) : active, 2023-01-09, 2023-01-11
        id2,a,(1) : done, 2023-01-02, 2023-01-05
        id2,a,(3) : done, 2023-01-05, 2023-01-06
        id2,a,(2) : active, 2023-01-06, 2023-01-11
        id3,b,(1) : done, 2023-01-03, 2023-01-05
        id3,b,(1) : active, 2023-01-05, 2023-01-11

    section sce_2__sum
        a,(1) : done, 2023-01-01, 2023-01-02
        a,(2) : done, 2023-01-02, 2023-01-03
        a,(4) : done, 2023-01-03, 2023-01-04
        a,(3) : done, 2023-01-04, 2023-01-05
        a,(5) : done, 2023-01-05, 2023-01-06
        a,(4) : done, 2023-01-06, 2023-01-07
        a,(6) : done, 2023-01-07, 2023-01-09
        a,(7) : active, 2023-01-09, 2023-01-11
        b,(1) : done, 2023-01-03, 2023-01-05
        b,(1) : active, 2023-01-05, 2023-01-11
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
