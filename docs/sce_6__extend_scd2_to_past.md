# Scenario 6: Extend an SCD2 to the past

## Problem

You have an SCD2 table that holds the history from 2 Jan. Its granularity of recognising
a change is daily.

Now you are given the table state on 1 Jan, and want to include it in the SCD2 table
to make the SCD2 to have history from 1 Jan.

## Setup

```sh
dbt build --select +path:models/sce_6__extend_scd2_to_past
```

```mermaid
gantt
    dateFormat  YYYY-MM-DD
    title       Aggregate SCD2 data (with one-stop macro)

    section sce_6__employees_scd1
        1, Engineering, Software Engineer : done, 2023-01-01, 2023-01-02
        2, Marketing, Analyst : done, 2023-01-01, 2023-01-02

    section sce_6__employees_scd2
        1, Engineering, Software Engineer : done, 2023-01-02, 2023-01-04
        1, Engineering, Data Engineer : done, 2023-01-04, 2023-01-11
        2, Marketing, Marketing Manager : done, 2023-01-03, 2023-01-11

    section sce_6__extend_by_one_day
        1, Engineering, Software Engineer : done, 2023-01-01, 2023-01-04
        1, Engineering, Data Engineer : done, 2023-01-04, 2023-01-11
        2, Marketing, Analyst : done, 2023-01-01, 2023-01-02
        2, Marketing, Marketing Manager : done, 2023-01-03, 2023-01-11
```

## Generating mermaid Gantt chart sections

```sh
dbt run-operation op_render_mermaid_gantt \
    --args "{relation: sce_6__extend_by_one_day, columns: [employee_id, department, role], order_by: [employee_id, valid_from]}"
```
