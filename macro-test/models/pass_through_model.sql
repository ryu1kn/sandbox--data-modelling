{{ config(materialized='ephemeral') }}

select * from {{ source('src_fixture', 'data') }}
