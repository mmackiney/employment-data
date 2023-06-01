-- Table of dates to make month-over-month queries easier

{{ 
    config(
        materialized="ephemeral"
    ) 
}}

with recursive date_range as (
    select
        (select date_trunc(month, min(to_date(hire_date, 'MM/DD/YY'))) 
            from {{ source('raw', 'employees') }}) as date_value, 
    greatest(
        (select date_trunc(month, max(to_date(hire_date, 'MM/DD/YY'))) 
            from {{ source('raw', 'employees') }}),
        (select date_trunc(month, max(to_date(exit_date, 'MM/DD/YY'))) 
            from {{ source('raw', 'departures') }})
    ) as end_date
    union all
    select dateadd(month, 1, date_value), end_date
    from date_range
    where date_value < end_date
)

select date_value
from date_range
