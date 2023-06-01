-- Headcount, newcomers, and leavers by quarter

{{ 
    config(
        materialized="incremental"
    ) 
}}

with quarters as (
    select *,
        case
            when month(date) between 1 and 3 then 1
            when month(date) between 4 and 6 then 2
            when month(date) between 7 and 9 then 3
            when month(date) between 10 and 12 then 4
        end as quarter
    from {{ ref('monthly_changes') }}
),

changes_by_quarter as (
    select min(date) as date, sum(newcomers) as newcomers, sum(leavers) as leavers, sum(net_change) as net_change
    from quarters
    group by year(date), quarter
    order by date desc
),

headcounts as (
    select *,
        sum(net_change) over (order by date) as current_headcount
    from changes_by_quarter
)

select *,
    round(div0(newcomers - lag(newcomers) over (order by date), lag(newcomers) over (order by date)) * 100, 2) AS newcomers_percent_change,
    round(div0(leavers - lag(leavers) over (order by date), lag(leavers) over (order by date)) * 100, 2) AS leavers_percent_change,
    round(div0(current_headcount - lag(current_headcount) over (order by date), lag(current_headcount) over (order by date)) * 100, 2) AS headcount_percent_change
from headcounts
order by date desc
