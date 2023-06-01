-- Headcount, newcomers, and leavers by month

{{ 
    config(
        materialized="incremental"
    ) 
}}

with monthly_leavers as (
    select date_trunc(month, to_date(exit_date, 'MM/DD/YY')) as exit_month, count(*) as leavers
    from {{ source('raw', 'departures') }}
    where emp_no is not null
        {% if is_incremental() %}
            AND date_trunc(month, to_date(exit_date, 'MM/DD/YY')) >= (select max(date) from {{ this }})
        {% endif %}
    group by 1
),

monthly_newcomers as (
    select date_trunc(month, to_date(hire_date, 'MM/DD/YY')) as hire_month, count(*) as newcomers
    from {{ source('raw', 'employees') }}
    where emp_no is not null
        {% if is_incremental() %}
            AND date_trunc(month, to_date(hire_date, 'MM/DD/YY')) >= (select max(date) from {{ this }})
        {% endif %}
    group by 1
),

combined as (
    select d.date_value as date,
        coalesce(mn.newcomers, 0) as newcomers,
        coalesce(ml.leavers, 0) as leavers
    from {{ ref('dates') }} d
    left join monthly_newcomers mn
    on d.date_value = mn.hire_month
    left join monthly_leavers ml
    on d.date_value = ml.exit_month
),

headcounts as (
    select *,
        newcomers - leavers as net_change,
        sum(net_change) over (order by date) as current_headcount
    from combined
)

select *,
    round(div0(newcomers - lag(newcomers) over (order by date), lag(newcomers) over (order by date)) * 100, 2) AS newcomers_percent_change,
    round(div0(leavers - lag(leavers) over (order by date), lag(leavers) over (order by date)) * 100, 2) AS leavers_percent_change,
    round(div0(current_headcount - lag(current_headcount) over (order by date), lag(current_headcount) over (order by date)) * 100, 2) AS headcount_percent_change
from headcounts
order by date desc
