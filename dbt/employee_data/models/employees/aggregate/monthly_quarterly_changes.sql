-- Headcount, newcomers, and leavers by month

{{ 
    config(
        materialized="incremental"
    ) 
}}

with monthly_leavers as (
    select date_trunc(month, to_date(exit_date, 'MM/DD/YY')) as exit_month, count(*) as leavers
    from {{ source('employee_data', 'departures') }}
    where emp_no is not null
        {% if is_incremental() %}
            AND date_trunc(month, to_date(exit_date, 'MM/DD/YY')) >= select max(exit_month) from {{ this }}
        {% endif %}
    group by 1
),

monthly_newcomers as (
    select date_trunc(month, to_date(hire_date, 'MM/DD/YY')) as hire_month, count(*) as newcomers
    from {{ source('employee_data', 'employees') }}
    where emp_no is not null
        {% if is_incremental() %}
            AND date_trunc(month, to_date(hire_date, 'MM/DD/YY')) >= select max(hire_month) from {{ this }}
        {% endif %}
    group by 1
),

combined as (
    select d.date_value as date,
        case
            when month(date) between 1 and 3 then 1
            when month(date) between 4 and 6 then 2
            when month(date) between 7 and 9 then 3
            else 4
        end as quarter,
        coalesce(mn.newcomers, 0) as newcomers,
        coalesce(ml.leavers, 0) as leavers
    from {{ ref('dates') }} d
    left join monthly_newcomers mn
    on d.date_value = mn.hire_month
    left join monthly_leavers ml
    on d.date_value = ml.exit_month
)

select *,
    newcomers - leavers as net_change,
    round(div0(newcomers - lag(newcomers) over (order by date), lag(newcomers) over (order by date)) * 100, 2) AS newcomers_percent_change,
    round(div0(leavers - lag(leavers) over (order by date), lag(leavers) over (order by date)) * 100, 2) AS leavers_percent_change
from combined
order by date desc
