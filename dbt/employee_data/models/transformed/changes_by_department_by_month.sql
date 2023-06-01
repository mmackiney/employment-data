-- Feeds the leavers by department by month section of the dashboard

{{ 
    config(
        materialized="incremental"
    ) 
}}

with departments_by_month as (
    select date_value, dept_name
    from {{ ref('dates') }}
    cross join {{ source('raw', 'departments') }}
),

newcomers_by_department_by_month as (
    select date_trunc(month, to_date(e.hire_date, 'MM/DD/YY')) as hire_month, d.dept_name as dept_name, count(*) as newcomers
    from {{ source('raw', 'employees') }} e
    inner join {{ source('raw', 'dept_emp') }} de
    on e.emp_no = de.emp_no
    inner join {{ source('raw', 'departments') }} d
    on de.dept_no = d.dept_no
    group by hire_month, dept_name
),

leavers_by_department_by_month as (
    select date_trunc(month, to_date(e.exit_date, 'MM/DD/YY')) as exit_month, d.dept_name as dept_name, count(*) as leavers
    from {{ source('raw', 'departures') }} e
    inner join {{ source('raw', 'dept_emp') }} de
    on e.emp_no = de.emp_no
    inner join {{ source('raw', 'departments') }} d
    on de.dept_no = d.dept_no
    group by exit_month, dept_name
),

combined as (
    select d.date_value, d.dept_name, ndm.newcomers as newcomers, ldm.leavers as leavers
    from departments_by_month d
    left join newcomers_by_department_by_month ndm
    on d.date_value = ndm.hire_month and d.dept_name = ndm.dept_name
    left join leavers_by_department_by_month ldm
    on d.date_value = ldm.exit_month and d.dept_name = ldm.dept_name
        {% if is_incremental() %}
            WHERE date_value >= (select max(date_value) from {{ this }})
        {% endif %}
)

select *
from combined
order by dept_name, date_value
