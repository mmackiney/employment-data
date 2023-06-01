-- Feeds the year-to-date changes by department section of dashboard

{{ 
    config(
        schema="reporting"
    ) 
}}

select *
from {{ ref('changes_by_department_by_month') }}
where year(date_value) = (select year(max(date_value)) from {{ ref('changes_by_department_by_month') }})
order by date_value
