-- Leavers with their employment duration and generation

{{ 
    config(
        materialized="incremental"
    ) 
}}

select e.emp_no as emp_no, to_date(e.birth_date, 'MM/DD/YY') - interval '100 years' as birth_date,
    case
        when year((to_date(e.birth_date, 'MM/DD/YY')) - interval '100 years') between 1922 and 1927 then 'WWII'
        when year((to_date(e.birth_date, 'MM/DD/YY')) - interval '100 years') between 1928 and 1945 then 'Post War'
        when year((to_date(e.birth_date, 'MM/DD/YY')) - interval '100 years') between 1946 and 1964 then 'Boomers'
        when year((to_date(e.birth_date, 'MM/DD/YY')) - interval '100 years') between 1965 and 1980 then 'Generation X'
        when year((to_date(e.birth_date, 'MM/DD/YY')) - interval '100 years') between 1981 and 1996 then 'Generation Y/Millennials'
        when year((to_date(e.birth_date, 'MM/DD/YY')) - interval '100 years') between 1997 and 2012 then 'Generation Z'
    end as generation,
    to_date(e.hire_date, 'MM/DD/YY') as hire_date,
    to_date(d.exit_date, 'MM/DD/YY') as exit_date,
    d.exit_reason as exit_reason
from {{ source('raw', 'employees') }} e
inner join {{ source('raw', 'departures') }} d
on e.emp_no = d.emp_no
        {% if is_incremental() %}
            where date_trunc(month, to_date(hire_date, 'MM/DD/YY')) >= (select max(hire_date) from {{ this }})
            or date_trunc(month, to_date(exit_date, 'MM/DD/YY')) >= (select max(exit_date) from {{ this }})
        {% endif %}
