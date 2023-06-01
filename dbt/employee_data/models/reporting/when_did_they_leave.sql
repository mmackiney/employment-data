-- Feeds 'when did they leave' report

{{ 
    config(
        schema="reporting"
    ) 
}}

select 
    case
        when datediff(month, hire_date, exit_date) < 12 then '<1'
        when datediff(month, hire_date, exit_date) between 12 and 23 then '1-2 yrs'
        when datediff(month, hire_date, exit_date) between 24 and 35 then '2-3 yrs'
        when datediff(month, hire_date, exit_date) between 36 and 59 then '3-5 yrs'
        else '>5'
    end as duration, count(*) as leavers
from {{ ref('leaver_attributes') }}
where year(exit_date) = (select max(year(exit_date)) from {{ ref('leaver_attributes') }})
group by duration
