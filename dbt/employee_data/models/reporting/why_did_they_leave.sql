-- Feeds why did they leave section of the dashboard

{{ 
    config(
        schema="reporting"
    ) 
}}

select exit_reason, count(*) as leavers
from {{ ref('leaver_attributes') }}
where year(exit_date) = (select(max(year(exit_date))) from {{ ref('leaver_attributes') }})
group by exit_reason
