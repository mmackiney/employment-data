-- Feeds the monthly fluctuation report line chart

{{ 
    config(
        schema="reporting"
    ) 
}}

select newcomers, leavers
from {{ ref('monthly_changes') }}
where year(date) = year(select max(date) from {{ ref('monthly_changes') }})
order by date
