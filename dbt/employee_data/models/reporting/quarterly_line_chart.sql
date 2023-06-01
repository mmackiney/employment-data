-- Feeds the quarterly fluctuation report line chart

{{ 
    config(
        schema="reporting"
    ) 
}}

select newcomers, leavers
from {{ ref('quarterly_changes') }}
where year(date) = year(select max(date) from {{ ref('quarterly_changes') }})
order by date
