-- Feed year-to-date leavers metric

{{ 
    config(
        schema="reporting"
    ) 
}}

select sum(leavers) as year_to_date_leavers
from {{ ref('quarterly_changes') }}
where year(date) = year(select max(date) from {{ ref('quarterly_changes') }})
