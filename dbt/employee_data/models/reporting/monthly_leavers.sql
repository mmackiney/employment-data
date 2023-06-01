-- Monthly leavers and percent change from previous month

{{ 
    config(
        schema="reporting"
    ) 
}}

select leavers, leavers_percent_change
from {{ ref('monthly_changes') }}
limit 1
