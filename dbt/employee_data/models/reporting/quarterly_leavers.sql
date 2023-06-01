-- Quarterly leavers and percent change from previous quarter

{{ 
    config(
        schema="reporting"
    ) 
}}

select leavers, leavers_percent_change
from {{ ref('quarterly_changes') }}
limit 1
