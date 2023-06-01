-- Get this quarter's total headcount and percent change from previous quarter

{{ 
    config(
        schema="reporting"
    ) 
}}

select current_headcount, headcount_percent_change
from {{ ref('quarterly_changes') }}
limit 1
