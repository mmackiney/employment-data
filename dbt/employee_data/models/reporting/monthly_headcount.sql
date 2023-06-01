-- Get this month's total headcount and percent change from previous month

{{ 
    config(
        schema="reporting"
    ) 
}}

select current_headcount, headcount_percent_change
from {{ ref('monthly_changes') }}
limit 1
