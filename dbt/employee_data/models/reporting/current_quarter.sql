-- Get current quarter for dashboard

{{ 
    config(
        schema="reporting"
    ) 
}}

select max(date) as current_quarter
from {{ ref('quarterly_changes') }}
