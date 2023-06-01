-- Get current month for dashboard

{{ 
    config(
        schema="reporting"
    ) 
}}

select max(date) as current_month
from {{ ref('monthly_changes') }}
