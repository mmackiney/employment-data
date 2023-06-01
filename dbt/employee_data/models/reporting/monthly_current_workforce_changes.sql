-- Get most recent month's net inflow/outflow of workers

{{ 
    config(
        schema="reporting"
    ) 
}}

select net_change
from {{ ref('monthly_changes') }}
limit 1
