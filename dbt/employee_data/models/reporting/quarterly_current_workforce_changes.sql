-- Get most recent quarter's net inflow/outflow of workers

{{ 
    config(
        schema="reporting"
    ) 
}}

select net_change
from {{ ref('quarterly_changes') }}
limit 1
