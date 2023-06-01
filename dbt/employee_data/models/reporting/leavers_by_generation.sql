-- Feed pie chart of leavers by generation

{{ 
    config(
        schema="reporting"
    ) 
}}

with count_leavers as (
    select generation, count(*) as leavers
    from {{ ref('leaver_attributes') }}
    where year(exit_date) = (select max(year(exit_date)) from {{ ref('leaver_attributes') }})
    group by generation
),

total_leavers as (
    select *, sum(leavers) over () as total_leavers
    from count_leavers
)

select generation, round(div0(leavers, total_leavers)*100, 2) as generation_percentage
from total_leavers
