with yellow_quart_revenue as (
    select
    'Yellow' as service_type,
    year,
    quarter,
    sum(total_amount) as total_revenue
    from {{ ref('fact_trips') }}
    where
        service_type = 'Yellow'
    group by year, quarter
    order by year asc, quarter asc
),
green_quart_revenue as (
    select
    'Green' as service_type,
    year,
    quarter,
    sum(total_amount) as total_revenue
    from {{ ref('fact_trips') }}
    where
        service_type = 'Green'
    group by year, quarter
    order by year asc, quarter asc
),
revenues_unioned as (
    select * from green_quart_revenue
    union all 
    select * from yellow_quart_revenue
)
select * 
from revenues_unioned
order by service_type, year asc, quarter asc