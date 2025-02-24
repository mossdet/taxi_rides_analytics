select
    year,
    quarter,
    sum(total_amount) as quarterly_revenue
from {{ ref('fact_trips') }}
group by year, quarter
order by year asc, quarter asc
