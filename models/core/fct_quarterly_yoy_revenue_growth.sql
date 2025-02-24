with quarterly_revenue as (
    select 
    * 
    from {{ ref('fct_taxi_trips_quarterly_revenue') }}
    where service_type = 'Green'
),
previous_year_revenue AS (
    SELECT
        year,
        quarter,
        total_revenue,
        -- Get the revenue from the same quarter in the previous year
        LAG(total_revenue) OVER (PARTITION BY quarter ORDER BY year) AS prev_year_revenue
    FROM
        quarterly_revenue
)

SELECT
    year,
    quarter,
    total_revenue,
    prev_year_revenue,
    -- Calculate the YoY growth
    ((total_revenue - prev_year_revenue) / prev_year_revenue) * 100 AS yoy_growth_percentage
FROM
    previous_year_revenue
WHERE
    prev_year_revenue IS NOT NULL
ORDER BY
    year, quarter
