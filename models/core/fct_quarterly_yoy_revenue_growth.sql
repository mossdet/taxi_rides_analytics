with revenue_data_g as (
    select 
    * 
    from {{ ref('fct_taxi_trips_quarterly_revenue') }}
    where service_type = 'Green'
),
prev_year_revenue_data_g as (
    select
        year,
        quarter,
        total_revenue,
        -- Get the revenue from the same quarter in the previous year
        LAG(total_revenue) over (partition by quarter order by year) as prev_year_revenue
    from
        revenue_data_g
),
yoy_revenue_data_g as (
    select
        'Green' as service_type,
        year,
        quarter,
        total_revenue,
        prev_year_revenue,
        -- Calculate the YoY growth
        ((total_revenue - prev_year_revenue) / prev_year_revenue) * 100 as yoy_growth_percentage
    from
        prev_year_revenue_data_g
    where
        prev_year_revenue IS NOT NULL
    order by
        year, quarter
),
revenue_data_y as (
    select 
    * 
    from {{ ref('fct_taxi_trips_quarterly_revenue') }}
    where service_type = 'Yellow'
),
prev_year_revenue_data_y as (
    select
        year,
        quarter,
        total_revenue,
        -- Get the revenue from the same quarter in the previous year
        LAG(total_revenue) over (partition by quarter order by year) as prev_year_revenue
    from
        revenue_data_y
),
yoy_revenue_data_y as (
    select
        'Yellow' as service_type,
        year,
        quarter,
        total_revenue,
        prev_year_revenue,
        -- Calculate the YoY growth
        ((total_revenue - prev_year_revenue) / prev_year_revenue) * 100 as yoy_growth_percentage
    from
        prev_year_revenue_data_y
    where
        prev_year_revenue IS NOT NULL
    order by
        year, quarter
),
yoy_revenue_unioned as (
    select * from yoy_revenue_data_g
    union all 
    select * from yoy_revenue_data_y
)

select 
    *
from yoy_revenue_unioned
order by service_type asc, yoy_growth_percentage desc