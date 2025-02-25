with q as (
    select
        service_type,
        fare_amount,
        year,
        month,
        (percentile_cont(fare_amount, 0.97) OVER (PARTITION BY service_type, year, month)) as p97,
        (percentile_cont(fare_amount, 0.95) OVER (PARTITION BY service_type, year, month)) as p95,
        (percentile_cont(fare_amount, 0.90) OVER (PARTITION BY service_type, year, month)) as p90
    from {{ ref('fact_trips') }}
    where
        fare_amount > 0 and
        trip_distance > 0 and
        payment_type_description in ('Cash', 'Credit Card', 'Credit card')
)
select
    service_type,
    year,
    month,
    avg(p97) as p97,
    avg(p95) as p95,
    avg(p90) as p90,

from q
group by service_type, year, month
order by year, month, service_type

