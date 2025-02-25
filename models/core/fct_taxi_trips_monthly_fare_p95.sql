with q as (
    select
        service_type,
        fare_amount,
        year,
        month,
        PERCENTILE_CONT(fare_amount, 0.97) OVER (PARTITION BY service_type, year, month) as prctl_fare
    from {{ ref('fact_trips') }}
    where
        --service_type='Yellow' and
        year >= 2020 and
        fare_amount > 0 and
        trip_distance > 0 and
        payment_type_description in ('Cash', 'Credit Card', 'Credit card')
)
select
    service_type,
    year,
    month,
    avg(q.prctl_fare) as d_prctl_fare
from q
group by service_type, year, month
order by service_type, year,month

