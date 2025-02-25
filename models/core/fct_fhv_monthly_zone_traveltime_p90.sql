with dropoff_zones_prctl as (
    select
        pickup_zone,
        dropoff_zone,
        year,
        month,
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) as trip_duration,
        ((percentile_cont(TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND), 0.90) OVER 
        (PARTITION BY year, month, pickup_zone, dropoff_zone))) as p90,
    from 
        {{ ref('dim_fhv_trips') }}
)
select
    pickup_zone,
    dropoff_zone,
    avg(p90) as p90
from dropoff_zones_prctl
where
    --pickup_zone in ('Newark Airport', 'SoHo', 'Yorkville') and
    pickup_zone in ('Newark Airport') and
    --pickup_zone in ('SoHo') and
    --pickup_zone in ('Yorkville East' ) and
    year = 2019 and
    month = 11
group by pickup_zone, dropoff_zone
order by p90 desc
