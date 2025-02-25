select
    extract(SECOND from dropoff_datetime - pickup_datetime) as trip_duration,
    (percentile_cont(extract(SECOND from dropoff_datetime - pickup_datetime) , 0.97) OVER 
        (PARTITION BY year, month, pickup_locationid, dropoff_locationid)) as p97,
from {{ ref('dim_fhv_trips') }}
