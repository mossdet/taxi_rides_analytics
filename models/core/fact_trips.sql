{{
    config(
        materialized='table'
    )
}}

with green_tripdata as (
    select *, 
        'Green' as service_type,
        extract(YEAR from pickup_datetime)  as year,
        extract(MONTH from pickup_datetime)  as month,
        extract(QUARTER from pickup_datetime) as quarter,  
        case
            when extract(QUARTER from pickup_datetime)=1 then concat(cast(extract(YEAR from pickup_datetime) as string), '/Q1') 
            when extract(QUARTER from pickup_datetime)=2 then concat(cast(extract(YEAR from pickup_datetime) as string), '/Q2')
            when extract(QUARTER from pickup_datetime)=3 then concat(cast(extract(YEAR from pickup_datetime) as string), '/Q3')
            when extract(QUARTER from pickup_datetime)=4 then concat(cast(extract(YEAR from pickup_datetime) as string), '/Q4')
            else 'N/A'
        end as year_quarter        
    from
        {{ ref('stg__green_tripdata') }}
    where
        extract(YEAR from pickup_datetime) >= 2019 and
        extract(YEAR from pickup_datetime) <= 2020
), 
yellow_tripdata as (
    select *, 
        'Yellow' as service_type,
        extract(YEAR from pickup_datetime) as year,
        extract(MONTH from pickup_datetime) as month,
        extract(QUARTER from pickup_datetime) as quarter,  
        case
            when extract(QUARTER from pickup_datetime)=1 then concat(cast(extract(YEAR from pickup_datetime) as string), '/Q1') 
            when extract(QUARTER from pickup_datetime)=2 then concat(cast(extract(YEAR from pickup_datetime) as string), '/Q2')
            when extract(QUARTER from pickup_datetime)=3 then concat(cast(extract(YEAR from pickup_datetime) as string), '/Q3')
            when extract(QUARTER from pickup_datetime)=4 then concat(cast(extract(YEAR from pickup_datetime) as string), '/Q4')
            else 'N/A'
        end as year_quarter        
    from
        {{ ref('stg__yellow_tripdata') }}
    where
        extract(YEAR from pickup_datetime) >= 2019 and
        extract(YEAR from pickup_datetime) <= 2020
), 
trips_unioned as (
    select * from green_tripdata
    union all 
    select * from yellow_tripdata
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select trips_unioned.tripid, 
    trips_unioned.vendorid, 
    trips_unioned.service_type,
    trips_unioned.ratecodeid, 
    trips_unioned.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    trips_unioned.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    trips_unioned.pickup_datetime, 
    trips_unioned.dropoff_datetime,
    trips_unioned.year,
    trips_unioned.month,
    trips_unioned.quarter,
    trips_unioned.year_quarter,
    trips_unioned.store_and_fwd_flag, 
    trips_unioned.passenger_count, 
    trips_unioned.trip_distance, 
    trips_unioned.trip_type, 
    trips_unioned.fare_amount, 
    trips_unioned.extra, 
    trips_unioned.mta_tax, 
    trips_unioned.tip_amount, 
    trips_unioned.tolls_amount, 
    trips_unioned.ehail_fee, 
    trips_unioned.improvement_surcharge, 
    trips_unioned.total_amount, 
    trips_unioned.payment_type, 
    trips_unioned.payment_type_description
from trips_unioned
inner join dim_zones as pickup_zone
on trips_unioned.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on trips_unioned.dropoff_locationid = dropoff_zone.locationid

-- Dev limit
-- dbt build --select fact_trips.sql --vars '{'is_test_run: false}'
{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}