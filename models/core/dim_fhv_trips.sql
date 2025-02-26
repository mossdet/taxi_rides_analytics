{{
    config(
        materialized='table'
    )
}}

with fhv_tripdata as (
    select 
        service_type,
        year,
        month,
        tripid, 
        dispatch_base_id, 
        pickup_locationid,
        dropoff_locationid, 
        pickup_datetime, 
        dropoff_datetime,
        sr_flag, 
        affiliated_base_number
    from {{ ref('stg__fhv_tripdata') }}
),  
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select
    service_type,
    year,
    month,
    tripid, 
    dispatch_base_id, 
    pickup_locationid,
    dropoff_locationid, 
    pickup_datetime, 
    dropoff_datetime,
    sr_flag, 
    affiliated_base_number,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone
from fhv_tripdata 
inner join dim_zones as pickup_zone on fhv_tripdata.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone on fhv_tripdata.dropoff_locationid = dropoff_zone.locationid