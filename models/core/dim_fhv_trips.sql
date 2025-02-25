{{
    config(
        materialized='table'
    )
}}

with fhv_tripdata as (
    select 
        'fhv' as service_type,
        extract(YEAR from pickup_datetime) as year,
        extract(MONTH from pickup_datetime) as month,
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
select * from fhv_tripdata 
inner join dim_zones on fhv_tripdata.pickup_locationid = dim_zones.locationid