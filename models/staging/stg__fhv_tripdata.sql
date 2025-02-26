{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
    select *,
    row_number() over(partition by dispatching_base_num, unique_row_id, pickup_datetime) as rn,
    from {{ source('staging','fhv_tripdata') }}
    where dispatching_base_num is not null
)
select

    -- identifiers
    'fhv' as service_type,
    {{ dbt.safe_cast("unique_row_id", api.Column.translate_type("string")) }} as tripid,
    {{ dbt.safe_cast("PULocationID", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("DOLocationID", api.Column.translate_type("integer")) }} as dropoff_locationid,
    
    -- -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    extract(YEAR from cast(pickup_datetime as timestamp)) as year,
    extract(MONTH from cast(pickup_datetime as timestamp)) as month,
    
    -- -- trip info
    {{ dbt.safe_cast("dispatching_base_num", api.Column.translate_type("string")) }} as dispatch_base_id,
    {{ dbt.safe_cast("SR_Flag", api.Column.translate_type("integer")) }} as sr_flag,
    cast(Affiliated_base_number as string) as affiliated_base_number
from tripdata
where rn = 1


-- Dev limit
-- dbt build --select <model.sql> --vars '{'is_test_run: false}'
{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}