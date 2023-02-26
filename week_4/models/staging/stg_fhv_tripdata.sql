{{ config(materialized='view') }}

select
-- identifiers
    cast(dispatching_base_num as string) as dispatching_base_num,
    cast(Affiliated_base_number as string) as affiliated_base_num,
    cast(PUlocationID as integer) as pickup_locationid,
    cast(DOlocationID as integer) as dropoff_locationid,

-- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,

-- trip info
    cast(SR_Flag as integer) as sr_flag

from {{ source('production', 'fhv_tripdata') }}