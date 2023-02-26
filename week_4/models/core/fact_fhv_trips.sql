{{ config(materialized='table') }}

with fhv_data as (
    select *
    from {{ ref('stg_fhv_tripdata') }}
),

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select
    dispatching_base_num,
    affiliated_base_num,
    pickup_locationid,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    dropoff_locationid,
    dropoff_zone.borough as dropoff_zone_borough,
    dropoff_zone.zone as dropoff_zone,
    pickup_datetime,
    dropoff_datetime,
    sr_flag

from fhv_data as fhv
inner join dim_zones as pickup_zone
on fhv.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv.dropoff_locationid = dropoff_zone.locationid
