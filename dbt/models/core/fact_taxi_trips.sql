{{ config(materialized='table') }}

with yellow_data as (
    select *,
        'Yellow' as service_type
    from {{ ref('stg_taxi_yellow') }}
),

trips_yellow as (
    select * from yellow_data
),

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select
    trips_yellow.tripid,
    trips_yellow.vendorid,
    trips_yellow.service_type,
    trips_yellow.ratecodeid,
    trips_yellow.pickup_locationid,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    trips_yellow.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    trips_yellow.pickup_datetime,
    trips_yellow.dropoff_datetime,
    {{ dbt.datediff("trips_yellow.pickup_datetime", "trips_yellow.dropoff_datetime", "second") }} as trip_duration_sec,
    trips_yellow.store_and_fwd_flag,
    trips_yellow.passenger_count,
    trips_yellow.trip_distance,
    trips_yellow.trip_type,
    trips_yellow.fare_amount,
    trips_yellow.extra,
    trips_yellow.mta_tax,
    trips_yellow.tip_amount,
    trips_yellow.tolls_amount,
    trips_yellow.ehail_fee,
    trips_yellow.improvement_surcharge,
    trips_yellow.total_amount,
    trips_yellow.payment_type,
    trips_yellow.payment_type_description,
    trips_yellow.congestion_surcharge
from trips_yellow
inner join dim_zones as pickup_zone
on trips_yellow.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on trips_yellow.dropoff_locationid = dropoff_zone.locationid