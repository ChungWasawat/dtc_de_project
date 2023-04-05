{{ config(materialized='table') }}

with taxi_data as (
    select 
        tripid,
        pickup_zone as start_location,
        pickup_datetime as start_date,
        trip_duration_sec,
        'Taxi' as vehicle_service_type
    from {{ ref('fact_taxi_trips') }}
),

bike_data as (
    select 
        tripid,
        start_station_name as start_location,
        start_time as start_date,
        trip_duration_sec,
        'Bike' as vehicle_service_type
    from {{ ref('stg_bike')}}
),

trips_unioned as (
    select * from taxi_data
    union all
    select * from bike_data
)
select
    -- Grouping
    start_location,
    date_trunc(start_date, month) as recorded_month, 
    vehicle_service_type,

    -- Calculation
    count(tripid) as total_monthly_trips,
    avg(trip_duration_sec)
    
from trips_unioned
group by 1,2,3



