{{ config(materialized='view') }}
 
with tripdata as 
(
  select *,
    row_number() over(partition by bikeid, starttime) as rn
  from {{ source('staging','bike') }}
  where bikeid is not null 
)
select
   -- identifiers
    {{ dbt_utils.surrogate_key(['bikeid', 'starttime']) }} as tripid,
    cast(bikeid as integer) as bikeid,
    cast(start_station_id as numeric) as start_station_id,
    cast(end_station_id as numeric) as end_station_id,
    
    -- timestamps
    cast(starttime as timestamp) as start_time,
    cast(stoptime as timestamp) as stop_time,
    cast(tripduration as int) as trip_duration_sec,
    
    -- user info
    cast(usertype as string) as usertype,
    cast(birth_year as integer) as  birth_year,
    cast(gender as integer) as gender_type,
    {{ get_gender_type('gender_type') }} as gender_type_description,

    -- station info
    cast(start_station_name as string) as start_station_name,
    cast(end_station_name as string) as end_station_name,
    cast(start_station_latitude as numeric) as start_station_latitude,
    cast(start_station_longitude as numeric) as start_station_longitude,
    cast(end_station_latitude as numeric) as end_station_latitude,
    cast(end_station_longitude as numeric) as end_station_longitude,
from tripdata
where rn = 1

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}