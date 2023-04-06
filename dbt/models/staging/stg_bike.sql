{{ config(materialized='view') }}
 
with tripdata as 
(
  select *,
    row_number() over(partition by bikeid, starttime) as rn
  from {{ source('staging','external_bike') }}
  where bikeid is not null 
)
select
   -- identifiers
    {{ dbt_utils.surrogate_key(['bikeid', 'starttime']) }} as tripid,
    cast(bikeid as string) as bikeid,
    cast(start_station_name as string) as start_station_name,
    cast(end_station_name as string) as end_station_name,
    
    -- timestamps
    cast(starttime as timestamp) as start_time,
    cast(stoptime as timestamp) as stop_time,
    cast(tripduration as int) as trip_duration_sec,

from tripdata
where rn = 1

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}