/*
  Gold dimension: one row per TLC taxi zone (the location dimension that both
  pickup and dropoff foreign keys resolve to).
*/
select
    location_id as zone_id,
    borough,
    zone        as zone_name,
    service_zone
from {{ ref('stg_taxi_zones') }}
