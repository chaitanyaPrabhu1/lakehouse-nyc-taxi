/*
  Silver staging for the zone lookup: typed + lightly conformed. The source
  has a couple of housekeeping rows (264 "Unknown", 265 "N/A"); we keep them so
  every trip's location_id resolves, but normalize blank service zones to NULL.
*/
with source as (
    select * from {{ source('nyc_taxi_bronze', 'taxi_zones') }}
)

select
    cast(location_id as integer)          as location_id,
    nullif(trim(borough), '')             as borough,
    nullif(trim(zone), '')                as zone,
    nullif(trim(service_zone), '')        as service_zone,
    ingested_at
from source
