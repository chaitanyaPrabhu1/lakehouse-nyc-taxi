/*
  Silver quarantine: trips that failed at least one quality check, with the
  reason(s) attached. Modeling bad data as a first-class, queryable table (vs
  silently dropping it) is the data-quality signal DE teams screen for — you can
  trend `quality_reason` over time to catch upstream regressions.
*/
select
    trip_id,
    quality_reason,
    pickup_at,
    dropoff_at,
    trip_duration_min,
    trip_distance_mi,
    passenger_count,
    pu_location_id,
    do_location_id,
    payment_type,
    rate_code_id,
    vendor_id,
    fare_amount,
    total_amount,
    ingested_at
from {{ ref('stg_yellow_trips') }}
where not is_valid
