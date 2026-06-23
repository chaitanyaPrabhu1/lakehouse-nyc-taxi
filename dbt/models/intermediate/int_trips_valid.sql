/*
  Silver: the conformed, analytics-ready trip grain — only rows that passed
  every data-quality check. This is the single clean input the gold fact table
  builds on, so the strict tests on fct_trips (relationships, ranges) hold.
*/
select
    trip_id,
    vendor_id,
    pickup_at,
    dropoff_at,
    pickup_date,
    passenger_count,
    trip_distance_mi,
    trip_duration_min,
    avg_speed_mph,
    rate_code_id,
    store_and_fwd_flag,
    pu_location_id,
    do_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    total_amount,
    ingested_at
from {{ ref('stg_yellow_trips') }}
where is_valid
