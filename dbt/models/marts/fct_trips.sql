/*
  Gold fact table: one row per valid taxi trip (grain = trip_id).

  Incremental + partitioned by pickup_date so each run only processes newly
  landed days instead of rebuilding history. dbt-athena's insert_overwrite
  replaces whole partitions, so re-running a day is idempotent. Note: with
  Athena/Hive the partition column must be the LAST column in the select.
*/
{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partitioned_by = ['pickup_date'],
    on_schema_change = 'append_new_columns'
  )
}}

with trips as (
    select * from {{ ref('int_trips_valid') }}

    {% if is_incremental() %}
      -- Only scan partitions newer than what we've already loaded.
      where pickup_date >= (select coalesce(max(pickup_date), date '1900-01-01') from {{ this }})
    {% endif %}
)

select
    trip_id,
    -- Foreign keys into the gold dimensions.
    vendor_id,
    pu_location_id,
    do_location_id,
    payment_type as payment_type_id,
    rate_code_id,
    -- Degenerate / descriptive attributes.
    pickup_at,
    dropoff_at,
    store_and_fwd_flag,
    -- Measures.
    passenger_count,
    trip_distance_mi,
    trip_duration_min,
    avg_speed_mph,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    total_amount,
    -- Tip as a share of the pre-tip fare (null when fare is 0).
    case when fare_amount > 0 then round(tip_amount / fare_amount, 4) end as tip_pct,
    -- Partition key MUST come last for Athena/Hive insert_overwrite.
    pickup_date
from trips
