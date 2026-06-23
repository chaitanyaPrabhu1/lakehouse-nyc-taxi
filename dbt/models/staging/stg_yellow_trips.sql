/*
  Silver staging: one typed, de-duplicated, quality-flagged row per trip.

  TLC trip records have no natural primary key, so we synthesize a stable
  surrogate (`trip_id`) from the immutable trip attributes and keep only the
  first copy of each. We DON'T drop bad rows here — staging flags them with
  boolean checks + a human-readable `quality_reason`; the intermediate layer
  splits valid trips from quarantined ones. This keeps the messy source fully
  inspectable.
*/
with source as (
    select * from {{ source('nyc_taxi_bronze', 'yellow_trips') }}
),

typed as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'vendor_id', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
            'pu_location_id', 'do_location_id', 'total_amount', 'trip_distance'
        ]) }}                                          as trip_id,
        cast(vendor_id as integer)                     as vendor_id,
        cast(tpep_pickup_datetime as timestamp)        as pickup_at,
        cast(tpep_dropoff_datetime as timestamp)       as dropoff_at,
        date(cast(tpep_pickup_datetime as timestamp))  as pickup_date,
        cast(passenger_count as integer)               as passenger_count,
        cast(trip_distance as double)                  as trip_distance_mi,
        cast(rate_code_id as integer)                  as rate_code_id,
        nullif(trim(store_and_fwd_flag), '')           as store_and_fwd_flag,
        cast(pu_location_id as integer)                as pu_location_id,
        cast(do_location_id as integer)                as do_location_id,
        cast(payment_type as integer)                  as payment_type,
        cast(fare_amount as double)                    as fare_amount,
        cast(extra as double)                          as extra,
        cast(mta_tax as double)                        as mta_tax,
        cast(tip_amount as double)                     as tip_amount,
        cast(tolls_amount as double)                   as tolls_amount,
        cast(improvement_surcharge as double)          as improvement_surcharge,
        cast(congestion_surcharge as double)           as congestion_surcharge,
        cast(airport_fee as double)                    as airport_fee,
        cast(total_amount as double)                   as total_amount,
        cast(ingested_at as timestamp)                 as ingested_at,
        {{ trip_duration_minutes('cast(tpep_pickup_datetime as timestamp)',
                                 'cast(tpep_dropoff_datetime as timestamp)') }} as trip_duration_min
    from source
),

flagged as (
    select
        *,
        -- Average speed (mph); null when duration is non-positive.
        case
            when trip_duration_min > 0 then trip_distance_mi / (trip_duration_min / 60.0)
        end as avg_speed_mph,

        -- Data-quality checks. Each is a hard gate for the gold fact table.
        (pickup_at is not null
            and dropoff_at is not null
            and dropoff_at > pickup_at
            and trip_duration_min between 0 and {{ var('max_trip_duration_min') }}
        )                                              as chk_timestamps,
        (trip_distance_mi > 0
            and trip_distance_mi < {{ var('max_trip_distance_mi') }}
        )                                              as chk_distance,
        (fare_amount >= 0 and total_amount > 0)        as chk_amounts,
        (passenger_count between 1 and {{ var('max_passenger_count') }}
        )                                              as chk_passengers,
        (pu_location_id between 1 and 265
            and do_location_id between 1 and 265
        )                                              as chk_zones,
        (payment_type between 1 and 6)                 as chk_payment_type,
        (rate_code_id in (1, 2, 3, 4, 5, 6, 99))       as chk_rate_code,
        (vendor_id in (1, 2, 6, 7))                    as chk_vendor
    from typed
),

deduped as (
    select
        *,
        row_number() over (
            partition by trip_id
            order by ingested_at desc
        ) as _row_num
    from flagged
)

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
    ingested_at,
    -- Overall validity + a comma-separated reason for anything that failed.
    (chk_timestamps and chk_distance and chk_amounts and chk_passengers
        and chk_zones and chk_payment_type and chk_rate_code and chk_vendor
    ) as is_valid,
    nullif(concat_ws(', ',
        case when not chk_timestamps   then 'bad_timestamps'   end,
        case when not chk_distance     then 'bad_distance'     end,
        case when not chk_amounts      then 'bad_amounts'      end,
        case when not chk_passengers   then 'bad_passengers'   end,
        case when not chk_zones        then 'unknown_zone'     end,
        case when not chk_payment_type then 'bad_payment_type' end,
        case when not chk_rate_code    then 'bad_rate_code'    end,
        case when not chk_vendor       then 'bad_vendor'       end
    ), '') as quality_reason
from deduped
where _row_num = 1
