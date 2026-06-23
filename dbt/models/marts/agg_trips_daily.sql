/*
  Gold aggregate: one row per day. The "what happened today" summary a
  dashboard would chart. Built off dim_date so zero-trip days still appear.
*/
with daily as (
    select
        pickup_date,
        count(*)                                   as trip_count,
        sum(passenger_count)                       as total_passengers,
        round(sum(total_amount), 2)                as total_revenue,
        round(avg(fare_amount), 2)                 as avg_fare,
        round(avg(trip_distance_mi), 2)            as avg_distance_mi,
        round(avg(trip_duration_min), 2)           as avg_duration_min,
        round(avg(tip_pct), 4)                     as avg_tip_pct,
        sum(case when payment_type_id = 1 then 1 else 0 end) as card_trips,
        sum(case when payment_type_id = 2 then 1 else 0 end) as cash_trips
    from {{ ref('fct_trips') }}
    group by pickup_date
)

select
    d.date_day                                     as pickup_date,
    d.day_name,
    d.is_weekend,
    coalesce(daily.trip_count, 0)                  as trip_count,
    coalesce(daily.total_passengers, 0)            as total_passengers,
    coalesce(daily.total_revenue, 0)               as total_revenue,
    daily.avg_fare,
    daily.avg_distance_mi,
    daily.avg_duration_min,
    daily.avg_tip_pct,
    coalesce(daily.card_trips, 0)                  as card_trips,
    coalesce(daily.cash_trips, 0)                  as cash_trips
from {{ ref('dim_date') }} d
left join daily
    on d.date_day = daily.pickup_date
