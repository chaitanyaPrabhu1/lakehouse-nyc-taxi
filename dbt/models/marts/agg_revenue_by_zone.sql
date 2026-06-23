/*
  Gold aggregate: revenue + demand per pickup zone, enriched with the borough/
  zone names. The classic "where does the money come from" mart.
*/
with by_zone as (
    select
        pu_location_id as zone_id,
        count(*)                            as trip_count,
        round(sum(total_amount), 2)         as total_revenue,
        round(avg(total_amount), 2)         as avg_total_amount,
        round(avg(trip_distance_mi), 2)     as avg_distance_mi,
        round(avg(tip_pct), 4)              as avg_tip_pct
    from {{ ref('fct_trips') }}
    group by pu_location_id
)

select
    z.zone_id,
    z.borough,
    z.zone_name,
    z.service_zone,
    by_zone.trip_count,
    by_zone.total_revenue,
    by_zone.avg_total_amount,
    by_zone.avg_distance_mi,
    by_zone.avg_tip_pct
from by_zone
inner join {{ ref('dim_zone') }} z
    on by_zone.zone_id = z.zone_id
