-- total_amount should equal the sum of its components (within a cent of
-- rounding). TLC data is famously imperfect here, so this is a WARN, not a hard
-- failure — it surfaces drift without blocking the build. Returns offenders.
{{ config(severity = 'warn') }}

select
    trip_id,
    total_amount,
    (
        coalesce(fare_amount, 0)
        + coalesce(extra, 0)
        + coalesce(mta_tax, 0)
        + coalesce(tip_amount, 0)
        + coalesce(tolls_amount, 0)
        + coalesce(improvement_surcharge, 0)
        + coalesce(congestion_surcharge, 0)
        + coalesce(airport_fee, 0)
    ) as components_sum
from {{ ref('fct_trips') }}
where abs(
    total_amount - (
        coalesce(fare_amount, 0)
        + coalesce(extra, 0)
        + coalesce(mta_tax, 0)
        + coalesce(tip_amount, 0)
        + coalesce(tolls_amount, 0)
        + coalesce(improvement_surcharge, 0)
        + coalesce(congestion_surcharge, 0)
        + coalesce(airport_fee, 0)
    )
) > 0.02
