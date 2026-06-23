-- A trip cannot start in the future. Returns offending rows (a passing test
-- returns zero rows). Catches clock/parse errors that range checks miss.
select
    trip_id,
    pickup_at
from {{ ref('fct_trips') }}
where pickup_at > current_timestamp
