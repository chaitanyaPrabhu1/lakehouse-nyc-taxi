/*
  Gold date dimension: a contiguous daily calendar covering the ingested range
  (set by the `date_spine_start` / `date_spine_end` vars). A real date spine —
  rather than relying on whatever dates happen to appear in the facts — means
  days with zero trips still show up in time-series reports.
*/
with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('" ~ var('date_spine_start') ~ "' as date)",
        end_date="cast('" ~ var('date_spine_end') ~ "' as date)"
    ) }}
)

select
    cast(date_day as date)                      as date_day,
    year(date_day)                              as year,
    quarter(date_day)                           as quarter,
    month(date_day)                             as month,
    day_of_month(date_day)                      as day_of_month,
    day_of_week(date_day)                       as day_of_week,   -- 1=Mon ... 7=Sun
    date_format(date_day, '%W')                 as day_name,
    (day_of_week(date_day) in (6, 7))           as is_weekend
from spine
