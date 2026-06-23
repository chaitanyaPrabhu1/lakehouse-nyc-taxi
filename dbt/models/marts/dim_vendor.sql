/*
  Gold dimension: TPEP vendor names (TLC data dictionary seed).
*/
select
    vendor_id,
    vendor_name
from {{ ref('vendor_lookup') }}
