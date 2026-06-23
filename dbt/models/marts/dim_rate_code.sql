/*
  Gold dimension: rate-code descriptions (TLC data dictionary seed).
*/
select
    rate_code_id,
    rate_code_desc
from {{ ref('rate_code_lookup') }}
