/*
  Gold dimension: payment-type codes (sourced from the TLC data dictionary seed).
*/
select
    payment_type as payment_type_id,
    payment_type_desc
from {{ ref('payment_type_lookup') }}
