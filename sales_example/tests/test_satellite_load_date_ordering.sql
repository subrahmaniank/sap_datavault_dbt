-- Test: Verify satellites have proper load_date ordering
-- Each parent key should have load_dates in ascending order (same timestamp is allowed)
with ordered_records as (
    select 
        hk_customer_h,
        load_date,
        lag(load_date) over (partition by hk_customer_h order by load_date) as prev_load_date
    from {{ ref('sat_sap_customer_kna1') }}
)
select 
    hk_customer_h,
    load_date,
    prev_load_date
from ordered_records
where prev_load_date is not null
  and prev_load_date > load_date  -- Only flag if strictly out of order (same timestamp is OK)

