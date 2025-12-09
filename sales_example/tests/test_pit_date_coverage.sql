-- Test: Verify PIT table has proper date coverage
-- Should have at least one record per customer per day in the date range
with date_range as (
    select 
        min(as_of_date) as min_date,
        max(as_of_date) as max_date
    from {{ ref('pit_customer_daily') }}
),
expected_dates as (
    select 
        dateadd(day, row_number() over (order by null) - 1, dr.min_date) as as_of_date
    from date_range dr
    cross join table(generator(rowcount => 10000))
    qualify as_of_date <= dr.max_date
),
customers as (
    select distinct hk_customer_h
    from {{ ref('hub_customer') }}
),
expected_combinations as (
    select 
        ed.as_of_date,
        c.hk_customer_h
    from expected_dates ed
    cross join customers c
)
select 
    ec.as_of_date,
    ec.hk_customer_h
from expected_combinations ec
left join {{ ref('pit_customer_daily') }} pit 
    on ec.as_of_date = pit.as_of_date 
    and ec.hk_customer_h = pit.hk_customer_h
where pit.as_of_date is null
limit 100  -- Limit to avoid too many results

