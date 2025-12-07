-- models/business_vault/pit/pit_customer_daily.sql
{{ config(
    materialized = 'table',
    alias = 'pit_customer_daily',
    tags = ['business_vault', 'pit', 'customer', 'as_of'],
    post_hook = [
        "{{ log('PIT_CUSTOMER_DAILY – daily snapshots created', info=True) }}"
    ]
) }}

with date_range as (
    -- Get min and max load dates from satellite
    select
        min(load_date::date) as min_load_date,
        current_date() as max_load_date
    from {{ ref('sat_sap_customer_kna1') }}
),

daily_dates as (
    -- Generate daily grain from first to last load date using generator
    select
        dateadd(day, row_number() over (order by null) - 1, dr.min_load_date) as as_of_date
    from date_range dr
    cross join table(generator(rowcount => 10000))  -- Generate enough rows for ~27 years
    qualify as_of_date <= dr.max_load_date
),

customer_versions as (
    -- All active + historical customer versions from satellite
    select
        hk_customer_h,
        customer_name,
        city,
        country_code,
        region_code,
        postal_code,
        street_address,
        phone_number,
        customer_account_group,
        hashdiff,
        record_source,
        load_date,
        effective_from,
        effective_to,
        is_current
    from {{ ref('sat_sap_customer_kna1') }}
),

-- Core PIT logic: join daily dates to active customer versions on that date
pit_customer as (
    select
        d.as_of_date,
        c.hk_customer_h,
        c.customer_name,
        c.city,
        c.country_code,
        c.region_code,
        c.postal_code,
        c.street_address,
        c.phone_number,
        c.customer_account_group,
        c.hashdiff,
        c.record_source,
        c.load_date as pit_load_date,
        c.effective_from,
        c.effective_to,
        c.is_current,

        -- Derived business fields
        upper(trim(coalesce(c.country_code, 'Unknown'))) as country_name,
        coalesce(c.postal_code, 'N/A') as formatted_postal,
        case
            when c.phone_number is not null and length(c.phone_number) >= 10
            then c.phone_number
            else null
        end as validated_phone,

        -- Technical metadata
        current_timestamp()::timestamp_ntz as pit_created_date

    from daily_dates d
    left join customer_versions c
        on c.effective_from <= d.as_of_date
       and c.effective_to >= d.as_of_date
    where c.hk_customer_h is not null  -- only customers that existed on this date
)

select * from pit_customer
order by as_of_date desc, hk_customer_h

-- Optional: add a simple test to ensure we have daily coverage
-- Only run this check if the table already exists (skip on first build)
{% if execute and adapter.get_relation(this.database, this.schema, this.identifier) is not none %}
    {% set daily_count_query %}
        select count(distinct as_of_date) as cnt
        from {{ this }}
        where as_of_date >= dateadd(month, -3, current_date())
    {% endset %}
    {% set results = run_query(daily_count_query) %}
    {% if results %}
        {% set daily_count = results.columns[0][0] %}
        {% if daily_count < 90 %}
            {{ log('WARNING: PIT_CUSTOMER_DAILY has only ' ~ daily_count ~ ' days in last 90 days', info=True) }}
        {% endif %}
    {% endif %}
{% endif %}
