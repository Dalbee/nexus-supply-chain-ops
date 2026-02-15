{{ config(materialized='table') }}

with date_spine as (
    -- Generates a list of dates. Adjust the start/end years as needed for your data.
    select explode(sequence(to_date('2020-01-01'), to_date('2026-12-31'), interval 1 day)) as date_day
)

select
    date_day as date_key,
    year(date_day) as year,
    month(date_day) as month,
    date_format(date_day, 'MMMM') as month_name,
    quarter(date_day) as quarter,
    dayofweek(date_day) as day_of_week,
    date_format(date_day, 'EEEE') as day_name,
    case when dayofweek(date_day) in (1, 7) then true else false end as is_weekend
from date_spine