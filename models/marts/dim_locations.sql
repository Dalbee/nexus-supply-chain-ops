{{ config(materialized='table') }}

select distinct
    location_key,
    order_country,
    order_region
from {{ ref('int_shipments') }}