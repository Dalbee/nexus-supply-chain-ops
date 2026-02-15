{{ config(materialized='table') }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['order_country', 'order_region']) }} as location_key,
    order_country,
    order_region
from {{ ref('stg_shipments') }}