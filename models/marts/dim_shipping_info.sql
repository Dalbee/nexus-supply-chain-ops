{{ config(materialized='table') }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['delivery_status', 'shipping_mode']) }} as shipping_key,
    delivery_status,
    shipping_mode
from {{ ref('stg_shipments') }}