{{ config(materialized='table') }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['product_name', 'category_name']) }} as product_key,
    product_name,
    category_name
from {{ ref('stg_shipments') }}