{{ config(materialized='table') }}

select distinct
    product_key,
    product_name,
    category_name
from {{ ref('int_shipments') }}