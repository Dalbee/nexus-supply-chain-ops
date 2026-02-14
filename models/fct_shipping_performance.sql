{{ config(materialized='table') }}

with shipments as (
    select * from {{ ref('stg_shipments') }}
)

select
    order_id,
    customer_id,
    order_at,
    shipped_at,
    -- Use the NEW names from your stg_shipments model:
    actual_shipping_days,
    scheduled_shipping_days,
    
    -- Business Logic: Calculate the delay
    (actual_shipping_days - scheduled_shipping_days) as delay_delta,
    
    -- Business Logic: Flag late deliveries
    case 
        when actual_shipping_days > scheduled_shipping_days then true 
        else false 
    end as is_late,

    delivery_status,
    shipping_mode,
    sales_amount,
    profit_amount,
    order_country,
    category_name,
    product_name

from shipments