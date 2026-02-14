{{ config(materialized='view') }}

with raw_data as (
    select * from {{ source('external_source', 'raw_supply_chain') }}
)

select
    -- 1. Identifiers (Cast to String for consistency)
    cast(`Order Id` as string) as order_id,
    cast(`Customer Id` as string) as customer_id,
    cast(`Product Card Id` as string) as product_id,
    cast(`Order Item Id` as string) as order_item_id,

    -- 2. Shipping Performance Logic
    cast(`Days for shipping (real)` as int) as actual_shipping_days,
    cast(`Days for shipment (scheduled)` as int) as scheduled_shipping_days,
    `Delivery Status` as delivery_status,
    cast(`Late_delivery_risk` as boolean) as is_late_risk,
    `Shipping Mode` as shipping_mode,

    -- 3. Dates (Converting strings to proper Timestamps)
    to_timestamp(`order date (DateOrders)`, 'M/d/yyyy H:mm') as order_at,
    to_timestamp(`shipping date (DateOrders)`, 'M/d/yyyy H:mm') as shipped_at,

    -- 4. Financials
    cast(`Sales` as decimal(10,2)) as sales_amount,
    cast(`Order Profit Per Order` as decimal(10,2)) as profit_amount,
    cast(`Order Item Discount` as decimal(10,2)) as discount_amount,
    cast(`Order Item Quantity` as int) as quantity,

    -- 5. Geography & Attributes
    `Customer City` as customer_city,
    `Customer State` as customer_state,
    `Order Country` as order_country,
    `Order Region` as order_region,
    `Category Name` as category_name,
    `Product Name` as product_name

from raw_data