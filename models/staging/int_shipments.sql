{{ config(materialized='view') }}

with raw_data as (
    select * from {{ source('external_source', 'raw_supply_chain') }}
),

renamed_and_cleaned as (
    select
        -- 1. IDs
        cast(`Order Item Id` as string) as shipment_item_id,
        cast(`Order Id` as string) as order_id,
        
        -- 2. Dates (Standardized to Timestamps)
        -- Format 'M/d/yyyy H:mm' ensures Spark correctly parses your raw date strings
        to_timestamp(`order date (DateOrders)`, 'M/d/yyyy H:mm') as order_at,
        to_timestamp(`shipping date (DateOrders)`, 'M/d/yyyy H:mm') as shipped_at,

        -- 3. Attributes for Dimensions
        `Product Name` as product_name,
        `Category Name` as category_name,
        `Order Country` as order_country,
        `Order Region` as order_region,
        `Delivery Status` as delivery_status,
        `Shipping Mode` as shipping_mode,
        
        -- Added Order Status to capture the logical state of the order (e.g., COMPLETE, PENDING)
        `Order Status` as order_status,

        -- 4. Measures
        cast(`Days for shipping (real)` as int) as actual_shipping_days,
        cast(`Days for shipment (scheduled)` as int) as scheduled_shipping_days,
        cast(`Sales` as decimal(10,2)) as sales_amount,
        cast(`Order Profit Per Order` as decimal(10,2)) as profit_amount
    from raw_data
)

select 
    *,
    -- Generate keys here so Gold tables can just inherit them
    {{ dbt_utils.generate_surrogate_key(['product_name', 'category_name']) }} as product_key,
    {{ dbt_utils.generate_surrogate_key(['order_country', 'order_region']) }} as location_key,
    
    -- Added order_status to the shipping_key. 
    -- This ensures our Shipping Dimension can track both physical delivery and logical order states.
    {{ dbt_utils.generate_surrogate_key(['delivery_status', 'shipping_mode', 'order_status']) }} as shipping_key
from renamed_and_cleaned