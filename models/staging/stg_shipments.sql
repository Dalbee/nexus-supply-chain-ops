{{ config(materialized='view') }}

-- CTE: raw_data acts as an abstraction layer between the source and our transformations.
-- This follows dbt best practices: Import once, use multiple times.
with raw_data as (
    select * from {{ source('external_source', 'raw_supply_chain') }}
)

select
    /* 1. IDENTIFIERS & UNIQUE KEYS 
       Grain of this table: One row per Order Item.
       - order_item_id: This is our PRIMARY KEY. It is unique even when order_id repeats.
       - customer_id & product_id: Standardized to strings to prevent join errors 
         between different data types in Databricks.
    */
    cast(`Order Item Id` as string) as shipment_item_id, -- Our new Primary Key for tests
    cast(`Order Id` as string) as order_id,
    cast(`Customer Id` as string) as customer_id,
    cast(`Product Card Id` as string) as product_id,

    /* 2. SHIPPING PERFORMANCE LOGIC
       We transform raw numbers into actionable shipping metrics.
       - actual vs scheduled: Used in the Gold layer to calculate 'delay_delta'.
       - delivery_status & mode: Essential for dimensional analysis (Star Schema).
       - is_late_risk: Converted to Boolean (True/False) for faster Power BI filtering.
    */
    cast(`Days for shipping (real)` as int) as actual_shipping_days,
    cast(`Days for shipment (scheduled)` as int) as scheduled_shipping_days,
    `Delivery Status` as delivery_status,
    cast(`Late_delivery_risk` as boolean) as is_late_risk,
    `Shipping Mode` as shipping_mode,

    /* 3. TEMPORAL ATTRIBUTES (DATES)
       Crucial step: Converting raw strings into high-precision Timestamps.
       Using 'M/d/yyyy H:mm' format to match the Databricks/Spark SQL standard.
       This allows us to perform Date Math (like date_diff) later.
    */
    to_timestamp(`order date (DateOrders)`, 'M/d/yyyy H:mm') as order_at,
    to_timestamp(`shipping date (DateOrders)`, 'M/d/yyyy H:mm') as shipped_at,

    /* 4. FINANCIALS & QUANTITIES
       - Decimal(10,2): We cast to a specific precision to ensure currency values 
         are accurate and don't suffer from floating-point rounding errors.
       - Sales & Profit: These will be our central 'Measures' in the Fact table.
    */
    cast(`Sales` as decimal(10,2)) as sales_amount,
    cast(`Order Profit Per Order` as decimal(10,2)) as profit_amount,
    cast(`Order Item Discount` as decimal(10,2)) as discount_amount,
    cast(`Order Item Quantity` as int) as quantity,

    /* 5. GEOGRAPHY & CATEGORIZATION
       These columns are extracted here and will be 'normalized' into Dimension 
       tables (dim_locations, dim_products) to build the Star Schema.
    */
    `Customer City` as customer_city,
    `Customer State` as customer_state,
    `Order Country` as order_country,
    `Order Region` as order_region,
    `Category Name` as category_name,
    `Product Name` as product_name

from raw_data