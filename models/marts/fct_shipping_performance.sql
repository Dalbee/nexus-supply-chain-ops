{{ config(materialized='table') }}

with shipments as (
    -- Bringing in our cleaned staging data which now includes the unique shipment_item_id
    select * from {{ ref('stg_shipments') }}
),
products as (
    select * from {{ ref('dim_products') }}
),
locations as (
    select * from {{ ref('dim_locations') }}
),
shipping_info as (
    select * from {{ ref('dim_shipping_info') }}
)

select
    -- Primary Key for this Fact Table (Grain: One row per line item)
    s.shipment_item_id,
    
    s.order_id,
    
    -- Date Keys (Casted to Date for Power BI Calendar Relationship)
    -- We keep the timestamp for detail, but use to_date for the relationship key
    s.order_at,
    to_date(s.order_at) as order_date, 
    s.shipped_at,
    to_date(s.shipped_at) as shipped_date,
    
    -- Surrogate Keys (The Glue of the Star Schema)
    -- These link our Fact table to our Dimensions
    p.product_key,
    l.location_key,
    si.shipping_key,

    -- Metrics for the Dashboard
    s.actual_shipping_days,
    s.scheduled_shipping_days,
    -- Calculation to determine the variance in days
    (s.actual_shipping_days - s.scheduled_shipping_days) as delay_delta,
    
    -- Booleans for easy filtering in Power BI
    -- CASE statement creates a simple flag for late shipments
    case 
        when s.actual_shipping_days > s.scheduled_shipping_days then true 
        else false 
    end as is_late,

    -- Financials
    -- We keep these at the line-item level to allow for accurate summing in BI
    s.sales_amount,
    s.profit_amount

from shipments s
left join products p 
    on s.product_name = p.product_name 
    and s.category_name = p.category_name
left join locations l 
    on s.order_country = l.order_country 
    and s.order_region = l.order_region
left join shipping_info si
    on s.delivery_status = si.delivery_status
    and s.shipping_mode = si.shipping_mode