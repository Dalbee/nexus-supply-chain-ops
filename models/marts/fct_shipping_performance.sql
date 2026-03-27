{{ config(materialized='table') }}

with shipments as (
    -- Pulling the transactional grain from Silver
    select * from {{ ref('int_shipments') }}
),

dim_products as (select * from {{ ref('dim_products') }}),
dim_locations as (select * from {{ ref('dim_locations') }}),
dim_shipping_info as (select * from {{ ref('dim_shipping_info') }}),
dim_date as (select * from {{ ref('dim_date') }})

select
    -- Primary Key for the Fact Table
    s.shipment_item_id,
    s.order_id,
    
    -- Foreign Keys (Referencing the Dimensions)
    p.product_key,
    l.location_key,
    si.shipping_key,
    
    -- Safety Net: Ensure the key is never null even if date is out of range for dim_date
    coalesce(d.date_key, cast(s.order_at as date)) as order_date_key,

    -- Dimensional Attributes kept for detail
    s.order_at,
    s.shipped_at,

    -- Performance Metrics (Calculated in the Fact layer)
    s.actual_shipping_days,
    s.scheduled_shipping_days,
    (s.actual_shipping_days - s.scheduled_shipping_days) as delay_delta,
    
    case 
        when s.actual_shipping_days > s.scheduled_shipping_days then true 
        else false 
    end as is_late,

    -- Quantitative flag for Power BI DAX (Late = 1, On-time = 0)
    case 
        when s.actual_shipping_days > s.scheduled_shipping_days then 1 
        else 0 
    end as late_delivery_risk,

    -- UPSTREAM UPGRADE: SLA Efficiency Gap Logic (Moved from DAX)
    -- This enables sub-second query performance for SLA analysis
    (s.actual_shipping_days - s.scheduled_shipping_days) as sla_efficiency_gap,
    
    case 
        when (s.actual_shipping_days - s.scheduled_shipping_days) > 0 then 'Delayed'
        else 'On-Track'
    end as sla_status_label,

    -- UPSTREAM UPGRADE: Waterfall & Risk Foundation
    -- Isolates revenue tied specifically to logistics failures
    case 
        when s.actual_shipping_days > s.scheduled_shipping_days then s.sales_amount 
        else 0 
    end as sales_at_late_risk,

    -- Perfect Order Flag: On-time delivery AND realizing profit
    case 
        when s.actual_shipping_days <= s.scheduled_shipping_days and s.profit_amount > 0 then 1 
        else 0 
    end as is_perfect_order,

    -- Financial Measures (Realized vs. Risk)
    s.sales_amount,
    s.profit_amount,
    
    -- Standardized Audit: Realized profit (Only for non-cancelled/fraud orders)
    case 
        when s.order_status in ('COMPLETE', 'CLOSED') then s.profit_amount 
        else 0 
    end as realized_profit

from shipments s
-- Joins restore the dbt Lineage Graph
left join dim_products p on s.product_key = p.product_key
left join dim_locations l on s.location_key = l.location_key
left join dim_shipping_info si on s.shipping_key = si.shipping_key
left join dim_date d on cast(s.order_at as date) = d.date_key