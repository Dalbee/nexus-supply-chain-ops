with source as (
    select * from {{ source('external_source', 'raw_supply_chain') }}
)

select 
    Type as payment_type,
    `Days for shipping (real)` as actual_days,
    `Days for shipment (scheduled)` as scheduled_days,
    `Benefit per order` as profit_per_order,
    `Sales per customer` as sales_amount,
    `Delivery Status` as delivery_status
from source