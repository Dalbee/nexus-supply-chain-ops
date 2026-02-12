{{ config(
    materialized='table',
    catalog='workspace',
    schema='supply_chain_analytics'
) }}

with shipments as (
  select * from {{ ref('stg_shipments') }}
)

select 
    *,
    (actual_days - scheduled_days) as delay_delta,
    case 
        when actual_days > scheduled_days then true 
        else false 
    end as is_late_delivery
from shipments