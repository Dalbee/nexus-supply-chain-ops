{{ config(materialized='table') }}

/* We pull from 'int_shipments' where the keys were already generated.
   Using 'distinct' ensures this table acts as a true reference dimension.
*/

select distinct
    shipping_key,
    delivery_status,
    shipping_mode
from {{ ref('int_shipments') }}