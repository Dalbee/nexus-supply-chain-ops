select *
from {{ ref('stg_shipments') }}
where cast(sales_amount as double) < 0.0