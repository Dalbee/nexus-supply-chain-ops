select *
from {{ ref('int_shipments') }}
where cast(sales_amount as double) < 0.0