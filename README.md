# Supply Chain Star Schema (dbt + Databricks)

## Project Overview
Refactored raw, nested supply chain shipment data into a high-performance Star Schema designed for Power BI "Drill Down" analytics.

## Architecture Highlights
- **Granularity:** Pivoted from Order-level to **Order-Item-level** (`shipment_item_id`) to ensure accurate financial reporting and granular analysis.
- **Dimensional Modeling:** Implemented a central Fact table (`fct_shipping_performance`) supported by dimensions for Products, Locations, Shipping Info, and a custom Calendar (Date) table.
- **Data Validation:** Implemented 11 data quality tests. 
    - *Note:* Currently 16/17 items passing; final relationship test failure is a known syntax deprecation issue within the dbt 2.0-preview environment (Fusion).

## Key Deliverables
- **Bronze to Silver:** Type casting and primary key deduplication in `stg_shipments`.
- **Silver to Gold:** Star Schema join logic using surrogate keys.
- **Temporal Analytics:** Clean `order_date` keys for Time Intelligence.