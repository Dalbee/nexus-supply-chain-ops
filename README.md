# Supply Chain Star Schema (dbt + Databricks)

## Project Overview
Refactored raw, nested supply chain shipment data into a high-performance Star Schema designed for Power BI "Drill Down" analytics. This project demonstrates a full-stack ELT pipeline from a Databricks Unified Data Analytics platform to a structured dbt modeling layer.

## Infrastructure & Setup

### 1. Databricks Warehouse Configuration
To establish the connection between dbt and the Databricks SQL Warehouse:
* **SQL Warehouse:** Created a "Pro" SQL Warehouse in Databricks to handle compute for dbt-fusion workloads.
* **Server Hostname:** Obtained from the 'Connection Details' tab of the SQL Warehouse in the Databricks UI.
* **HTTP Path:** Configured the unique cluster path (e.g., `/sql/1.0/warehouses/...`) in the dbt connection settings to route queries directly to the compute instance.
* **Authentication:** Generated a **Personal Access Token (PAT)** via Databricks User Settings to securely authorize the dbt service account.

### 2. dbt Project Initialization
* **Adapter:** Utilized the `dbt-databricks` adapter for optimized compatibility with Delta Lake.
* **Schema Strategy:** Defined a custom target schema (e.g., `dbt_shipping_dev`) to isolate development models from raw source data.
* **Naming Conventions:**
    * `stg_`: Staging models for light transformation and renaming.
    * `dim_`: Dimension tables containing descriptive attributes.
    * `fct_`: Fact tables containing quantitative metrics and surrogate keys.

## Architecture Highlights
- **Granularity:** Pivoted from Order-level to **Order-Item-level** (`shipment_item_id`) to ensure accurate financial reporting and granular analysis.
- **Dimensional Modeling:** Implemented a central Fact table (`fct_shipping_performance`) supported by dimensions for Products, Locations, Shipping Info, and a custom Calendar (Date) table.

## Data Validation & Test Coverage
We implemented a multi-layered testing strategy to ensure "Zero-Defect" reporting:

### 1. Schema Tests (Generic)
* **Uniqueness:** Applied `unique` tests to `shipment_item_id` in both Staging and Fact layers to prevent row-inflation and duplicate sales reporting.
* **Null Handling:** Applied `not_null` constraints to all primary keys and critical foreign keys (`product_key`, `location_key`) to ensure referential integrity.
* **Financial Integrity:** Added `not_null` tests to `sales_amount` and `profit_amount` to ensure dashboard metrics are always credible.

### 2. Referential Integrity (Relationship Tests)
* **Date Validation:** Implemented a `relationships` test between `fct_shipping_performance.order_date` and `dim_date.date_key`. This ensures every transaction is linked to a valid date in the calendar, preventing "Blank" values in Power BI time slicers.
* **dbt 2.0 Compliance:** Migrated relationship tests to the new `arguments` block syntax to support dbt-fusion 2.0-preview requirements.



## Key Deliverables
- **Bronze to Silver:** Applied schema-on-read logic, explicit type casting (Decimal/Timestamp), and primary key deduplication in `stg_shipments`.
- **Silver to Gold:** Established Star Schema join logic using surrogate keys to eliminate data fan-out.
- **Temporal Analytics:** Created a `dim_date` table and standardized `order_date` keys in the fact table to support Power BI Time Intelligence.

## How to Run
1. Ensure your Databricks SQL Warehouse is **Running**.
2. Run `dbt deps` to install required packages.
3. Run `dbt build` to execute models and run data quality tests simultaneously.