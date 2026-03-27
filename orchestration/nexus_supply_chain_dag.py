from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# 🕒 THE PLATFORM CONFIGURATION
# This proves you understand data reliability and retries
default_args = {
    'owner': 'Data_Platform_Engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 27),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'nexus_supply_chain_optimization',
    default_args=default_args,
    description='Orchestrating Medallion Architecture & SLA Governance on Databricks',
    schedule_interval='@daily', # Runs every morning for the logistics team
    catchup=False,
    tags=['Gold_Layer', 'SLA_Governance', 'Databricks'],
) as dag:

    # 1. Dependency Management
    # Ensures the dbt environment is synced before running
    install_deps = BashOperator(
        task_id='dbt_deps',
        bash_command='dbt deps',
    )

    # 2. Gold Layer Transformation
    # This runs the fct_shipping_performance model we just built
    run_marts = BashOperator(
        task_id='dbt_run_gold_marts',
        bash_command='dbt run --select path:models/marts',
    )

    # 3. Governance & Quality Gates
    # This ensures "Zero-Defect" reporting before the data hits Power BI
    test_governance = BashOperator(
        task_id='dbt_test_sla_compliance',
        bash_command='dbt test --select path:models/marts',
    )

    # The Lineage Flow
    install_deps >> run_marts >> test_governance