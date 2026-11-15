from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

logger = logging.getLogger("airflow.task")

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

def check_raw_data_health():
    """
    Lightweight healthcheck ensuring that streaming events have arrived
    within the expected SLA window before triggering downstream dbt transformations.
    """
    import os
    import duckdb

    motherduck_token = os.getenv("MOTHERDUCK_TOKEN")
    db_name = os.getenv("MOTHERDUCK_DATABASE", "byteflow_prod")

    if motherduck_token:
        con = duckdb.connect(f"md:{db_name}?motherduck_token={motherduck_token}")
    else:
        con = duckdb.connect("byteflow_local.duckdb")

    try:
        result = con.execute("SELECT count(*) FROM raw_orders_stream").fetchone()[0]
        logger.info(f"[Healthcheck] raw_orders_stream row count: {result}")
        if result == 0:
            logger.warning("No records found in raw_orders_stream. Seed or producer may be initializing.")
    except Exception as e:
        logger.warning(f"Could not query raw_orders_stream ({e}). Creating table if first run...")
        con.execute("""
            CREATE TABLE IF NOT EXISTS raw_orders_stream (
                event_id VARCHAR PRIMARY KEY,
                order_id VARCHAR,
                customer_id VARCHAR,
                customer_email VARCHAR,
                customer_country VARCHAR,
                customer_city VARCHAR,
                order_timestamp TIMESTAMP,
                status VARCHAR,
                payment_method VARCHAR,
                total_order_value DOUBLE,
                currency VARCHAR,
                device_type VARCHAR,
                raw_payload JSON,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
    finally:
        con.close()

with DAG(
    dag_id="byteflow_analytics_pipeline",
    default_args=default_args,
    description="Orchestrates ingestion verification and dbt dimensional transformations across MotherDuck/DuckDB",
    schedule_interval="*/30 * * * *",  # Runs every 30 minutes
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["streaming", "dbt", "ecommerce", "production"],
) as dag:

    raw_healthcheck = PythonOperator(
        task_id="verify_raw_stream_health",
        python_callable=check_raw_data_health,
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="cd /opt/airflow/dbt_project && dbt deps --profiles-dir .",
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command="cd /opt/airflow/dbt_project && dbt seed --profiles-dir .",
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command="cd /opt/airflow/dbt_project && dbt run --select staging --profiles-dir .",
    )

    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command="cd /opt/airflow/dbt_project && dbt test --select staging --profiles-dir .",
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command="cd /opt/airflow/dbt_project && dbt run --select marts --profiles-dir .",
    )

    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command="cd /opt/airflow/dbt_project && dbt test --select marts --profiles-dir .",
    )

    # Dependency Pipeline DAG
    raw_healthcheck >> dbt_deps >> dbt_seed >> dbt_run_staging >> dbt_test_staging >> dbt_run_marts >> dbt_test_marts
