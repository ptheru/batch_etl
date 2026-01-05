from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "/opt/project"
PY = "python"

default_args = {
    "owner": "test",
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="batch_etl_bronze_silver_gold",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",          
    catchup=False,
    max_active_runs=1,                  
    tags=["batch", "etl", "spark"],
) as dag:

    # Airflow execution date (YYYY-MM-DD)
    process_date = "{{ ds }}"

    gen_raw = BashOperator(
        task_id="generate_raw_events",
        bash_command=(
            f"cd {PROJECT} && "
            f"PROCESS_DATE={process_date} "
            f"{PY} scripts/api_extract_events.py --process-date {process_date}"
        ),
    )

    bronze = BashOperator(
        task_id="bronze_ingest",
        bash_command=(
            f"cd {PROJECT} && "
            f"PROCESS_DATE={process_date} "
            f"{PY} -m bronze.ingest_events"
        ),
    )

    silver = BashOperator(
        task_id="silver_transform",
        bash_command=(
            f"cd {PROJECT} && "
            f"PROCESS_DATE={process_date} "
            f"{PY} -m silver.transform_events"
        ),
    )

    gold = BashOperator(
        task_id="gold_build",
        bash_command=(
            f"cd {PROJECT} && "
            f"PROCESS_DATE={process_date} "
            f"{PY} -m gold.build_analytics"
        ),
    )

    gen_raw >> bronze >> silver >> gold
