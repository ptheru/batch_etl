from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# This DAG runs inside the Airflow container.
PROJECT = "/opt/project"
PY = "python"

default_args = {
    "owner": "test",
    "retries": 2,
}

with DAG(
    dag_id="batch_etl_bronze_silver_gold",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # trigger manually (good for interviews)
    catchup=False,
    tags=["batch", "etl", "spark"],
) as dag:

    gen_raw = BashOperator(
        task_id="generate_raw_events",
        bash_command=f"cd {PROJECT} && {PY} scripts/generate_raw_events.py",
    )

    bronze = BashOperator(
        task_id="bronze_ingest",
        bash_command=f"cd {PROJECT} && {PY} -m src.bronze.ingest_events",
    )

    silver = BashOperator(
        task_id="silver_transform",
        bash_command=f"cd {PROJECT} && {PY} -m src.silver.transform_events",
    )

    gold = BashOperator(
        task_id="gold_build",
        bash_command=f"cd {PROJECT} && {PY} -m src.gold.build_analytics",
    )

    gen_raw >> bronze >> silver >> gold
