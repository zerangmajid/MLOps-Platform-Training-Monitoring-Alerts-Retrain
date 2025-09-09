from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
import os

with DAG(
    dag_id="metrics_backfill_to_postgres",
    start_date=datetime(2025, 1, 1),
    schedule=None,          #
    catchup=False,
    tags=["monitoring","evidently"],
) as dag:
    run = BashOperator(
        task_id="run_backfill_script",
        bash_command="/home/airflow/.venvs/monitor/bin/python /opt/airflow/jobs/monitor_backfill.py",
        env={"PYTHONUNBUFFERED": "1"},
    )
