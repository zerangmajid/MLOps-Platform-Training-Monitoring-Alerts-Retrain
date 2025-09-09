# dags/train_duration_model_dag.py
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="train_duration_model",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["train", "mlflow"],
) as dag:

    year_tmpl = "{{ dag_run.conf.get('year', data_interval_start.year) }}"
    month_tmpl = "{{ dag_run.conf.get('month', data_interval_start.month) }}"

    train = BashOperator(
        task_id="train",
        bash_command=(
            "/home/airflow/.venvs/monitor/bin/python "
            "/opt/airflow/jobs/duration-prediction.py "
            f"--year {year_tmpl} --month {month_tmpl}"
        ),
        env={
            "MLFLOW_TRACKING_URI": os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
            # "MLFLOW_TRACKING_URI": "{{ env.MLFLOW_TRACKING_URI | default('http://mlflow:5000') }}"
            # "MLFLOW_TRACKING_URI": "{{ env_var('MLFLOW_TRACKING_URI') or 'http://mlflow:5000' }}"

        },
    )
