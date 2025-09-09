# /opt/airflow/dags/drift_sentinel.py
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

THRESHOLD = float(os.getenv("DRIFT_THRESHOLD", "0.19"))
ALERT_TO = os.getenv("ALERT_EMAIL_TO")
RETRAIN_DAG_ID = os.getenv("RETRAIN_DAG_ID", "train_duration_model")

def decide_branch(ti):
    hook = PostgresHook(postgres_conn_id="metrics_db")

    total = hook.get_first("SELECT count(*) FROM dummy_metrics")
    if not total or int(total[0]) == 0:
        return "no_data"

    last_ts, last_val = hook.get_first(
        """
        SELECT "timestamp", prediction_drift
        FROM dummy_metrics
        ORDER BY "timestamp" DESC
        LIMIT 1
        """
    )
    ti.xcom_push(key="latest_ts", value=str(last_ts))
    ti.xcom_push(key="latest_val", value=float(last_val))

    # Only take action when the latest point breaches the threshold
    if float(last_val) > THRESHOLD:
        return "trigger_retrain"

    return "ok"

with DAG(
    dag_id="drift_sentinel",
    start_date=datetime(2024, 1, 1),
    schedule=None,              # set a cron here if you want it time-based
    catchup=False,
    max_active_runs=1,
    tags=["monitoring", "alert", "retrain"],
) as dag:

    decide = BranchPythonOperator(
        task_id="decide",
        python_callable=decide_branch,
    )

    trigger_retrain = TriggerDagRunOperator(
        task_id="trigger_retrain",
        trigger_dag_id=RETRAIN_DAG_ID,
        reset_dag_run=True,
        wait_for_completion=True,      # wait until the retrain DAG finishes
        poke_interval=30,
        conf={
            "reason": "drift_threshold_breached",
            "latest_ts": "{{ ti.xcom_pull(task_ids='decide', key='latest_ts') }}",
            "latest_val": "{{ ti.xcom_pull(task_ids='decide', key='latest_val') }}",
            "threshold": THRESHOLD,
        },
    )

    send_alert = EmailOperator(
        task_id="send_alert",
        to=ALERT_TO,
        subject="Drift alert — retrain finished (latest={{ ti.xcom_pull(task_ids='decide', key='latest_val') }})",
        html_content="""
          <h3>Drift threshold breached and retrain completed</h3>
          <p><b>Latest point:</b> {{ ti.xcom_pull(task_ids='decide', key='latest_ts') }}
             — {{ '%.4f'|format((ti.xcom_pull(task_ids='decide', key='latest_val') or 0)|float) }}</p>
          <p>Threshold: {{ params.threshold }}</p>
          <p>Retrain DAG: {{ params.retrain_dag_id }} finished.</p>
        """,
        params={"threshold": THRESHOLD, "retrain_dag_id": RETRAIN_DAG_ID},
    )

    ok = EmptyOperator(task_id="ok")
    no_data = EmptyOperator(task_id="no_data")

    # order: decide → (trigger_retrain → send_alert) or ok / no_data
    decide >> trigger_retrain >> send_alert
    decide >> [ok, no_data]
