# /opt/airflow/dags/drift_alert_dag.py
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

THRESHOLD = float(os.getenv("DRIFT_THRESHOLD", "0.20"))
ALERT_TO = os.getenv("ALERT_EMAIL_TO")  # در docker-compose ست شده

def branch_on_drift(ti):
    hook = PostgresHook(postgres_conn_id="metrics_db")
    rows = hook.get_records(
        """
        SELECT to_char("timestamp",'YYYY-MM-DD HH24:MI') AS ts, prediction_drift
        FROM dummy_metrics
        WHERE prediction_drift > %s
        ORDER BY "timestamp" DESC
        """,
        parameters=(THRESHOLD,),
    )
    # اگر چیزی پیدا شد، برای ایمیل بفرست
    if rows:
        ti.xcom_push(key="breaches", value=rows)
        return "send_alert"
    return "ok"

with DAG(
    dag_id="drift_alert_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,          # اگر می‌خوای زمان‌بندی بشه، اینو مثلاً روزانه کن
    catchup=False,
    tags=["monitoring", "alert"],
) as dag:
    decide = BranchPythonOperator(
        task_id="decide",
        python_callable=branch_on_drift,
    )

    send_alert = EmailOperator(
    task_id="send_alert",
    to=ALERT_TO,
    subject="🚨 Drift alert — {{ params.threshold }} threshold exceeded",
    params={"threshold": THRESHOLD},
    html_content="""
      <h3>Threshold breaches (>{{ params.threshold }})</h3>
      {% set rows = ti.xcom_pull(task_ids='decide', key='breaches') or [] %}
      <ul>
      {% for ts, val in rows %}
        <li><b>{{ ts }}</b> — {{ '%.4f'|format(val|float) }}</li>
      {% endfor %}
      </ul>
    """,
)


    ok = EmptyOperator(task_id="ok")
    no_data = EmptyOperator(task_id="no_data")

    decide >> [send_alert, ok, no_data]
