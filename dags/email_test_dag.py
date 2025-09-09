import os
from datetime import datetime
from airflow import DAG
from airflow.operators.email import EmailOperator

ALERT_EMAIL_TO = os.environ.get("ALERT_EMAIL_TO", "majidzerang.de@gmail.com")

with DAG("test_email", start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    EmailOperator(
        task_id="send_test_email",
        to=ALERT_EMAIL_TO,   # ← رشته‌ی معمولی، نه قالب Jinja
        subject="Airflow SMTP test",
        html_content="SMTP is OK ✅",
    )
