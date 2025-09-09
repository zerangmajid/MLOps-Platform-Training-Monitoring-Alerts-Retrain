# dags/dummy_metrics_dag.py
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import random, logging, uuid

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")
rand = random.Random()

CREATE_SQL = """
create table if not exists dummy_metrics(
  ts timestamptz,
  value1 integer,
  value2 varchar,
  value3 double precision
);
"""

def insert_one():
    import psycopg, pytz
    tz = pytz.timezone('Asia/Tehran')
    with psycopg.connect("host=db port=5432 user=postgres password=postgres", autocommit=True) as conn:
        res = conn.execute("SELECT 1 FROM pg_database WHERE datname='test'")
        if not res.fetchall():
            conn.execute("create database test;")
    with psycopg.connect("host=db port=5432 dbname=test user=postgres password=postgres", autocommit=True) as conn:
        conn.execute(CREATE_SQL)
        with conn.cursor() as cur:
            cur.execute(
                "insert into dummy_metrics(ts, value1, value2, value3) values (%s,%s,%s,%s)",
                (tz.localize(datetime.datetime.now()),
                 rand.randint(0, 1000), str(uuid.uuid4()), rand.random())
            )

with DAG(
    dag_id="dummy_metrics_dag",
    start_date=datetime.datetime(2025,1,1),
    schedule=None,  # یا "* * * * *" برای هر دقیقه
    catchup=False,
) as dag:
    PythonOperator(task_id="insert_row", python_callable=insert_one)
