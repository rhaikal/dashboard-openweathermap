from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime

def test_postgres_connection():
    connection_id = "open_weather_map"
    
    postgres_hook = PostgresHook(postgres_conn_id=connection_id)
    
    with postgres_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT version();")
            version = cursor.fetchone()
            print(f"PostgreSQL version: {version[0]}")
            return version[0]

with DAG(
    "test_postgres_connection",
    default_args={"retries": 1},
    description="A simple DAG to test PostgreSQL connection via environment variable",
    schedule_interval=None,
    catchup=False,
) as dag:

    test_connection_task = PythonOperator(
        task_id="test_connection",
        python_callable=test_postgres_connection,
    )

    test_connection_task
