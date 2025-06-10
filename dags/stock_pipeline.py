from datetime import datetime
from airflow.sdk import DAG, task
from airflow.providers.standard.operators.python import PythonOperator
import subprocess

default_args = {
    'owner': 'bideen',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1)
}

def run_producer():
    subprocess.run(['python', '/opt/airflow/dags/producers.py'], check=True)


def run_consumers():
    subprocess.run(['python', '/opt/airflow/dags/consumers.py'], check=True)
    

with DAG(
    dag_id='stock_market_stream',
    default_args=default_args,
    schedule='@daily',
    catchup=False
) as dag:

    producer_task = PythonOperator(
        task_id='run_producer',
        python_callable=run_producer
    )

    consumer_task = PythonOperator(
        task_id='run_consumers',
        python_callable=run_consumers
    )

    producer_task >> consumer_task  # Ensure producer runs before consumer
# This DAG runs the producer and consumer scripts in sequence.
# The producer fetches stock data and sends it to Kafka, while the consumer processes this data and stores it in Snowflake.
    
    