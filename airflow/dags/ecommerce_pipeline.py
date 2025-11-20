# type: ignore
from airflow import DAG  # type: ignore
from airflow.operators.python_operator import PythonOperator  # type: ignore
from airflow.operators.bash import BashOperator  # type: ignore
from datetime import datetime, timedelta
import sys
sys.path.insert(0, '/opt/airflow/scripts')

from kafka_producer import EcommerceProducer  # type: ignore
from kafka_consumer import EcommerceConsumer  # type: ignore

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_kafka_producer():
    """Stream batch of orders to Kafka"""
    producer = EcommerceProducer()
    producer.load_data()
    producer.stream_orders(speed_multiplier=1000, max_events=10000)
    producer.close()

def run_kafka_consumer():
    """Consume messages from Kafka and write to PostgreSQL"""
    import time
    import threading
    
    # Give producer a head start
    time.sleep(2)
    consumer = EcommerceConsumer()
    
    # Run consumer in a thread with timeout
    consumer_thread = threading.Thread(target=consumer.process_messages)
    consumer_thread.daemon = True
    consumer_thread.start()
    
    # Wait for 45 seconds (enough time for 10000 events at 1000x speed)
    consumer_thread.join(timeout=45)
    
    # Force shutdown if still running
    consumer.shutdown_requested = True
    consumer_thread.join(timeout=5)
    
    # Clean up
    consumer.close()

with DAG(
    'ecommerce_daily_pipeline',
    default_args=default_args,
    description='Daily e-commerce data pipeline',
    schedule='@daily',
    catchup=False,
    tags=['ecommerce', 'etl']
) as dag:
    
    # Task 1: Stream orders to Kafka
    stream_orders = PythonOperator(
        task_id='stream_orders_to_kafka',
        python_callable=run_kafka_producer
    )
    
    # Task 2: Consume from Kafka and load to PostgreSQL
    consume_orders = PythonOperator(
        task_id='consume_orders_from_kafka',
        python_callable=run_kafka_consumer
    )
    
    # Task 3: Run dbt models
    run_dbt = BashOperator(
        task_id='run_dbt_models',
        bash_command='cd /opt/airflow/dbt/ecommerce_analytics && dbt run'
    )
    
    # Task 4: Run dbt tests
    test_dbt = BashOperator(
        task_id='test_dbt_models',
        bash_command='cd /opt/airflow/dbt/ecommerce_analytics && dbt test'
    )
    
    # Set dependencies - producer and consumer run in parallel, then dbt
    [stream_orders, consume_orders] >> run_dbt >> test_dbt