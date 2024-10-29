from airflow import DAG
from airflow.operators.python import PythonOperator  # Update import for Airflow 2.x
from datetime import datetime, timedelta
from steps.extract2 import (
    extract_item_prices, 
    load_price, 
   create_price_table,
   extract_item_descriptions,
   load_descriptions,
)
from steps.messages import send_telegram_success_message, send_telegram_failure_message
import pendulum



with DAG(
    dag_id='get_data_price',
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message) as dag:

    create_prices = PythonOperator(           
        task_id='create_price_table',
        python_callable=create_price_table,
    )

    extract_prices = PythonOperator(
        task_id='extract_item_prices',
        python_callable=extract_item_prices,
        retries=2, 
        retry_delay=timedelta(seconds=20)
    )

    load_price_task = PythonOperator(
        task_id='load_price',
        python_callable=load_price,
        retries=2, 
        retry_delay=timedelta(seconds=20)
    )

    create_descriptions = PythonOperator(           
        task_id='create_descriptions_table',
        python_callable=create_descriptions_table,
    )

    extract_descriptions = PythonOperator(
        task_id='extract_item_descriptions',
        python_callable=extract_item_descriptions,
        retries=2, 
        retry_delay=timedelta(seconds=20)
    )

    load_descriptions_task = PythonOperator(
        task_id='load_descriptions',
        python_callable=load_descriptions,
        retries=2, 
        retry_delay=timedelta(seconds=20)
    )


    # Setting up dependencies
    create_prices >> extract_prices >> load_price_task
    create_descriptions >> extract_descriptions >> load_descriptions_task

