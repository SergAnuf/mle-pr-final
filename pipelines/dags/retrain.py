import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator  # Update import for Airflow 2.x
from datetime import datetime, timedelta
import pendulum
from steps.messages import send_telegram_success_message, send_telegram_failure_message
from steps.retrain import (
    extract_item_categories, 
    extract_events,
    transform_events,
    create_events_train_table,
    load_events_train_table,
    load_and_train_model)


with open('dags/train_params.yml', 'r') as file:
        params = yaml.safe_load(file)

with DAG(
    dag_id='model_update',
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message,
    catchup=False  # Disable catchup if you want to run only the latest schedule
) as dag:

    create_events_train_table_task = PythonOperator(
        task_id='create_events_train_table',
        python_callable=create_events_train_table,
        retries=2, 
        retry_delay=timedelta(seconds=20)
    )

    extract_categories_task = PythonOperator(
        task_id='extract_item_categories',
        python_callable=extract_item_categories,
        retries=2, 
        retry_delay=timedelta(seconds=20)
    )

    extract_events_task = PythonOperator(
        task_id='extract_events',
        python_callable=extract_events,
        op_kwargs={'start_date': params["start_date"], 'end_date': params["end_date"]},
        retries=2, 
        retry_delay=timedelta(seconds=20)
    )

    transform_events_task = PythonOperator(  
        task_id='transform_events',
        python_callable=transform_events,
        retries=2, 
        retry_delay=timedelta(seconds=20)
    )

    load_events_train_table_task = PythonOperator(
        task_id='load_events_train_table',
        python_callable=load_events_train_table,
        retries=2, 
        retry_delay=timedelta(seconds=20)
    )
    

    train_model_task = PythonOperator(
        task_id='train_model',
        python_callable=load_and_train_model,
        op_kwargs = params["model_config"],
        retries=2, 
        retry_delay=timedelta(seconds=20)
    )

     # Setting up dependencies
    create_events_train_table_task >> extract_categories_task >> extract_events_task >> transform_events_task >> load_events_train_table_task  >> train_model_task