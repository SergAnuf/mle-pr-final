from airflow import DAG
from airflow.operators.python import PythonOperator  # Update import for Airflow 2.x
from datetime import datetime, timedelta
from steps.retrain import (
    extract_item_categories, 
    extract_events,
    transform_events,
    create_events_train_table,
    load_events_train_table
)
from steps.messages import send_telegram_success_message, send_telegram_failure_message
import pendulum

start_date = '2015-06-15'  # train start date
end_date = '2015-09-01'    # train end date

with DAG(
    dag_id='retrain_model',
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
        op_kwargs={'start_date': start_date, 'end_date': end_date},
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

    # Uncomment if you need to train the model
    # train_model_task = PythonOperator(
    #     task_id='train_model',
    #     python_callable=train_model,
    #     retries=2, 
    #     retry_delay=timedelta(seconds=20)
    # )

    # Setting up dependencies
    extract_categories_task >> extract_events_task >> transform_events_task >> load_events_train_table_task
    create_events_train_table_task >> load_events_train_table_task
