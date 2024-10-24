from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta

from steps.get_item_data import (
    create_clean_table,

    extract_item_descriptions,
    extract_item_categories,
    extract_item_prices,

    transform_price_table, 
    transform_descriptions_table,
    
    load_item_table,
)

from steps.messages import send_telegram_success_message, send_telegram_failure_message
import pendulum

with DAG(
    dag_id='create_item_data',
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message) as dag:

    create_t = PythonOperator(
        task_id='create',
        python_callable=create_clean_table,
        retries=2, 
        retry_delay=timedelta(seconds=20) 
    )
    # Extract steps for prices, descriptions, and categories
    extract_prices = PythonOperator(
        task_id='extract_item_prices',
        python_callable=extract_item_prices,
        retries=2, 
        retry_delay=timedelta(seconds=20) 
    )

    extract_descriptions = PythonOperator(
        task_id='extract_item_descriptions',
        python_callable=extract_item_descriptions,
        retries=2, 
        retry_delay=timedelta(seconds=20)  # Extract descriptions
    )

    extract_categories = PythonOperator(
        task_id='extract_item_categories',
        python_callable=extract_item_categories,
        retries=2, 
        retry_delay=timedelta(seconds=20)  # Extract descriptions  # Extract categories
    )

    # Transform steps for prices and descriptions
    transform_price = PythonOperator(
        task_id='transform_price',
        python_callable=transform_price_table,  # Transform price table data
        retries=2, 
        retry_delay=timedelta(seconds=20)  # Extract descriptions  # Extract categories
    )
    

    transform_description = PythonOperator(
        task_id='transform_description',
        python_callable=transform_descriptions_table,
        retries=2, 
        retry_delay=timedelta(seconds=20)  # Transform description data
    )

    # Load step for final item data table
    load_items = PythonOperator(
        task_id='load_item_table',
        python_callable=load_item_table,
        op_kwargs={'table': 'item_data'}  # Specify the final table name
    )

    # Task dependencies
    extract_prices >> transform_price  # Extract prices then transform
    extract_descriptions >> transform_description  # Extract descriptions then transform
    [transform_price, transform_description, extract_categories] >> load_items  # Merge and load final table

