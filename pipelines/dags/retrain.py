from airflow import DAG
from airflow.operators.python import PythonOperator  # Update import for Airflow 2.x
from datetime import datetime, timedelta
import pendulum
from steps.messages import send_telegram_success_message, send_telegram_failure_message
from steps.retrain import load_and_train_model
#     extract_item_categories, 
#     extract_events,
#     transform_events,
#     create_events_train_table,
#     load_events_train_table,
#     get_events_train_table,
#     load_and_train_model

# )
# training configuration
start_date = '2015-06-15'  # train start date
end_date = '2015-09-01'    # train end date
config5 =  {"K": 7, "FACTORS": 50, "REGULARIZATION": 0.01, "ITERATIONS": 70}  # model config

with DAG(
    dag_id='model_update',
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message,
    catchup=False  # Disable catchup if you want to run only the latest schedule
) as dag:

    # create_events_train_table_task = PythonOperator(
    #     task_id='create_events_train_table',
    #     python_callable=create_events_train_table,
    #     retries=2, 
    #     retry_delay=timedelta(seconds=20)
    # )

    # extract_categories_task = PythonOperator(
    #     task_id='extract_item_categories',
    #     python_callable=extract_item_categories,
    #     retries=2, 
    #     retry_delay=timedelta(seconds=20)
    # )

    # extract_events_task = PythonOperator(
    #     task_id='extract_events',
    #     python_callable=extract_events,
    #     op_kwargs={'start_date': start_date, 'end_date': end_date},
    #     retries=2, 
    #     retry_delay=timedelta(seconds=20)
    # )

    # transform_events_task = PythonOperator(  
    #     task_id='transform_events',
    #     python_callable=transform_events,
    #     retries=2, 
    #     retry_delay=timedelta(seconds=20)
    # )

    # load_events_train_table_task = PythonOperator(
    #     task_id='load_events_train_table',
    #     python_callable=load_events_train_table,
    #     retries=2, 
    #     retry_delay=timedelta(seconds=20)
    # )
    
    # upload_events_train_table = PythonOperator(
    #     task_id='extract_events_train_table',
    #     python_callable=get_events_train_table,
    #     retries=2, 
    #     retry_delay=timedelta(seconds=20)
    # )
   
    train_model_task = PythonOperator(
        task_id='train_model',
        python_callable=load_and_train_model,
        op_kwargs = config5,
        retries=2, 
        retry_delay=timedelta(seconds=20)
    )

    # Setting up dependencies
    # extract_categories_task >> extract_events_task >> transform_events_task >> load_events_train_table_task
    # create_events_train_table_task >> load_events_train_table_task
    # upload_events_train_table >> train_model_task
    # train_model_task