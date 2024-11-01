from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import MetaData, Table, Column, Integer, UniqueConstraint, Integer, Text
import pandas as pd
import logging
from sklearn.preprocessing import LabelEncoder
from steps.model import ALSRecommender
import numpy as np
from dotenv import load_dotenv
import os
import boto3
from io import BytesIO
import pyarrow as pa
import pyarrow.parquet as pq


load_dotenv()


s3_client = boto3.client(
        's3',
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
        endpoint_url='https://storage.yandexcloud.net'  # Use the appropriate endpoint for your S3 service
    )
    


def create_events_train_table():
    metadata = MetaData()
    events_table = Table(
        'events_train',
        metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('itemid', Integer),
        Column('categoryid', Integer),
        Column('user_id_enc', Integer),
        Column('visitorid', Integer),
        Column('categoryid_enc', Integer),
        Column('event', Text),
        UniqueConstraint('user_id_enc', 'categoryid_enc', name='user_category_unique')
    )
    
    hook = PostgresHook('destination_db')
    try:
        metadata.create_all(hook.get_sqlalchemy_engine())
        logging.info("Events table created successfully with unique constraint on user_id_enc and categoryid_enc.")
    except Exception as e:
        logging.error(f"Failed to create events table: {e}")



def save_df_to_s3(df, bucket_name, s3_key, s3_client):
    """Save a DataFrame to S3 as a Parquet file."""
    buffer = BytesIO()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, buffer)
    buffer.seek(0)  # Move to the beginning of the buffer
    s3_client.upload_fileobj(buffer, bucket_name, s3_key)
    logging.info(f"Uploaded {s3_key} to bucket {bucket_name}")


def load_and_train_model(**kwargs):
    # Load Data
    hook = PostgresHook('destination_db')
    conn = hook.get_conn()
    sql = """
        SELECT 
          *
        FROM 
            events_train
    """
    events_train = pd.read_sql(sql, conn)
    
    if not events_train.empty:
        events_train["event"] = events_train["event"].astype("string")
        logging.info(f"Extracted Train Events: {events_train.head()}")
    else:
        logging.warning("No events train data extracted.")
        raise ValueError("No events data to train on.")

    # Define Configuration
    config = {
        "K": kwargs.get('K'),
        "FACTORS": kwargs.get('FACTORS'),
        "REGULARIZATION": kwargs.get('REGULARIZATION'),
        "ITERATIONS": kwargs.get('ITERATIONS')
    }
    
    # Initialize ALS Recommender and Train
    als_recommender = ALSRecommender(events_train=events_train, config=config)
    
    # Save Results
    filtered_recommendations = als_recommender.get_filtered_recommendations()
    similar_categories = als_recommender.get_similar_categories(chunk_size=1000, max_similar_items=10)

    bucket_name = os.environ.get('S3_BUCKET_NAME')
    
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
        endpoint_url='https://storage.yandexcloud.net' 
    )
    
    save_df_to_s3(filtered_recommendations, bucket_name, "models/staging/offline_dag.parquet", s3_client)
    save_df_to_s3(similar_categories, bucket_name, "models/staging/online_dag.parquet", s3_client)
    print("Uploaded recommendations and similar items to S3")




def extract_events(**kwargs):
    # start/end dates passed as **kewargs arguments
    hook =  PostgresHook('destination_db')
    conn = hook.get_conn()

    start_date = kwargs.get('start_date')  # train data between these two dates
    end_date = kwargs.get('end_date')      # 
    
    sql = f"""
        SELECT 
            TO_TIMESTAMP(timestamp / 1000) AS timestamp,  -- Convert Unix timestamp (in milliseconds) to proper timestamp
            visitorid,
            event,
            itemid
        FROM 
            events
        WHERE 
            TO_TIMESTAMP(timestamp / 1000) BETWEEN '{start_date}' AND '{end_date}'  -- Filter by date
    """

    events = pd.read_sql(sql, conn)
    if not events.empty:
        logging.info(f"Extracted Events: {events.head()}")
        kwargs['ti'].xcom_push(key='extracted_events', value=events)
    else:
        logging.warning("No events data extracted.")



def extract_item_categories(**kwargs):
    hook = PostgresHook('destination_db')
    sql_file_path = '/opt/airflow/scripts/extract_item_categories.sql'

    with open(sql_file_path, 'r') as file:
        sql = file.read()

    # Fetching the data from Postgres
    data = pd.read_sql(sql, con=hook.get_conn())

    if not data.empty:
        # Casting columns to the appropriate data types
        data['itemid'] = data['itemid'].astype('int64')
        data['categoryid'] = data['categoryid'].astype('int')

        # Check if 'value' exists and cast 'parentid' accordingly
        if 'value' in data.columns:
            data = data.rename(columns={'value': 'parentid'})

        logging.info(f"Extracted item categories: {data.head()}")
        kwargs['ti'].xcom_push(key='extracted_item_categories', value=data)
    else:
        logging.warning("No data extracted.")


def transform_events(**kwargs):
    ti = kwargs['ti']

    # Initialize LabelEncoders for encoding user and category IDs
    category_encoder = LabelEncoder()
    user_encoder = LabelEncoder()
    
    # Pull extracted data from XCom
    categories = ti.xcom_pull(task_ids='extract_item_categories', key='extracted_item_categories')
    events = ti.xcom_pull(task_ids='extract_events', key='extracted_events')

    if categories is None or events is None:
        logging.warning("Either categories or events data is missing. Transformation skipped.")
        return
    
    # Merge event data with category data on 'itemid'
    events = events.merge(categories, how="left", on="itemid")

    max_category_id = events["categoryid"].max()
    events["categoryid"] = events["categoryid"].fillna(max_category_id + 1000).astype(int)

    # Encode 'visitorid' and 'categoryid' as integers
    events['user_id_enc'] = user_encoder.fit_transform(events['visitorid']).astype(int)
    events['categoryid_enc'] = category_encoder.fit_transform(events['categoryid']).astype(int)

    # Convert 'visitorid' and 'categoryid' to integers
    events['visitorid'] = events['visitorid'].astype(int)
    events['categoryid'] = events['categoryid'].astype(int)
    events['itemid'] = events['itemid'].astype(int)

    # Convert 'event' column to float
    events['event'] = events['event'].astype(str)

    # Remove duplicate combinations of 'user_id_enc' and 'categoryid_enc'
    events = events.drop_duplicates(subset=["user_id_enc", "categoryid_enc"])
    
    columns = ["user_id_enc", "categoryid_enc", "event", "visitorid", "categoryid", "itemid"]
    # Push the transformed data to XCom
    ti.xcom_push(key='transformed_events', value=events[columns])
    logging.info("Transformation complete and data pushed to XCom.")


def load_events_train_table(**kwargs):
    hook = PostgresHook('destination_db')
    ti = kwargs['ti']

    # Pull transformed data from XCom
    transformed_events = ti.xcom_pull(task_ids='transform_events', key='transformed_events')
    
    # Check if transformed_events is a list and convert to DataFrame if necessary
    if isinstance(transformed_events, list):
        transformed_events = pd.DataFrame(transformed_events)

    # Logging the data pulled from XCom
    logging.info(f"Pulled data from XCom: {transformed_events.head() if isinstance(transformed_events, pd.DataFrame) else 'No data'}")
    
    # Check if the DataFrame is not empty
    if transformed_events is not None and not transformed_events.empty:
        # Reset the index to prepare for insertion
        events_reset = transformed_events.reset_index(drop=True)

        # Prepare rows and columns for insertion
        rows = events_reset[['itemid', 'categoryid', 'user_id_enc', 'visitorid', 'categoryid_enc', 'event']].values.tolist()
        data_columns = ['itemid', 'categoryid', 'user_id_enc', 'visitorid', 'categoryid_enc', 'event']

        logging.info(f"Inserting {len(rows)} rows into events table.")
        try:
            hook.insert_rows(
                table='events_train',
                rows=rows,
                target_fields=data_columns,
                replace=False,  # Set to False since we may want to keep existing rows with unique constraints
            )
            logging.info("Events data loaded successfully.")
        except Exception as e:
            logging.error(f"Failed to insert rows into events table: {e}")
    else:
        logging.error("No data available to load into events table.")


