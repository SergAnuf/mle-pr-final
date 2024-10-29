from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import MetaData, Table, Column, Integer, UniqueConstraint,BigInteger, Integer, TIMESTAMP,Text,Float
import pandas as pd
import logging
from sklearn.preprocessing import LabelEncoder

def create_events_train_table():
    metadata = MetaData()
    events_table = Table(
        'events_train',
        metadata,
        Column('itemid', Integer, primary_key=True),
        Column('categoryid', Integer),
        Column('user_id_enc', Integer),
        Column('visitorid', Integer),
        Column('categoryid_enc', Integer),
        Column('event', Float),
        UniqueConstraint('user_id_enc', 'categoryid_enc', name='unique_user_category')
    )
    
    hook = PostgresHook('destination_db')
    try:
        metadata.create_all(hook.get_sqlalchemy_engine())
        logging.info("Events table created successfully with unique constraint on user_id_enc and categoryid_enc.")
    except Exception as e:
        logging.error(f"Failed to create events table: {e}")


def extract_events(**kwargs):
    
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

    # Encode 'visitorid' and 'categoryid' as integers
    events['user_id_enc'] = user_encoder.fit_transform(events['visitorid'])
    events['categoryid_enc'] = category_encoder.fit_transform(events['categoryid'])

    # Remove duplicate combinations of 'user_id_enc' and 'categoryid_enc'
    events = events.drop_duplicates(subset=["user_id_enc", "categoryid_enc"])
    
    columns = ["user_id_enc", "categoryid_enc", "event", "visitorid", "categoryid","itemid"]
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
        rows = events_reset.values.tolist()
        data_columns = events_reset.columns.tolist()

        logging.info(f"Inserting {len(rows)} rows into events table.")
        try:
            hook.insert_rows(
                table='events',
                rows=rows,
                target_fields=data_columns,
                replace=True,
                replace_index=['user_id_enc', 'categoryid_enc']  # Unique constraint fields
            )
            logging.info("Events data loaded successfully.")
        except Exception as e:
            logging.error(f"Failed to insert rows into events table: {e}")
    else:
        logging.error("No data available to load into events table.")

# def create_categories_table():
#     metadata = MetaData()
#     categories_table = Table(
#         'item_categories',  # Correct table name for categories
#         metadata,
#         Column('itemid', Integer, primary_key=True),
#         Column('categoryid', Integer),
#         Column('parentid', Float),
#         UniqueConstraint('itemid', name='itemid_unique')
#     )
#     hook = PostgresHook('destination_db')
#     metadata.create_all(hook.get_sqlalchemy_engine())



# def load_categories(**kwargs):
#     hook = PostgresHook('destination_db')
#     ti = kwargs['ti']

#     # Pull data from XCom
#     data_categories = ti.xcom_pull(task_ids='extract_item_categories', key='extracted_item_categories')
    
#     # Check if data_prices is a list and convert to DataFrame if necessary
#     if isinstance(data_categories, list):
#         data_categories = pd.DataFrame(data_categories)

#     # Logging the data pulled from XCom
#     logging.info(f"Pulled data from XCom: {data_categories.head() if isinstance(data_categories, pd.DataFrame) else 'No data'}")
    
#     # Check if the DataFrame is not empty
#     if data_categories is not None and not data_categories.empty:
#         # Reset the index to move 'itemid', 'timestamp', 'value' back to columns
#         data_categories_reset = data_categories.reset_index(drop=True)

#         # Prepare rows and columns for insertion
#         rows = data_categories_reset.values.tolist()
#         data_columns = data_categories_reset.columns.tolist()

#         logging.info(f"Inserting {len(rows)} rows into item_categories.")
#         try:
#             hook.insert_rows(
#                 table='item_categories',
#                 rows=rows,
#                 target_fields=data_columns,
#                 replace=True,
#                 replace_index=['itemid']  # Adjust based on your unique constraints
#             )
#             logging.info("Item categories data loaded successfully.")
#         except Exception as e:
#             logging.error(f"Failed to insert rows into item_categories: {e}")
#     else:
#         logging.error("No data available to load into item_categories.")


