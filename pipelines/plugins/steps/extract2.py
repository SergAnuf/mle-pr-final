from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from sqlalchemy import MetaData, Table, Column, Integer, UniqueConstraint,BigInteger, Integer, TIMESTAMP
import logging


def extract_item_prices(**kwargs):
    hook = PostgresHook('destination_db')

    sql_file_path = '/opt/airflow/scripts/extract_item_prices.sql'

    with open(sql_file_path, 'r') as file:
        sql = file.read()
        data = pd.read_sql(sql, con=hook.get_conn())
    
    if not data.empty:
        # Ensure the correct data types are set
        data['itemid'] = data['itemid'].astype('int64')  # bigint
        data['value'] = data['value'].astype('int')      # integer
        data['timestamp'] = pd.to_datetime(data['timestamp'], unit='s')  # Ensure timestamp is in datetime format
        
        logging.info(f"Extracted item prices: {data.head()}")

        # Push the DataFrame to XCom
        kwargs['ti'].xcom_push(key='extracted_item_prices', value=data)
    else:
        logging.warning("No data extracted.")



def create_price_table():
    metadata = MetaData()
    price_table = Table(
        'price_data2', 
        metadata,
        Column('itemid', BigInteger, primary_key=False),
        Column('timestamp', TIMESTAMP, primary_key=False),
        Column('value', Integer),
        UniqueConstraint('itemid', 'timestamp', name='unique_price_stamp')
    )
    hook = PostgresHook('destination_db')
    metadata.create_all(hook.get_sqlalchemy_engine())



def load_price(**kwargs):
    hook = PostgresHook('destination_db')
    ti = kwargs['ti']

    # Pull data from XCom
    data_prices = ti.xcom_pull(task_ids='extract_item_prices', key='extracted_item_prices')
    
    # Check if data_prices is a list and convert to DataFrame if necessary
    if isinstance(data_prices, list):
        data_prices = pd.DataFrame(data_prices)

    # Logging the data pulled from XCom
    logging.info(f"Pulled data from XCom: {data_prices.head() if isinstance(data_prices, pd.DataFrame) else 'No data'}")
    
    # Check if the DataFrame is not empty
    if data_prices is not None and not data_prices.empty:
        # Reset the index to move 'itemid', 'timestamp', 'value' back to columns
        data_prices_reset = data_prices.reset_index(drop=True)

        # Prepare rows and columns for insertion
        rows = data_prices_reset.values.tolist()
        data_columns = data_prices_reset.columns.tolist()

        logging.info(f"Inserting {len(rows)} rows into price_data2.")
        try:
            hook.insert_rows(
                table='price_data2',
                rows=rows,
                target_fields=data_columns,
                replace=True,
                replace_index=['itemid', 'timestamp']  # Adjust based on your unique constraints
            )
            logging.info("Price data loaded successfully.")
        except Exception as e:
            logging.error(f"Failed to insert rows into price_data2: {e}")
    else:
        logging.error("No data available to load into price_data2.")

