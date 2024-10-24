from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import MetaData, Table, Column, Integer, UniqueConstraint,BigInteger, Integer, TIMESTAMP,Text,Float
import pandas as pd
import logging




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



def extract_item_descriptions(**kwargs):

    ti = kwargs['ti']
    hook = PostgresHook('destination_db')
    sql_file_path = '/opt/airflow/scripts/extract_item_descriptions.sql'

    with open(sql_file_path, 'r') as file:
        sql = file.read()
        data = pd.read_sql(sql, con=hook.get_conn())
    if not data.empty:
        data['itemid'] = data['itemid'].astype('int64')  # bigint
        data['timestamp'] = pd.to_datetime(data['timestamp'], unit='s')
        data['description'] =data['value'].astype(object)
        data = data.drop('value', axis=1)
        kwargs['ti'].xcom_push(key='extracted_item_descriptions', value=data)
    else:
        logging.warning("No data extracted.")


def create_price_table():
    metadata = MetaData()
    price_table = Table(
        'item_price', 
        metadata,
        Column('itemid', BigInteger, primary_key=False),
        Column('timestamp', TIMESTAMP, primary_key=False),
        Column('value', Integer),
        UniqueConstraint('itemid', 'timestamp', name='price_stamp')
    )
    hook = PostgresHook('destination_db')
    metadata.create_all(hook.get_sqlalchemy_engine())


def create_descriptions_table():
    metadata = MetaData()
    descriptions_table = Table(
        'item_info',
        metadata,
        Column('itemid', Integer, primary_key=False),  # Autoincremented Integer ID
        Column('timestamp', TIMESTAMP, primary_key=False),
        Column('description', Text),  # Use Text for potentially long descriptions
        UniqueConstraint('itemid', 'timestamp', name='description_stamp')
    )

    # Get the Postgres connection and create the table
    hook = PostgresHook('destination_db')
    metadata.create_all(hook.get_sqlalchemy_engine())


def create_categories_table():
    metadata = MetaData()
    categories_table = Table(
        'item_categories',  # Correct table name for categories
        metadata,
        Column('itemid', Integer, primary_key=True),
        Column('categoryid', Integer),
        Column('parentid', Float),
        UniqueConstraint('itemid', name='itemid_unique')
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

        logging.info(f"Inserting {len(rows)} rows into item_price.")
        try:
            hook.insert_rows(
                table='item_price',
                rows=rows,
                target_fields=data_columns,
                replace=True,
                replace_index=['itemid','timestamp']  # Adjust based on your unique constraints
            )
            logging.info("Items prices loaded successfully.")
        except Exception as e:
            logging.error(f"Failed to insert rows into item_price: {e}")
    else:
        logging.error("No data available to load into item_price.")



def load_descriptions(**kwargs):
    hook = PostgresHook('destination_db')
    ti = kwargs['ti']

    # Pull data from XCom
    data_descriptions = ti.xcom_pull(task_ids='extract_item_descriptions', key='extracted_item_descriptions')
    
    # Check if data_prices is a list and convert to DataFrame if necessary
    if isinstance(data_descriptions, list):
        data_descriptions = pd.DataFrame(data_descriptions)

    # Logging the data pulled from XCom
    logging.info(f"Pulled data from XCom: {data_descriptions.head() if isinstance(data_descriptions, pd.DataFrame) else 'No data'}")
    
    # Check if the DataFrame is not empty
    if data_descriptions is not None and not data_descriptions.empty:
        # Reset the index to move 'itemid', 'timestamp', 'value' back to columns
        data_descriptions_reset = data_descriptions.reset_index(drop=True)

        # Prepare rows and columns for insertion
        rows = data_descriptions_reset.values.tolist()
        data_columns = data_descriptions_reset.columns.tolist()

        logging.info(f"Inserting {len(rows)} rows into item_info.")
        try:
            hook.insert_rows(
                table='item_info',
                rows=rows,
                target_fields=data_columns,
                replace=True,
                replace_index=['itemid', 'timestamp']  # Adjust based on your unique constraints
            )
            logging.info("Item info data loaded successfully.")
        except Exception as e:
            logging.error(f"Failed to insert rows into item_info: {e}")
    else:
        logging.error("No data available to load into item_info.")




def load_categories(**kwargs):
    hook = PostgresHook('destination_db')
    ti = kwargs['ti']

    # Pull data from XCom
    data_categories = ti.xcom_pull(task_ids='extract_item_categories', key='extracted_item_categories')
    
    # Check if data_prices is a list and convert to DataFrame if necessary
    if isinstance(data_categories, list):
        data_categories = pd.DataFrame(data_categories)

    # Logging the data pulled from XCom
    logging.info(f"Pulled data from XCom: {data_categories.head() if isinstance(data_categories, pd.DataFrame) else 'No data'}")
    
    # Check if the DataFrame is not empty
    if data_categories is not None and not data_categories.empty:
        # Reset the index to move 'itemid', 'timestamp', 'value' back to columns
        data_categories_reset = data_categories.reset_index(drop=True)

        # Prepare rows and columns for insertion
        rows = data_categories_reset.values.tolist()
        data_columns = data_categories_reset.columns.tolist()

        logging.info(f"Inserting {len(rows)} rows into item_categories.")
        try:
            hook.insert_rows(
                table='item_categories',
                rows=rows,
                target_fields=data_columns,
                replace=True,
                replace_index=['itemid']  # Adjust based on your unique constraints
            )
            logging.info("Item categories data loaded successfully.")
        except Exception as e:
            logging.error(f"Failed to insert rows into item_categories: {e}")
    else:
        logging.error("No data available to load into item_categories.")


