from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from sqlalchemy import MetaData, Table, Column, Integer, Float, String, UniqueConstraint,DateTime
import logging

def remove_duplicated(df):
    df = df.set_index('itemid')
    return df[~df.duplicated()]

def remove_outliers(df, features, coef=1.5):
    for feature in features:
        Q1 = df[feature].quantile(0.25)
        Q3 = df[feature].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - coef * IQR
        upper_bound = Q3 + coef * IQR
        df = df[(df[feature] >= lower_bound) & (df[feature] <= upper_bound)]
    return df

def create_clean_table():
    metadata = MetaData()
    items_table = Table(
        'item_info',
        metadata,
        Column('itemid', Integer, primary_key=True, autoincrement=True),
        Column('price', Float),
        Column('product_description', String),
        Column('categoryid', Integer),
        Column('parentid', Integer),
        UniqueConstraint('itemid', name='itemid')
    )
    hook = PostgresHook('destination_db')
    metadata.create_all(hook.get_sqlalchemy_engine())


def create_price_table():
    metadata = MetaData()
    price_table = Table(
        'price_data',  # Table for item prices with time series
        metadata,
        Column('itemid', Integer, primary_key=False),  # No autoincrement, as same item can appear with different timestamps
        Column('timestamp', DateTime, primary_key=False),  # Can use DateTime if needed
        Column('value', Float),
        UniqueConstraint('itemid', 'timestamp', name='unique_price_entry')  # Ensure uniqueness of item and timestamp
    )
    hook = PostgresHook('destination_db')
    metadata.create_all(hook.get_sqlalchemy_engine())  



def extract_item_categories(**kwargs):
    ti = kwargs['ti']
    hook = PostgresHook('destination_db')
    conn = hook.get_conn()
    
    sql_file_path = '/opt/airflow/scripts/extract_item_categories.sql'
    try:
        with open(sql_file_path, 'r') as file:
            sql = file.read()
        data = pd.read_sql(sql, conn)
        ti.xcom_push(key='extracted_item_categories', value=data)
    except FileNotFoundError:
        logging.error("SQL file not found. Please check the path.")
    finally:
        conn.close()

def extract_item_prices(**kwargs):
    ti = kwargs['ti']
    hook = PostgresHook('destination_db')
    conn = hook.get_conn()

    sql_file_path = '/opt/airflow/scripts/extract_item_prices.sql'
    try:
        with open(sql_file_path, 'r') as file:
            sql = file.read()
        data = pd.read_sql(sql, conn)
        ti.xcom_push(key='extracted_item_prices', value=data)
    except FileNotFoundError:
        logging.error("SQL file not found. Please check the path.")
    finally:
        conn.close()

def extract_item_descriptions(**kwargs):
    ti = kwargs['ti']
    hook = PostgresHook('destination_db')
    conn = hook.get_conn()

    sql_file_path = '/opt/airflow/scripts/extract_item_descriptions.sql'
    try:
        with open(sql_file_path, 'r') as file:
            sql = file.read()
        data = pd.read_sql(sql, conn)
        ti.xcom_push(key='extracted_item_descriptions', value=data)
    except FileNotFoundError:
        logging.error("SQL file not found. Please check the path.")
    finally:
        conn.close()

def transform_price_table(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='extract_item_prices', key='extracted_item_prices')
    df_mean = df.groupby("itemid", as_index=False).agg({"value": "mean"}).reset_index()
    df_cleaned = remove_outliers(df_mean, ['value'], coef=4)
    ti.xcom_push('transformed_price_data', df_cleaned)

def transform_descriptions_table(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='extract_item_descriptions', key='extracted_item_descriptions')
    df = remove_duplicated(df)
    # df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # middle_data = (
    #     df.groupby('itemid')
    #     .apply(lambda x: x.iloc[len(x) // 2])
    #     .reset_index(drop=True)
    # )

    # middle_data = middle_data[['itemid', 'value']]  # Ensure 'value' exists in the original df
    ti.xcom_push('transformed_descriptions_data', df)

def load_item_table(**kwargs):
    hook = PostgresHook('destination_db')
    ti = kwargs['ti']

    data_categories = ti.xcom_pull(task_ids='extract_item_categories', key='extracted_item_categories')
    data_prices = ti.xcom_pull(task_ids='transform_price_table', key='transformed_price_data')
    data_descriptions = ti.xcom_pull(task_ids='transform_descriptions_table', key='transformed_descriptions_data')

    data_categories['categoryid'] = data_categories['categoryid'].astype('int32')
    data_prices['value'] = data_prices['value'].astype('float32')  # Ensure consistency with your column naming

    batch_size = 10000
    for start in range(0, len(data_prices), batch_size):
        batch_prices = data_prices[start:start + batch_size]
        merged_data = pd.merge(batch_prices, data_descriptions, on='itemid', how='outer')
        merged_data = pd.merge(merged_data, data_categories, on='itemid', how='outer')

        merged_data['value'].fillna('No description', inplace=True)
        merged_data['categoryid'].fillna(-1, inplace=True)
        merged_data['price'].fillna(0.0, inplace=True)

        rows = merged_data.to_dict(orient='records')
        try:
            hook.insert_rows(
                table="item_data",
                rows=rows,
                target_fields=merged_data.columns.tolist(),
                replace=True
            )
        except Exception as e:
            logging.error(f"Failed to insert rows into item_data: {e}")