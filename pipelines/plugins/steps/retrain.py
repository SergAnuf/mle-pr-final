from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import MetaData, Table, Column, Integer, UniqueConstraint, Integer, Text
import pandas as pd
import logging
from sklearn.preprocessing import LabelEncoder
from implicit.als import AlternatingLeastSquares 
import scipy
import numpy as np
from dotenv import load_dotenv
import os
import boto3

load_dotenv()

def get_session_student():
    session = boto3.session.Session()
    return session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID_STUDENT'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY_STUDENT')
    )


class ALSRecommender:
    def __init__(self, config, events_train):
        """
        Parameters:
        - config: Гиперпараметры модели (K, FACTORS, ITERATIONS, REGULARIZATION)
        - view_but_no_cart: Датафрейм где юзеры смотрели но не добавили
        - add_to_cart: Датафрэйм где юзеры добавили в корзину товары
        - user_encoder: Энкодер для юзеров
        - category_encoder: Энкодер категорий 
        - events_train: Обучаемая выборка
        - events: Все евенты там есть где были покупки
        """
        self.config = config
        self.view_but_no_cart = events_train[(events_train["event"]!="addtocart") & (events_train["event"]=="view")]
        self.add_to_cart = events_train[events_train["event"]=="addtocart"]
        self.events_train = events_train
        self.als_model = None

    def build_interaction_matrix(self):
        """Создание матрицы взаимодействия пользователей с категориями товаров."""
        # Присвоение весов: K добавил в корзину, 1 смотрел только, 0 иначе
        # Товары которые купили были в корзине и им ноль не будет присущен
        scores = list(np.ones(self.view_but_no_cart.shape[0])) + list(np.ones(self.add_to_cart.shape[0]) * self.config["K"])

        idx_users = list(self.view_but_no_cart["user_id_enc"].values.astype(int)) + list(self.add_to_cart["user_id_enc"].values.astype(int))
        idx_items = list(self.view_but_no_cart["categoryid_enc"].values.astype(int)) + list(self.add_to_cart["categoryid_enc"].values.astype(int))
        
        user_item_matrix_train = scipy.sparse.csr_matrix((scores, (idx_users, idx_items)), dtype=np.int8)
        
        return user_item_matrix_train

    def train_als_model(self):
        """Обучение."""
        user_item_matrix_train = self.build_interaction_matrix()

        # Train ALS model
        self.als_model = AlternatingLeastSquares(
            factors=self.config["FACTORS"], 
            iterations=self.config["ITERATIONS"], 
            regularization=self.config["REGULARIZATION"], 
            random_state=0
        )
        self.als_model.fit(user_item_matrix_train)


    def sim_item_chunk(self, chunk_idx, max_similar_items=10):
        """Находим похожие категорий для листа категорий."""
        similar_items = self.als_model.similar_items(chunk_idx, N=max_similar_items+1)
        
        # Convert to DataFrame
        sim_item_item_ids_enc = similar_items[0]
        sim_item_scores = similar_items[1]
        similar_items_df = pd.DataFrame({
            "item_id_enc": chunk_idx,
            "sim_item_id_enc": sim_item_item_ids_enc.tolist(), 
            "score": sim_item_scores.tolist()
        })
        
        similar_items_df = similar_items_df.explode(["sim_item_id_enc", "score"], ignore_index=True)

        return similar_items_df

    def get_similar_categories(self, chunk_size=10000, max_similar_items=10):
        """находим общие айтемы для всех категорий."""
        unique_categories_train = self.events_train['categoryid_enc'].unique()
        num_rows = len(unique_categories_train)
        chunks = []
        
        for start in range(0, num_rows, chunk_size):
            end = min(start + chunk_size, num_rows)
            chunk_idx = unique_categories_train[start:end]
            chunk = self.sim_item_chunk(chunk_idx, max_similar_items=max_similar_items)
            chunks.append(chunk)
        
        # собираем все вместе
        similar_categories = pd.concat(chunks, axis=0)
        
        return similar_categories

    def recommend_items(self, N=30):
        """Generate ALS recommendations for all users."""
        if self.als_model is None:
            raise ValueError("ALS model has not been trained yet. Call `train_als_model` first.")
        
        user_ids_encoded = range(self.events_train['user_id_enc'].max() + 1)
        user_item_matrix_train = self.build_interaction_matrix()
        
        # Generate ALS recommendations
        als_recommendations = self.als_model.recommend(
            user_ids_encoded, 
            user_item_matrix_train[user_ids_encoded], 
            filter_already_liked_items=False, N=N # малая хитрость ставим False чтобы категории которые смотрели
        )                                         # алс модель не выкинула, а то что купили отфильтруется ниже

        item_ids_enc = als_recommendations[0]
        als_scores = als_recommendations[1]

        # Format recommendations into a DataFrame
        als_recommendations_df = pd.DataFrame({
            "user_id_enc": user_ids_encoded,
            "categoryid_enc": item_ids_enc.tolist(), 
            "score": als_scores.tolist()
        })

        chunk_size = 10000
        num_rows = len(als_recommendations_df)
        chunks = []
        count=0
        for start in range(0, num_rows, chunk_size):
            count+=1
            end = min(start + chunk_size, num_rows)
            chunk = als_recommendations_df.iloc[start:end]
            exploded = chunk.explode(['categoryid_enc','score'],ignore_index=True)
            chunks.append(exploded)

        als_predictions = pd.concat(chunks,axis=0)

        return als_predictions

    def filter_already_bought(self, als_recommendations): # на будущие нужно еще убрать из предсказаний товары которые не доступны (not available)
        """Убираем из рекомендаций категории которые юзеры уже покупали."""
        already_bought = self.events_train[self.events_train["event"] == "transaction"][["user_id_enc", "categoryid_enc"]]

        # кодировка юзеров и категорий товаров которые уже купили
        # already_bought["user_id_enc"] = self.user_encoder.transform(already_bought["visitorid"])
        # already_bought["categoryid_enc"] = self.category_encoder.transform(already_bought["categoryid"])

        # already_bought = already_bought.drop(columns=["visitorid", "categoryid"])

        # Filter out already bought categories
        filtered_recommendations = als_recommendations.merge(already_bought, on=['user_id_enc', 'categoryid_enc'], how='left', indicator=True)
        filtered_recommendations = filtered_recommendations[filtered_recommendations['_merge'] == 'left_only'].drop(columns=['_merge'])
        
        # Rank the filtered recommendations by score
        filtered_recommendations["rank"] = filtered_recommendations.groupby("user_id_enc")["score"].rank(method="first", ascending=False)
        
        return filtered_recommendations

    def get_filtered_recommendations(self):
        """Train ALS, generate recommendations, and filter out already bought items."""
        self.train_als_model()
        als_recommendations = self.recommend_items()
        return self.filter_already_bought(als_recommendations)



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
    
    # не могу сохранить 
    filtered_recommendations.to_parquet("../models/staging/offline_dag.parquet")
    similar_categories.to_parquet("../models/staging/online_dag.parquet")





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


