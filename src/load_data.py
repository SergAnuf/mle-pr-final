import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
import os 
import psycopg

# подгружаем .env
load_dotenv()


connection = {"sslmode": "require", "target_session_attrs": "read-write"}
postgres_credentials = {
    "host": os.getenv("DB_DESTINATION_HOST"),
    "port": os.getenv("DB_DESTINATION_PORT"),
    "dbname": os.getenv("DB_DESTINATION_NAME"),
    "user": os.getenv("DB_DESTINATION_USER"),
    "password": os.getenv("DB_DESTINATION_PASSWORD"),
}

# Create a connection string
connection_string = (
    f"postgresql://{postgres_credentials['user']}:{postgres_credentials['password']}"
    f"@{postgres_credentials['host']}:{postgres_credentials['port']}/{postgres_credentials['dbname']}"
)

# Create a SQLAlchemy engine
engine = create_engine(connection_string)


def load_postgres(TABLE_NAME):
    "LOAD TABLE FROM DATABASE"
    connection.update(postgres_credentials)
    with psycopg.connect(**connection) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {TABLE_NAME}")
            data = cur.fetchall()
            columns = [col[0] for col in cur.description]
    df = pd.DataFrame(data, columns=columns)

    return df


def upload(df, TABLE_NAME):
    "UPLOAD DATAFRAME AS TABLE TO DATABASE"
    try:
        df.to_sql(TABLE_NAME, engine, if_exists='replace', index=False)
        print(f"Data loaded successfully into {TABLE_NAME}")
    except Exception as e:
        print(f"An error occurred while loading data: {e}")