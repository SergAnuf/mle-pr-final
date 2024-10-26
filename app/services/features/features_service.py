import logging
from contextlib import asynccontextmanager
from fastapi.responses import FileResponse
import pandas as pd
from fastapi import FastAPI
import os
from dotenv import load_dotenv
import boto3
from io import BytesIO
from fastapi import HTTPException

# Load environment variables
load_dotenv()

logger = logging.getLogger("uvicorn.error")

PATH_S3_SIMILAR = "recsys/recommendations/online_4.parquet"

class SimilarItems:

    def __init__(self):

        self._similar_items = None

    def load(self,  s3_client, bucket_name, s3_key, **kwargs):
        """
        Загружаем данные из файла
        """

        logger.info(f"Loading data, type: {type}")
        # self._similar_items = pd.read_parquet(path)

        buffer = BytesIO()
        s3_client.download_fileobj(Bucket=bucket_name, Key=s3_key, Fileobj=buffer)
        buffer.seek(0)
        self._similar_items = pd.read_parquet(buffer, **kwargs)
        self._similar_items = self._similar_items.set_index("category_id_enc1")

        logger.info(f"Loaded")

    def get(self, categoryid_enc: int, k: int = 10):
        """
        Возвращает список похожих объектов
        """
        try:
            i2i = self._similar_items.loc[categoryid_enc].head(k)
            i2i = i2i[["category_id_enc2", "score"]].to_dict(orient="list")
        except KeyError:
            logger.error("No recommendations found")
            i2i = {"category_id_enc2": [], "score": {}}

        return i2i

    
def get_session_student():
    # connect to s3 to load data
    session = boto3.session.Session()
    return session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID_STUDENT'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY_STUDENT'))

sim_items_store = SimilarItems()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # код ниже (до yield) выполнится только один раз при запуске сервиса
    s3_client = get_session_student()
    bucket_name = os.environ.get('S3_BUCKET_NAME')

    sim_items_store.load(
        s3_client=s3_client,
        bucket_name=bucket_name,
        s3_key=PATH_S3_SIMILAR,
        columns=["category_id_enc1", "category_id_enc2", "score"],
    )
    logger.info("Ready!")
    # код ниже выполнится только один раз при остановке сервиса
    yield

# создаём приложение FastAPI
app = FastAPI(title="features", lifespan=lifespan)


@app.post("/similar_categories")
async def recommendations(categoryid_enc: int, k: int = 10):
    if not isinstance(categoryid_enc, int) or not isinstance(k, int) or k <= 0:
        raise HTTPException(status_code=400, detail="Invalid arguments.")
    try:
        return sim_items_store.get(categoryid_enc, k)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))