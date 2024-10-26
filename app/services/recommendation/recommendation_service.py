import logging
from fastapi import FastAPI
from contextlib import asynccontextmanager
import pandas as pd
import requests
import os
from dotenv import load_dotenv
import boto3
from io import BytesIO

# Load environment variables
load_dotenv()

features_store_url = "http://localhost:8010"
events_store_url = "http://localhost:8020"

PATH_S3_CATBOOST_MODEL = "recsys/recommendations/recommendations.parquet"
PATH_S3_TOP_POPULAR = "recsys/recommendations/top_popular.parquet"

class Recommendations:

    def __init__(self):

        self._recs = {"personal": None, "default": None}
        self._stats = {
            "request_personal_count": 0,
            "request_default_count": 0,
        }

    def load(self, type, s3_client, bucket_name, s3_key, **kwargs):
        """Загружает рекомендации из файла из S3"""

        logger.info(f" Начали загрузку ркомендаций: {type} из S3")
        buffer = BytesIO()
        
        try:
            s3_client.download_fileobj(Bucket=bucket_name, Key=s3_key, Fileobj=buffer)
            buffer.seek(0)

            self._recs[type] = pd.read_parquet(buffer, **kwargs)

            if self._recs[type].empty:
                logger.warning(f"Рекомендации {type} пустые.")
                raise ValueError(f"Загруженные рекомендации {type} пустые.")

            if type == "personal":
                self._recs[type] = self._recs[type].set_index("user_id")
                
            logger.info(f"Рекомендации {type} успешно загруженны из S3")

        except Exception as e:
            logger.error(f"Ошибка загрузки рекомендаций из S3: {e}")
            raise

    def get(self, user_id: int, k: int=20):
        """
        Возвращает список рекомендаций для пользователя
        """
        try:
            recs = self._recs["personal"].loc[user_id]
            recs = recs["item_id"].to_list()[:k]
            self._stats["request_personal_count"] += 1
        except KeyError:
            recs = self._recs["default"]
            recs = recs["item_id"].to_list()[:k]
            self._stats["request_default_count"] += 1
        except:
            logger.error("No recommendations found")
            recs = []

        return recs

    def stats(self):

        logger.info("Stats for recommendations")
        for name, value in self._stats.items():
            logger.info(f"{name:<30} {value} ") 


def get_session_student():
    # connect to s3 to load data
    session = boto3.session.Session()
    return session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID_STUDENT'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY_STUDENT'))


logger = logging.getLogger("uvicorn.error")
rec_store = Recommendations()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # код ниже (до yield) выполнится только один раз при запуске сервиса
    logger.info("Starting")

    s3_client = get_session_student()
    bucket_name = os.environ.get('S3_BUCKET_NAME')
    rec_store.load(
        type="personal",
        s3_client=s3_client,
        bucket_name=bucket_name,
        s3_key=PATH_S3_CATBOOST_MODEL,
        columns=["user_id", "item_id", "rank"]
    )
    rec_store.load(
        type="default",
        s3_client=s3_client,
        bucket_name=bucket_name,
        s3_key=PATH_S3_TOP_POPULAR,
        columns=["item_id", "rank"]
    )
    
    yield
    # этот код выполнится только один раз при остановке сервиса
    logger.info("Stopping")
    rec_store.stats()
    
# создаём приложение FastAPI
app = FastAPI(title="recommendations", lifespan=lifespan)



@app.post("/recommendations_offline/")
async def recommendations_offline(user_id: int, k: int = 20):
    """
    Возвращает список рекомендаций длиной k для пользователя user_id
    """

    recs = rec_store.get(user_id=user_id, k=k) 

    return {"recs": recs}


def dedup_ids(ids):
    """
    Дедублицирует список идентификаторов, оставляя только первое вхождение
    """
    seen = set()
    ids = [x for x in ids if not (x in seen or seen.add(x))]

    return ids

@app.post("/recommendations_online/")
async def recommendations_online(user_id: int, k: int = 20):
    """
    Возвращает список онлайн-рекомендаций длиной k для пользователя user_id
    """

    headers = {"Content-type": "application/json", "Accept": "text/plain"}
    # получаем список последних событий пользователя, возьмём три последних
    params = {"user_id": user_id, "k": 3}
    # ваш код здесь
    resp = requests.post(events_store_url + "/get", headers=headers, params=params)
    events_items = resp.json()["events"]

    # получаем список айтемов, похожих на последние три, с которыми взаимодействовал пользователь
    items = []
    scores = []
    for categoryid_enc in events_items:
        # для каждого item_id получаем список похожих в item_similar_items
        # ваш код здесь
        params = {"categoryid_enc": categoryid_enc,'k': k}
        resp = requests.post(features_store_url +"/similar_categories", headers=headers, params=params)
        item_similar_items = resp.json()
        
        items += item_similar_items["category_id_enc2"]
        scores += item_similar_items["score"]
    # сортируем похожие объекты по scores в убывающем порядке
    # для старта это приемлемый подход
    combined = list(zip(items, scores))
    combined = sorted(combined, key=lambda x: x[1], reverse=True)
    combined = [item for item, _ in combined]

    # удаляем дубликаты, чтобы не выдавать одинаковые рекомендации
    recs = dedup_ids(combined)[:k]

    return {"recs": recs}



@app.post("/recommendations/")
async def recommendations(user_id: int, k: int = 20):
    """
    Возвращает список рекомендаций длиной k для пользователя user_id
    """

    recs_offline = await recommendations_offline(user_id, k)
    recs_online = await recommendations_online(user_id, k)

    recs_offline = recs_offline["recs"]
    recs_online = recs_online["recs"]

    recs_blended = []

    min_length = min(len(recs_offline), len(recs_online))
    # чередуем элементы из списков, пока позволяет минимальная длина
    for i in range(min_length):
        # ваш код здесь #
        recs_blended.append(recs_offline[i])
        recs_blended.append(recs_online[i])

    # добавляем оставшиеся элементы в конец
    # ваш код здесь #
    k, m = len(recs_offline), len(recs_online)
    if k>min_length:
        recs_blended+=recs_offline[min_length:]
    if m>min_length:
        recs_blended+=recs_online[min_length:]
    # удаляем дубликаты
    recs_blended = dedup_ids(recs_blended)
    
        # оставляем только первые k рекомендаций
        # ваш код здесь #
    recs_blended = recs_blended[:k]

    return {"recs": recs_blended}