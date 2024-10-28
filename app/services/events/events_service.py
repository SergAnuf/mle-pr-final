from fastapi import FastAPI, HTTPException
import logging
from fastapi.responses import JSONResponse
from prometheus_client import Counter
from prometheus_fastapi_instrumentator import Instrumentator
import requests
from collections import defaultdict

recommendations_url = "http://recommendation:1300"

class EventStore:

    def __init__(self, max_events_per_user=10):
        self.events = {}
        self.max_events_per_user = max_events_per_user
        self.false_negatives = defaultdict(int)  # Store false negatives per user

    def put(self, user_id_enc, categoryid_enc, recommendations):
        """
        Сохраняет событие
        """
        user_events = self.events.get(user_id_enc, [])
        self.events[user_id_enc] = [categoryid_enc] + user_events[: self.max_events_per_user]

        interacted_items = set(user_events)  # All previously interacted items
        recommended_items = set(recommendations)  # Current recommendations

        true_positives = recommended_items & {categoryid_enc}  # Recommended and interacted
        false_positives = recommended_items - interacted_items  # Recommended but not interacted
        false_negatives = interacted_items - recommended_items  # Interacted but not recommended

        # Increment the stored FN count for the user
        self.false_negatives[user_id_enc] += len(false_negatives)

        return len(true_positives), len(false_positives), self.false_negatives[user_id_enc]

    def get_false_negatives(self, user_id_enc):
        """
        Returns the number of false negatives for a user.
        """
        return self.false_negatives[user_id_enc]

    def get(self, user_id_enc, k):
        """
        Возвращает события для пользователя
        """
        user_events = self.events.get(user_id_enc, [])

        return user_events


events_store = EventStore()

# создаём приложение FastAPI
app = FastAPI(title="events")
instrumentator = Instrumentator()
instrumentator.instrument(app).expose(app)

TP_counter = Counter("TP","True Positives")
FP_counter = Counter("FP","False Positives")
FN_counter = Counter("FN", "False Negatives")



@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    # перехватывает все необработанные исключения и возвращает сообщение об ошибке 500 с логированием
    logging.error(f"Произошла ошибка  {exc}")
    return JSONResponse(
        status_code=500,
        content={"message": "Internal Server Error"},
    )

@app.post("/put")
async def put(user_id_enc: int, categoryid_enc: int):
    """
    Сохраняет событие для user_id_enc, categoryid_enc
    """
    if not isinstance(user_id_enc, int) or not isinstance(categoryid_enc, int):
        raise HTTPException(status_code=400, detail="Не верные аргументы: user_id_enc and categoryid_enc должны быть integers.")
    
    # Соберем текущие рекомендации пользователя
    params = {"user_id_enc": user_id_enc, "k": 5}
    headers = {"Content-type": "application/json", "Accept": "text/plain"}
    response = requests.post(recommendations_url + "/recommendations_offline/", headers=headers, params=params)
    five_recommendations = response.json()["recs"]
    
    # найдем качество рекомендаций 
    try:
        tp, fp, fn = events_store.put(user_id_enc, categoryid_enc, five_recommendations)
        # Increment Prometheus counters
        TP_counter.inc(tp)
        FP_counter.inc(fp)
        FN_counter.inc(fn)

    except Exception as e:
        raise HTTPException(status_code=500, detail="Ошибка при сохранении евента")
    
    return {"result": "ok"}
 

@app.post("/get")
async def get(user_id_enc: int, k: int = 10):
    """
    Возвращает список последних k событий для пользователя user_id_enc
    """
   
    events = events_store.get(user_id_enc, k)
    return {"events": events}
  
    
