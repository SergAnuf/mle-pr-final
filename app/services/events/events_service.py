from fastapi import FastAPI, HTTPException
import logging
from fastapi.responses import JSONResponse

class EventStore:

    def __init__(self, max_events_per_user=10):

        self.events = {}
        self.max_events_per_user = max_events_per_user

    def put(self, user_id, item_id):
        """
        Сохраняет событие
        """

        user_events = self.events.get(user_id, [])
        self.events[user_id] = [item_id] + user_events[: self.max_events_per_user]

    def get(self, user_id, k):
        """
        Возвращает события для пользователя
        """
        user_events = self.events.get(user_id, [])

        return user_events

events_store = EventStore()

# создаём приложение FastAPI
app = FastAPI(title="events")

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    # перехватывает все необработанные исключения и возвращает сообщение об ошибке 500 с логированием
    logging.error(f"Произошла ошибка  {exc}")
    return JSONResponse(
        status_code=500,
        content={"message": "Internal Server Error"},
    )

@app.post("/put")
async def put(user_id: int, item_id: int):
    """
    Сохраняет событие для user_id, item_id
    """
    if not isinstance(user_id, int) or not isinstance(item_id, int):
        raise HTTPException(status_code=400, detail="Не верные аргументы: user_id and item_id должны быть integers.")
    try:
        events_store.put(user_id, item_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail="Ошибка при сохранении евента")
    
    return {"result": "ok"}
 

@app.post("/get")
async def get(user_id: int, k: int = 10):
    """
    Возвращает список последних k событий для пользователя user_id
    """
    if not isinstance(user_id, int) or not isinstance(k, int):
        raise HTTPException(status_code=400, detail="Не верные аргументы: user_id и k должны быть целыми числами.")
    
    try:
        events = events_store.get(user_id, k)
        return {"events": events}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Ошибка при получении событий: " + str(e))
    
