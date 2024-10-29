1. Launch app:

cd docker
docker compose up --build

2. Test monitor service:

cd app
python call_service.py

cd monitor
monitoring.md -> пояснение какие метрики в онлайн режиме следили за app
monititoring_dashboard.json -> dashboard мониторинга в графане


3. EDA / Modelling:

cd notebooks
A) EDA выводы -> находяться в ноутбуке
B) Modelling:

Модель однаступенчатая ALS - учитывает только взаимодействие пользователей с категорией товаров

матрица пользователь - категории:
1 - пользователь смотрел товар но не добавил в корзину
K - integer param > 1, пользователь добавил в корзину

далее обучение

в предсказаниях ставим чтобы не убирать евенты смотрел толкько
filter_already_liked_items=False

в конце фильтр рекомендаций которые пользователь уже добавил (где K значение в юзер-айтем матрице)

C) Track experiments Mlflow server:
cd mlflow_server
bash run_services.sh


4. Retrain 

cd pipelines 
docker compose -up build

DAG code : dags/retrain.py
DAG name : "model_retrain"
