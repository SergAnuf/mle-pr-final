from implicit.als import AlternatingLeastSquares 
import scipy
import numpy as np
import pandas as pd




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

