import pandas as pd
import requests
import random
import time

# Define the endpoint URL and headers
events_store_url = "http://localhost:5050"
headers = {"Content-type": "application/json", "Accept": "text/plain"}

# Load the data from the parquet file
test_addtocart = pd.read_parquet("../app/events_monitor.parquet")
test_addtocart = test_addtocart.sort_values("timestamp", ascending=False)
test_addtocart = test_addtocart[["user_id_enc", "categoryid_enc"]]

# Set parameters for sending requests
total_duration = 1000  # Total duration for sending requests in seconds
requests_count = len(test_addtocart)  # Total number of requests to send
start_time = time.time()
end_time = start_time + total_duration
requests_sent = 0

# Send requests with the specified load
while time.time() < end_time and requests_sent < requests_count:
    for index, row in test_addtocart.iterrows():
        if requests_sent >= requests_count or time.time() >= end_time:
            break
        
        # Prepare the parameters from the row
        params = {"user_id_enc": row['user_id_enc'], "categoryid_enc": row['categoryid_enc']}
        
        # Send the POST request
        response = requests.post(events_store_url + "/put", headers=headers, params=params)

        # Print the response status and JSON response
        print(f"Запрос {requests_sent + 1} - Статус ответа: {response.status_code}, Ответ: {response.json()}")
        requests_sent += 1

        # Random pause between 1 and 4 seconds
        sleep_time = random.uniform(1, 4)
        print(f"Пауза {sleep_time:.2f} секунд")
        time.sleep(sleep_time)
