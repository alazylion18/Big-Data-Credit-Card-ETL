import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

num_users = 500000
min_events = 100
max_events = 300
num_products = 100

user_ids = np.arange(num_users)
num_events_per_user = np.random.randint(min_events, max_events + 1, size=num_users)
total_events = np.sum(num_events_per_user)

user_id_array = np.repeat(user_ids, num_events_per_user)

start_time = datetime(2023, 1, 1, 0, 0, 0)
end_time = datetime(2023, 12, 31, 23, 59, 59)
time_delta = (end_time - start_time).total_seconds()

random_seconds = np.random.randint(0, int(time_delta), size=total_events)
timestamps = [start_time + timedelta(seconds=int(sec)) for sec in random_seconds]

event_types = ["hold", "confirmation", "refund", "other"]
categories = ["electronics", "clothing", "books", "furniture", "toys", "sports", "beauty", "tools", "food", "other"]

city_country_map = {
    "USA": ["New York", "Chicago", "Los Angeles", "Houston", "Philadelphia"],
    "United Kingdom": ["London", "Manchester", "Liverpool", "Edinburgh"],
    "Japan": ["Tokyo", "Osaka", "Kyoto"],
    "Australia": ["Sydney", "Melbourne", "Brisbane"],
    "France": ["Paris"],
    "Brazil": ["Rio de Janeiro", "Sao Paulo", "Santiago"],
    "Egypt": ["Cairo"],
    "China": ["Beijing", "Shanghai", "Guangzhou"],
    "Canada": ["Toronto", "Vancouver", "Montreal"],
    "India": ["Mumbai", "Delhi", "Kolkata"],
    "Germany": ["Berlin", "Frankfurt"],
    "Spain": ["Madrid", "Barcelona"],
    "Italy": ["Rome", "Milan"],
    "Argentina": ["Buenos Aires"],
    "Mexico": ["Mexico City", "Monterrey"],
    "Netherlands": ["Amsterdam"],
    "South Korea": ["Seoul"],
    "South Africa": ["Cape Town", "Johannesburg"],
    "Sweden": ["Stockholm"],
    "Ireland": ["Dublin"],
    "Nigeria": ["Lagos"],
    "Colombia": ["Bogota"],
    "Belgium": ["Brussels"],
    "Singapore": ["Singapore"],
    "Norway": ["Oslo"],
    "Kenya": ["Nairobi"]
}

countries = list(city_country_map.keys())

product_ids = np.random.randint(0, num_products, size=total_events)
unique_product_ids = np.unique(product_ids)
product_prices = {product_id: round(random.uniform(0, 10000), 2) for product_id in unique_product_ids}
prices = [product_prices[pid] for pid in product_ids]

# Simple User Name Generation
user_names = [f"user_{i}" for i in range(num_users)]
user_name_map = dict(zip(user_ids, user_names))
names_array = [user_name_map[user_id] for user_id in user_id_array]

# Optimized City-Country Generation
country_list = np.random.choice(countries, size=total_events)
city_list = [random.choice(city_country_map[country]) for country in country_list]

chunk_size = 100

for chunk_start in range(0, total_events, chunk_size):
    chunk_end = min(chunk_start + chunk_size, total_events)

    chunk_user_ids = user_id_array[chunk_start:chunk_end]
    chunk_timestamps = timestamps[chunk_start:chunk_end]
    chunk_product_ids = product_ids[chunk_start:chunk_end]
    chunk_prices = prices[chunk_start:chunk_end]
    chunk_categories = np.random.choice(categories, size=chunk_end - chunk_start)
    chunk_countries = country_list[chunk_start:chunk_end]
    chunk_cities = city_list[chunk_start:chunk_end]
    chunk_event_types = np.random.choice(event_types, size=chunk_end - chunk_start)
    chunk_names = names_array[chunk_start:chunk_end]

    chunk_df = pd.DataFrame({
        "user_id": chunk_user_ids,
        "user_name": chunk_names,
        "timestamp": chunk_timestamps,
        "event_type": chunk_event_types,
        "product_id": chunk_product_ids,
        "price": chunk_prices,
        "category": chunk_categories,
        "city": chunk_cities,
        "country": chunk_countries
    })

    print(f"Processing chunk from {chunk_start} to {chunk_end}")

    if chunk_start == 0:
        chunk_df.to_csv("user_transactions_optimized_with_products_and_categories_and_names_and_correct_locations.csv", index=False)
    else:
        chunk_df.to_csv("user_transactions_optimized_with_products_and_categories_and_names_and_correct_locations.csv", mode='a', header=False, index=False)

print(f"Generated {total_events} records with product IDs, prices, categories, cities, countries, and names.")