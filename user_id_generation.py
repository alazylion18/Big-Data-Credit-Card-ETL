import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import names

num_users = 1000000
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

# City-Country Mapping
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

# Generate product IDs
product_ids = np.random.randint(0, num_products, size=total_events)

# Generate prices and create a product-price mapping
unique_product_ids = np.unique(product_ids)
product_prices = {product_id: round(random.uniform(0, 10000), 2) for product_id in unique_product_ids}
prices = [product_prices[pid] for pid in product_ids]

# Generate full names for each user
def generate_full_names(num_users):
    full_names = []
    for _ in range(num_users):
        full_names.append(names.get_full_name())
    return full_names

user_names = generate_full_names(num_users)

# Create a user_id to name mapping.
user_name_map = dict(zip(user_ids, user_names))

# Map the names to the user_id_array.
names_array = [user_name_map[user_id] for user_id in user_id_array]

# Generate city and country lists ensuring city is in country
country_list = np.random.choice(countries, size=total_events)
city_list = []
for country in country_list:
    cities_in_country = city_country_map[country]
    city_list.append(random.choice(cities_in_country))

df = pd.DataFrame({
    "user_id": user_id_array,
    "user_name": names_array,
    "timestamp": timestamps,
    "event_type": np.random.choice(event_types, size=total_events),
    "product_id": product_ids,
    "price": prices,
    "category": np.random.choice(categories, size=total_events),
    "city": city_list,
    "country": country_list
})

df = df.sample(frac=1).reset_index(drop=True)

df.to_csv("user_transactions_optimized_with_products_and_categories_and_names_and_correct_locations.csv", index=False)

print(f"Generated {total_events} records with product IDs, prices, categories, cities, countries, and names.")