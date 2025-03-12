import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import names
from faker import Faker  # Import Faker

fake = Faker()  # Initialize Faker

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

country_list = np.random.choice(countries, size=total_events)
city_list = [random.choice(city_country_map[country]) for country in country_list]

chunk_size = 100

def generate_formatted_ssn():
    digits = [str(random.randint(0, 9)) for _ in range(9)]
    return f"{''.join(digits[:3])}-{''.join(digits[3:5])}-{''.join(digits[5:])}"

user_ssn_map = {user_id: generate_formatted_ssn() for user_id in user_ids}
ssn_array = [user_ssn_map[user_id] for user_id in user_id_array]

def generate_credit_card_number():
    length = random.randint(15, 19)
    return "".join([str(random.randint(0, 9)) for _ in range(length)])

user_credit_card_map = {user_id: generate_credit_card_number() for user_id in user_ids}
credit_card_array = [user_credit_card_map[user_id] for user_id in user_id_array]

user_cvv_map = {user_id: "".join([str(random.randint(0, 9)) for _ in range(3)]) for user_id in user_ids}
cvv_array = [user_cvv_map[user_id] for user_id in user_id_array]

def generate_expiration_date():
    month = random.randint(1, 12)
    year = random.randint(23, 33)
    return f"{month:02d}-{year:02d}"

user_expiration_map = {user_id: generate_expiration_date() for user_id in user_ids}
expiration_array = [user_expiration_map[user_id] for user_id in user_id_array]

# Generate addresses for each user
user_address_map = {user_id: fake.street_address() for user_id in user_ids}
address_array = [user_address_map[user_id] for user_id in user_id_array]

def generate_phone_number(country):
    if country == "USA":
        return f"+1-{random.randint(200, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
    elif country == "United Kingdom":
        return f"+44-{random.randint(7000000000, 7999999999)}"
    elif country == "Japan":
        return f"+81-{random.randint(70, 90)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}"
    elif country == "India":
        return f"+91-{random.randint(6000000000, 9999999999)}"
    else: #generic format
        return f"+{random.randint(1, 999)}-{random.randint(1000000, 9999999999)}"

user_phone_map = {user_id: generate_phone_number(random.choice(countries)) for user_id in user_ids}
phone_array = [user_phone_map[user_id] for user_id in user_id_array]


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
    chunk_ssns = ssn_array[chunk_start:chunk_end]
    chunk_credit_cards = credit_card_array[chunk_start:chunk_end]
    chunk_cvvs = cvv_array[chunk_start:chunk_end]
    chunk_exps = expiration_array[chunk_start:chunk_end]
    chunk_addresses = address_array[chunk_start:chunk_end]
    chunk_phones = phone_array[chunk_start:chunk_end]


    unique_user_ids_in_chunk = np.unique(chunk_user_ids)
    user_name_map = {}
    for user_id in unique_user_ids_in_chunk:
        user_name_map[user_id] = names.get_full_name()
    chunk_names = [user_name_map[user_id] for user_id in chunk_user_ids]

    chunk_df = pd.DataFrame({
        "user_id": chunk_user_ids,
        "user_name": chunk_names,
        "ssn": chunk_ssns,
        "credit_card_number": chunk_credit_cards,
        "cvv": chunk_cvvs,
        "expiration_date": chunk_exps,
        "address": chunk_addresses,
        "timestamp": chunk_timestamps,
        "event_type": chunk_event_types,
        "product_id": chunk_product_ids,
        "price": chunk_prices,
        "category": chunk_categories,
        "city": chunk_cities,
        "country": chunk_countries,
        "phone_numer" : chunk_phones
    })

    print(f"Processing chunk from {chunk_start} to {chunk_end}")

    if chunk_start == 0:
        chunk_df.to_csv("user_transactions_complete_dataset.csv", index=False)
    else:
        chunk_df.to_csv("user_transactions_complete_dataset.csv", mode='a', header=False, index=False)