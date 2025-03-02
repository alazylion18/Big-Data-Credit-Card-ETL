import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

num_users = 1000000
min_events = 100
max_events = 600

user_ids = np.arange(num_users)
num_events_per_user = np.random.randint(min_events, max_events + 1, size=num_users)
total_events = np.sum(num_events_per_user)

user_id_array = np.repeat(user_ids, num_events_per_user)

start_time = datetime(2023, 1, 1, 0, 0, 0)
end_time = datetime(2023, 12, 31, 23, 59, 59)
time_delta = (end_time - start_time).total_seconds()

random_seconds = np.random.randint(0, int(time_delta), size=total_events)

# Convert numpy.int64 to native Python integers
timestamps = [start_time + timedelta(seconds=int(sec)) for sec in random_seconds]

# Generate event types
event_types = ["hold", "confirmation", "refund", "other"]
df = pd.DataFrame({
    "user_id": user_id_array,
    "timestamp": timestamps,
    "event_type": np.random.choice(event_types, size=total_events)
})

df = df.sample(frac=1).reset_index(drop=True)

df.to_csv("user_transactions_optimized.csv", index=False)

print(f"Generated {total_events} records.")