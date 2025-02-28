import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# Generate 10,000 user IDs
user_ids = np.arange(1000000, 3000000)

# Create a list to store the data
data = []

# Generate data for each user ID
for user_id in user_ids:
    # Generate a random number of events for this user (between 1 and 50)
    num_events = random.randint(100, 10000)

    # Generate random timestamps for each event
    start_time = datetime(2023, 1, 1, 0, 0, 0)  # Start date
    end_time = datetime(2023, 12, 31, 23, 59, 59)  # End date
    time_delta = (end_time - start_time).total_seconds()

    # Generate random timestamps within the specified range
    timestamps = [
        start_time + timedelta(seconds=random.randint(0, int(time_delta)))
        for _ in range(num_events)
    ]

    # Add the user ID and timestamps to the data list
    for timestamp in timestamps:
        data.append([user_id, timestamp])

# Create a Pandas DataFrame
df = pd.DataFrame(data, columns=["user_id", "timestamp"])

# Shuffle the DataFrame to randomize the order of events
df = df.sample(frac=1).reset_index(drop=True)

# Save the DataFrame to a CSV file

df.to_csv("user_timestamps.csv", index=False)