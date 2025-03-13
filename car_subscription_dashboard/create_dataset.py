import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Set random seed for reproducibility
np.random.seed(42)

# Generate 500 customer IDs
num_records = 500
customer_ids = [f"CUST_{str(i).zfill(4)}" for i in range(1, num_records + 1)]

# Define possible subscription types
subscription_types = ["Rideshare", "Personal", "Fleet"]

# Generate random start dates within a given range
start_dates = [datetime(2024, 1, 1) + timedelta(days=np.random.randint(0, 365)) for _ in range(num_records)]

# Generate random end dates (some will be missing)
end_dates = [start + timedelta(days=np.random.randint(60, 365)) if np.random.rand() > 0.25 else None for start in start_dates]

# Generate random utilization percentages
vehicle_utilization = np.round(np.random.uniform(70, 100, num_records), 1)

# Generate random idle days
idle_days = np.random.randint(0, 15, num_records)

# Generate random weekly revenue
weekly_revenue = np.random.randint(150, 500, num_records)

# Generate random churn values (0 or 1, with 30% churn rate)
churn = np.random.choice([0, 1], size=num_records, p=[0.7, 0.3])

# Generate random swap frequency
swap_frequency = np.random.randint(1, 5, num_records)

# Create DataFrame
df = pd.DataFrame({
    "Customer_ID": customer_ids,
    "Subscription_Type": np.random.choice(subscription_types, num_records),
    "Start_Date": [date.strftime("%Y-%m-%d") for date in start_dates],
    "End_Date": [date.strftime("%Y-%m-%d") if date else None for date in end_dates],
    "Vehicle_Utilization": vehicle_utilization,
    "Idle_Days": idle_days,
    "Weekly_Revenue_AUD": weekly_revenue,
    "Churn": churn,
    "Swap_Frequency": swap_frequency
})

# Save to CSV
df.to_csv("car_subscription_service.csv", index=False)

print("Mock dataset generated and saved as 'car_subscription_service.csv'")