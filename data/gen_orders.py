import csv
import random
from datetime import datetime, timedelta

rows = []

start_date = datetime(2026, 1, 1)
end_date = datetime(2026, 6, 30)

total_days = (end_date - start_date).days

total_num = 100000 # レコード数

for i in range(1, total_num+1):
    random_days = random.randint(0, total_days)
    dt = start_date + timedelta(days=random_days)

    rows.append([
        f"o-{i}",                                             # order_id
        f"u-{random.randint(1, 20000)}",                      # user_id
        dt.strftime("%Y-%m-%dT%H:%M:%SZ"),                    # order_ts
        round(random.uniform(100, 50000), 2),                 # amount
        "JPY",                                                # currency
        random.choice(["jp", "JP", "us"]),                    # country
        random.choice(["COMPLETED", "REFUNDED", "CANCELLED"]) # status
    ])

with open("orders_large.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["order_id","user_id","order_ts","amount","currency","country","status"])
    writer.writerows(rows)
