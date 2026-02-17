import csv
import random
from datetime import datetime, timedelta

rows = []
base_date = datetime(2026, 2, 1)

total_num = 20000 # レコード数

for i in range(1, total_num+1):
    dt = base_date + timedelta(days=random.randint(0, 9))
    rows.append([
        f"o-{i}",                                             # order_id
        f"u-{random.randint(1, 5000)}",                       # user_id
        dt.strftime("%Y-%m-%dT%H:%M:%SZ"),                    # order_ts
        round(random.uniform(100, 20000), 2),                 # amount
        "JPY",                                                # currency
        random.choice(["jp", "JP", "us"]),                    # country
        random.choice(["COMPLETED", "REFUNDED", "CANCELLED"]) # status
    ])

with open("orders_large.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["order_id","user_id","order_ts","amount","currency","country","status"])
    writer.writerows(rows)
