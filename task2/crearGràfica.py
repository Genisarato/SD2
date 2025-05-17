import csv
from datetime import datetime
import matplotlib.pyplot as plt

timestamps = []
active_workers = []

with open('stats_workers.csv', newline='') as f:
    reader = csv.DictReader(f)
    for row in reader:
        timestamps.append(datetime.fromisoformat(row['timestamp']))
        active_workers.append(int(row['active_workers']))

plt.plot(timestamps, active_workers, marker='o')
plt.xlabel('Temps')
plt.ylabel('Workers actius')
plt.title('Evolució dels workers actius al llarg del temps')
plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
