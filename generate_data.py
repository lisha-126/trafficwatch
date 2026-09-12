import pandas as pd
import random

roads = ["GT Road", "M-2 Motorway", "Shahrah-e-Faisal", "Ring Road", "Mall Road", "Peshawar Rd"]
types = ["ACCIDENT", "ROAD_WORKS", "TRAFFIC_JAM", "ROAD_CLOSURE"]
regions = ["Pakistan", "London"]

data = []
for i in range(2400):
    data.append({
        "id": i,
        "latitude": round(random.uniform(24, 37), 4),
        "longitude": round(random.uniform(67, 75), 4),
        "severity_score": round(random.uniform(1, 10), 1),
        "road_name": random.choice(roads),
        "type": random.choice(types),
        "region": random.choice(regions)
    })

pd.DataFrame(data).to_csv("data/traffic_accidents.csv", index=False)
print("Done! 2400 records generated.") 

