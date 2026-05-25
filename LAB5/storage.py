from kafka import KafkaConsumer
from pymongo import MongoClient

import json
from datetime import datetime

client = MongoClient("mongodb://localhost:27017/")

# Database

db = client["camera_system"]

# Collection

collection = db["detections"]

consumer = KafkaConsumer(
    'detections',
    bootstrap_servers='localhost:9092'
)

for msg in consumer:

    data = json.loads(msg.value)

    data["time"] = str(datetime.now())

    collection.insert_one(data)

    print("Saved:", data)
