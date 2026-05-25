import cv2
import base64
import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092'
)

# Webcam
# cap = cv2.VideoCapture(0)

# Video file
cap = cv2.VideoCapture("test.mp4")

while True:

    ret, frame = cap.read()

    if not ret:
        break

    _, buffer = cv2.imencode('.jpg', frame)

    jpg_as_text = base64.b64encode(buffer).decode()

    data = {
        'image': jpg_as_text
    }

    producer.send(
        'frames',
        json.dumps(data).encode()
    )

    print("Frame sent")

cap.release()
