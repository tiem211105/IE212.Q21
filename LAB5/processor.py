from ultralytics import YOLO
consumer = KafkaConsumer(
    'frames',
    bootstrap_servers='localhost:9092'
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092'
)

for msg in consumer:

    data = json.loads(msg.value)

    image_data = base64.b64decode(data['image'])

    np_arr = np.frombuffer(image_data, np.uint8)

    frame = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)

    results = model(frame)

    people = []

    for r in results:

        for box in r.boxes:

            cls = int(box.cls[0])

            # class 0 = person
            if cls == 0:

                x1, y1, x2, y2 = map(int, box.xyxy[0])

                people.append({
                    "x1": x1,
                    "y1": y1,
                    "x2": x2,
                    "y2": y2
                })

                cv2.rectangle(
                    frame,
                    (x1, y1),
                    (x2, y2),
                    (0, 255, 0),
                    2
                )

    result = {
        "count": len(people),
        "boxes": people
    }

    producer.send(
        'detections',
        json.dumps(result).encode()
    )

    print("People count:", len(people))

    cv2.imshow("People Detection", frame)

    if cv2.waitKey(1) == ord('q'):
        break

cv2.destroyAllWindows()
