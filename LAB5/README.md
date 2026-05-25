# People Counting System using Kafka + YOLO + MongoDB

## Mô tả

Hệ thống đếm số lượng người realtime từ camera/video bằng công nghệ Big Data.

Hệ thống gồm:

- Producer Server nhận video/camera và gửi frame vào Kafka.
- Processing Server dùng YOLOv8 nhận diện người.
- Storage Server lưu kết quả vào MongoDB.

## Công nghệ sử dụng

- Python
- Apache Kafka
- Docker
- MongoDB
- YOLOv8
- OpenCV

## Kiến trúc hệ thống

Camera/Video
↓
Producer Server
↓ Kafka Topic: frames
Processing Server (YOLOv8)
↓ Kafka Topic: detections
Storage Server (MongoDB)

## Cài đặt

### Chạy Kafka

```bash
sudo docker-compose up -d
