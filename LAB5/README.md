# People Counting System using Kafka + YOLO + MongoDB

## Mô tả

Hệ thống đếm số lượng người realtime từ camera/video bằng công nghệ Big Data.

Hệ thống gồm:

- Producer Server nhận video/camera và gửi frame vào Kafka.
- Processing Server dùng YOLOv8 nhận diện người.
- Storage Server lưu kết quả vào MongoDB.


## Giới thiệu

Hệ thống đếm số lượng người xuất hiện trong camera sử dụng:

- Apache Kafka
- YOLOv8
- MongoDB
- Docker
- OpenCV
- Flask

Hệ thống được xây dựng theo kiến trúc phân tán nhằm mô phỏng môi trường xử lý dữ liệu lớn thời gian thực.

---

# Kiến trúc hệ thống

Camera/Webcam
↓
Receiver Server
↓
Apache Kafka
↓
Detection Server (YOLOv8)
↓
Storage Server
↓
MongoDB

---

# Công nghệ sử dụng

- Python
- Flask
- OpenCV
- Apache Kafka
- YOLOv8
- MongoDB
- Docker

---

# Chức năng hệ thống

## Receiver Server

- Nhận video từ webcam
- Chuyển frame sang bytes
- Gửi dữ liệu vào Kafka

## Detection Server

- Nhận frame từ Kafka
- Detect người bằng YOLOv8
- Trả về bounding box
- Đếm số lượng người

## Storage Server

- Nhận kết quả detect
- Lưu MongoDB

cd people-counting-system
