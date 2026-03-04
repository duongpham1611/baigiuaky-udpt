# Distributed Key-Value Store

Dự án này triển khai một hệ thống lưu trữ **Key-Value phân tán** đơn giản bằng Python, sử dụng gRPC để giao tiếp giữa các node

## 🚀 Tính năng chính

- **Lưu trữ Key-Value**: Hỗ trợ các thao tác cơ bản `PUT`, `GET`, `DELETE`.
- **Phân tán & Nhân bản**: Dữ liệu được nhân bản qua các node để đảm bảo tính sẵn sàng.
- **Persistence**: Dữ liệu được lưu trữ dưới dạng JSON trên ổ đĩa (`node*_data.json`).
- **Web UI**: Giao diện người dùng trực quan để quản lý và truy vấn dữ liệu.
- **gRPC**: Sử dụng gRPC cho hiệu suất cao trong giao tiếp giữa các thành phần.
- **Fault Tolerance**: Cơ chế Heartbeat để giám sát trạng thái các node.

## 📂 Cấu trúc thư mục

```text
.
├── protos/                # Định nghĩa gRPC (.proto)
├── src/                   # Mã nguồn chính
│   ├── node.py            # Logic của một Node
│   ├── client.py          # Client API
│   ├── config.json        # Cấu hình mạng (IP/Port)
│   └── service_pb2*.py    # Code gRPC được sinh ra
├── templates/             # Giao diện HTML
├── run_cluster.py         # Script chạy nhanh 3 node
├── web_ui.py              # Server Web (Flask)
├── test_integration.py    # Kiểm thử tích hợp
└── node*_data.json        # Dữ liệu lưu trữ của từng node
```

## 🛠 Cài đặt

1. **Cài đặt Python 3.8+**
2. **Cài đặt thư viện cần thiết:**
   ```bash
   pip install grpcio grpcio-tools flask
   ```
3. **(Tùy chọn) Sinh lại code từ file proto:**
   ```bash
   python -m grpc_tools.protoc -I./protos --python_out=./src --grpc_python_out=./src ./protos/service.proto
   ```

## 📖 Cách sử dụng

### 1. Chạy cụm Server (Cluster)
Sử dụng script `run_cluster.py` để khởi động nhanh 3 node trong 3 cửa sổ console khác nhau:
```bash
python run_cluster.py
```

### 2. Chạy Web UI
Sau khi các node đã chạy, khởi động giao diện web:
```bash
python web_ui.py
```
Truy cập: `http://localhost:5000`

### 3. Kiểm thử
Chạy script test để kiểm tra tính đúng đắn của hệ thống:
```bash
python test_integration.py
```
