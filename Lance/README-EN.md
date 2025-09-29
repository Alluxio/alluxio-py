# AI Model and Dataset Lifecycle Management System

This project is built on **Lance** and **Alluxio-Py**, providing a unified pipeline for **model / dataset downloading, conversion, storage, and training**.
It supports converting **PyTorch `.pth` model weights** and **image / text / table datasets** into the Lance format, storing them in **S3 / MinIO**, and then directly loading them into **PyTorch / TensorFlow** for training.

---

## ✨ Features

### 📥 Downloader
- **ModelDownloader**: Supports downloading models from HuggingFace Hub / URL / Transformers.
- **DatasetDownloader**: Supports downloading datasets from HuggingFace Datasets (Arrow/Repo).

### 🔄 Converter
- **TensorConverter**: Converts `.pth` / `.pt` / `.safetensors` model weights into Lance.
- **PngConverter**: Converts image files or Arrow-format image datasets into Lance.
- **TextConverter**: Converts `.txt` / `.json` / `.csv` text data into Lance.
- **ParquetConverter**: Converts `.zip` / `.parquet` compressed or tabular files into Lance.

### ☁️ Storage Support
- Seamless integration with **S3 / MinIO** (`s3_options.py`).
- Lance datasets support **overwrite / append** write modes.

### 🧑‍💻 Training Demo
- **Image classification task**: `demo_cv_resnet_train.py`
  Uses ResNet18 + CIFAR10 dataset for quick training.

### 🔗 Framework Integration
- **PyTorch**: `LanceTorchDataset`

### 📦 Dataset & Model Management
- **DataManager**: Provides **search / rollback / register / delete** functionalities.

### ⚙️ Engine Modes
- **Local**: Call DataManager directly in local mode.
- **Remote**: Access DataManager via **HTTP / gRPC / WebSocket**.

---

## 🚀 Usage

### Demo

Example command to run **ResNet18 + CIFAR10** training and upload both model and dataset to MinIO:

```bash
python Lance/src/blender/main/demo/demo_cv_resnet_train.py   --model https://download.pytorch.org/models/resnet18-f37072fd.pth   --model_mode url   --dataset cifar10   --dataset_mode arrow   --split train   --limit 500   --batch 32   --epochs 1   --steps 50   --s3-endpoint http://localhost:9000   --aws-access-key minioadmin   --aws-secret-key minioadmin123   --aws-region us-east-1   --bucket my-bucket   --prefix resnet_demo
```

### proto

Instructions for generating `pb2` and `pb2_grpc` from `.proto`:

```bash
python -m grpc_tools.protoc -I . --pyi_out=. --python_out=. --grpc_python_out=. test.proto
```

---

## ⚙️ Demo Parameters

| Parameter           | Default Value                                               | Description                                               |
| ------------------- | ----------------------------------------------------------- | --------------------------------------------------------- |
| `--cache`           | `./_cache`                                                  | Local cache path for models and datasets                  |
| `--model`           | `https://download.pytorch.org/models/resnet18-f37072fd.pth` | Model path (URL / HuggingFace Repo / Transformers)        |
| `--model_mode`      | `url`                                                       | Model download mode: `url` / `repo` / `transformers`      |
| `--dataset`         | `cifar10`                                                   | Dataset name                                              |
| `--dataset_mode`    | `arrow`                                                     | Dataset mode: `repo` / `arrow`                            |
| `--split`           | `train`                                                     | Dataset split: e.g., `train` / `test`                     |
| `--limit`           | `1000`                                                      | Limit on number of samples loaded                         |
| `--batch`           | `32`                                                        | Batch size                                                |
| `--epochs`          | `1`                                                         | Number of training epochs                                 |
| `--steps`           | `50`                                                        | Iterations per epoch                                      |
| `--s3-endpoint`     | `http://localhost:9000`                                     | S3/MinIO service endpoint                                 |
| `--aws-access-key`  | `minioadmin`                                                | MinIO access key                                          |
| `--aws-secret-key`  | `minioadmin123`                                             | MinIO secret key                                          |
| `--aws-region`      | `us-east-1`                                                 | AWS region (default: us-east-1)                           |
| `--bucket`          | `my-bucket`                                                 | S3/MinIO bucket name                                      |
| `--prefix`          | `resnet_demo`                                               | Storage prefix (project namespace)                        |

---

## 📌 Roadmap
- [ ] Support more deep learning frameworks (TF, JAX, MindSpore).
- [ ] Enhance multimodal dataset support (audio, video, point cloud).
- [ ] Improve fault tolerance and enrich download/convert functionalities.

---

## 📜 License
This project follows the **Apache 2.0 License**.
