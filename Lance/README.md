# AI 模型与数据集生命周期管理系统

本项目基于 **Lance** 与 **Alluxio-Py**，实现了一个统一的 **模型 / 数据集下载、转换、存储与训练管道**。
支持将 **PyTorch `.pth` 模型权重** 与 **图片 / 文本 / 表格数据集** 转换为 Lance 格式，并存储到 **S3 / MinIO**，随后可直接在 **PyTorch / TensorFlow** 中加载并训练。

---

## ✨ 功能特性

### 📥 下载器 (Downloader)

- **ModelDownloader**
  支持从 HuggingFace Hub / URL / Transformers 下载模型。
- **DatasetDownloader**
  支持从 HuggingFace Datasets (Arrow/Repo) 下载数据集。

### 🔄 转换器 (Converter)

- **TensorConverter**：将 `.pth` / `.pt` / `.safetensors` 模型权重转为 Lance
- **PngConverter**：将图片文件或 Arrow 格式的图像数据集转为 Lance
- **TextConverter**：将 `.txt` / `.json` / `.csv` 文本格式转为 Lance
- **ParquetConverter**：将 `.zip` / `.parquet` 压缩或表格文件转为 Lance

### ☁️ 存储支持

- 与 **S3 / MinIO** 无缝集成 (`s3_options.py`)
- Lance 数据集支持 **overwrite / append** 两种写入模式

### 🧑‍💻 训练 Demo

- **图像分类任务**：`demo_cv_resnet_train.py`
  使用 ResNet18 + CIFAR10 数据集进行快速训练。

### 🔗 框架集成

- **PyTorch**: `LanceTorchDataset`

### 📦 数据集与模型管理

- **DataManager**: 提供 **搜索 / 回滚 / 注册 / 删除** 等功能

### ⚙️ 引擎模式

- **Local**: 本地模式，直接调用 DataManager
- **Remote**: 提供 **HTTP / gRPC / WebSocket** 三种远程访问方式调用 DataManager

---

## 🚀 使用说明

### Demo

以下示例展示了如何运行 **ResNet18 + CIFAR10** 的训练任务，并将模型与数据集上传至 MinIO：

```bash
python Lance/src/blender/main/demo/demo_cv_resnet_train.py   --model https://download.pytorch.org/models/resnet18-f37072fd.pth   --model_mode url   --dataset cifar10   --dataset_mode arrow   --split train   --limit 500   --batch 32   --epochs 1   --steps 50   --s3-endpoint http://localhost:9000   --aws-access-key minioadmin   --aws-secret-key minioadmin123   --aws-region us-east-1   --bucket my-bucket   --prefix resnet_demo
```

### proto

下面为生成proto转pb2和pb2_grpc的使用说明

```bash
python -m grpc_tools.protoc -I . --pyi_out=. --python_out=. --grpc_python_out=. test.proto
```

---

## ⚙️ DEMO参数说明

| 参数               | 默认值                                                      | 说明                                                |
| ------------------ | ----------------------------------------------------------- | --------------------------------------------------- |
| `--cache`          | `./_cache`                                                  | 模型与数据集的本地缓存路径                          |
| `--model`          | `https://download.pytorch.org/models/resnet18-f37072fd.pth` | 模型路径（URL / HuggingFace Repo / Transformers）   |
| `--model_mode`     | `url`                                                       | 模型下载模式，可选：`url` / `repo` / `transformers` |
| `--dataset`        | `cifar10`                                                   | 数据集名称                                          |
| `--dataset_mode`   | `arrow`                                                     | 数据集模式，可选：`repo` / `arrow`                  |
| `--split`          | `train`                                                     | 数据集划分，如 `train` / `test`                     |
| `--limit`          | `1000`                                                      | 限制加载的数据条数                                  |
| `--batch`          | `32`                                                        | 批大小 (Batch Size)                                 |
| `--epochs`         | `1`                                                         | 训练轮数                                            |
| `--steps`          | `50`                                                        | 每轮训练的迭代步数                                  |
| `--s3-endpoint`    | `http://localhost:9000`                                     | S3/MinIO 服务地址                                   |
| `--aws-access-key` | `minioadmin`                                                | MinIO 访问密钥                                      |
| `--aws-secret-key` | `minioadmin123`                                             | MinIO 密钥                                          |
| `--aws-region`     | `us-east-1`                                                 | AWS 区域（默认 us-east-1）                          |
| `--bucket`         | `my-bucket`                                                 | S3/MinIO 的存储桶名称                               |
| `--prefix`         | `resnet_demo`                                               | 存储前缀（项目命名空间）                            |

---

## 📌 开发计划 (Roadmap)

- [ ] 支持更多深度学习框架（TF，JAX, MindSpore）
- [ ] 增强多模态数据集支持（音频，视频，点阵云）
- [ ] 增加下载、转化功能的容错和更多内容

---

## 📜 License

本项目遵循 **Apache 2.0 License**。
