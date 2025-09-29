import os
import uuid

import pytest
import s3fs

ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin123")
REGION = os.getenv("S3_REGION", "us-east-1")
BUCKET = os.getenv("S3_BUCKET", "my-bucket")
TEST_PREFIX = os.getenv("S3_TEST_PREFIX", "pytest")


@pytest.fixture(scope="session")
def s3fs_client():
    fs = s3fs.S3FileSystem(
        key=ACCESS_KEY,
        secret=SECRET_KEY,
        client_kwargs={"endpoint_url": ENDPOINT, "region_name": REGION},
    )
    # 创建 bucket（若不存在）
    if not fs.exists(BUCKET):
        fs.mkdir(BUCKET)
    # 创建测试前缀“目录”（S3 虚拟目录，确保父路径存在）
    fs.makedirs(f"{BUCKET}/{TEST_PREFIX}", exist_ok=True)
    yield fs
    # 测试结束清理测试前缀下的对象
    try:
        if fs.exists(f"{BUCKET}/{TEST_PREFIX}"):
            for p in fs.find(f"{BUCKET}/{TEST_PREFIX}"):
                fs.rm(p)
            # 删空目录占位（可选，不影响下次测试）
            try:
                fs.rmdir(f"{BUCKET}/{TEST_PREFIX}")
            except Exception:
                pass
    except Exception:
        pass


def test_bucket_exists(s3fs_client):
    assert s3fs_client.exists(BUCKET), f"Bucket {BUCKET} should exist"


def test_write_and_read(s3fs_client):
    key = f"{TEST_PREFIX}/{uuid.uuid4().hex}.txt"
    path = f"{BUCKET}/{key}"
    content = "hello from pytest"

    with s3fs_client.open(path, "w") as f:
        f.write(content)

    assert s3fs_client.exists(path), f"{path} not found after write"

    with s3fs_client.open(path, "r") as f:
        read_back = f.read()

    assert read_back == content
