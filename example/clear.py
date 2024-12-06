from linecache import cache
from logging import exception

from alluxiofs.client import AlluxioClient, RMOption
from alluxiofs.config import ETCDConfig
from alluxiofs.exception import AlluxioException

alluxio_client = AlluxioClient(etcd_config=ETCDConfig(etcd_hosts="192.168.28.45"),page_size="4MB")

try:
    # bytes_data = alluxio_client.read(file_path="s3://yxd-fsspec/python_sdk_test_folder/file3")
    # print(bytes_data)
    # print("head and tail")
    # print(
    #     alluxio_client.head(file_path="s3://yxd-fsspec/python_sdk_test_folder/words", num_of_bytes=1))
    # print(
    #     alluxio_client.tail(file_path="s3://yxd-fsspec/python_sdk_test_folder/words", num_of_bytes=1))
    # res = alluxio_client.listdir("s3://yxd-fsspec/")
    # print(res)
    # # alluxio_client.remove("s3://yxd-fsspec/python_sdk_test_file",RMOption(recursive=True, remove_alluxio_only=False))
    alluxio_client.remove("s3://yxd-fsspec/python_sdk_test_folder2", RMOption(recursive=True, remove_alluxio_only=False))
    alluxio_client.remove("s3://yxd-fsspec/python_sdk_test_folder", RMOption(recursive=True, remove_alluxio_only=False))

except AlluxioException as e:
    raise e