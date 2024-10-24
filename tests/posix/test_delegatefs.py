import os
from alluxio.posix.delegateFs import DelegateFileSystem
import alluxio.posix.delegate


def test_delegatefs_open_write():
    # 定义文件路径
    file_path = f'oss://alhz-ossp-alluxio-test/alluxio-py/delegatefs-hello-test.txt'

    # 写入文件内容
    with os.open(file_path, 'w') as f:
        f.write('Hello, OSSP!')


def test_delegatefs_open_read():
    file_path = f'oss://alhz-ossp-alluxio-test/alluxio-py/delegatefs-hello-test.txt'
    with os.open(file_path, 'r') as f:
        content = f.read()
        print("File content:")
        print(content)


def test_delegatefs_close():
    file_path = 'oss://alhz-ossp-alluxio-test/alluxio-py/delegatefs-hello-test.txt'
    # Using low-level file descriptor
    fd = os.open(file_path, 'w')
    fd.write('Hello, test!')
    fd.close()


def test_delegatefs_stat():
    file_path = f'oss://alhz-ossp-alluxio-test/alluxio-py/delegatefs-hello-test.txt'
    print(os.stat(file_path))


def test_alluxio_open_write():
    file_path = 'alluxio://alhz-ossp-alluxio-test/alluxio-py/delegatefs-hello-test.txt'
    fd = os.open(file_path, os.O_WRONLY | os.O_CREAT)
    os.write(fd, b'Hello, OSSP!')
    os.close(fd)


def test_alluxio_open_read():
    file_path = 'alluxio://alhz-ossp-alluxio-test/alluxio-py/delegatefs-hello-test.txt'
    fd = os.open(file_path, os.O_RDONLY)
    content = os.read(fd, 100)  # 假设读取100字节足够
    print("File content:")
    print(content.decode())
    os.close(fd)


def test_alluxio_close():
    file_path = 'alluxio://alhz-ossp-alluxio-test/alluxio-py/delegatefs-hello-test.txt'
    fd = os.open(file_path, os.O_WRONLY | os.O_CREAT)
    os.write(fd, b'Hello, test!')
    os.close(fd)


def test_alluxio_stat():
    file_path = 'alluxio://alhz-ossp-alluxio-test/alluxio-py/delegatefs-hello-test.txt'
    print(os.stat(file_path))


if __name__ == "__main__":
    test_delegatefs_open_write()
    test_delegatefs_open_read()
