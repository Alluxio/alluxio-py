import os
import delegate


def delegatefs_open_write():
    write_file_path = f'oss://alhz-ossp-alluxio-test/alluxio-py/delegatefs-io-1.txt'
    with os.open(write_file_path, 'w') as f:
        f.write('Hello, OSSP! Hello alluxio-py!')


def delegatefs_open_read():
    read_file_path = f'oss://alhz-ossp-alluxio-test/alluxio-py/delegatefs-io-1.txt'
    with os.open(read_file_path, mode='r') as f:
        content = f.read()
        print("File content:")
        print(content)


def delegatefs_listdir():
    dir_path = f'oss://alhz-ossp-alluxio-test/alluxio-py/'
    print(os.listdir(dir_path))


def delegatefs_rename():
    origin_file_path = f'oss://alhz-ossp-alluxio-test/alluxio-py/delegatefs-rename-1.txt'
    renamed_file_path = f'oss://alhz-ossp-alluxio-test/alluxio-py/delegatefs-rename-2.txt'
    dir_path = f'oss://alhz-ossp-alluxio-test/alluxio-py/'
    with os.open(origin_file_path, mode='w') as f:
        f.write('Test rename...')
    os.rename(origin_file_path, renamed_file_path)
    os.listdir(dir_path)


def delegatefs_stat():
    stat_file_path = f'oss://alhz-ossp-alluxio-test/alluxio-py/delegatefs-io-1.txt'
    print(os.stat(stat_file_path))


def delegatefs_mkdir():
    dir_path = f'oss://alhz-ossp-alluxio-test/alluxio-py/'
    mkdir_path = f'oss://alhz-ossp-alluxio-test/alluxio-py/delegatefs-mkdir'
    os.mkdir(mkdir_path)
    print(os.listdir(mkdir_path))


def delegatefs_remove():
    dir_path = f'oss://alhz-ossp-alluxio-test/alluxio-py/'
    remove_path = f'oss://alhz-ossp-alluxio-test/alluxio-py/delegatefs-io-1.txt'
    print(os.listdir(dir_path))
    os.remove(remove_path)


def delegatefs_truncate():
    read_file_path = f'oss://alhz-ossp-alluxio-test/alluxio-py/delegatefs-io-1.txt'
    with os.open(read_file_path, mode='r') as f:
        f.truncate(10)
        content = f.read()
        print("File content:")
        print(content)


def delegatefs_rmdir():
    rmdir_path = f'oss://alhz-ossp-alluxio-test/alluxio-py/rmdirtest'
    os.rmdir(rmdir_path)


if __name__ == "__main__":
    delegatefs_open_write()
    delegatefs_open_read()
    delegatefs_listdir()
    delegatefs_rename()
    delegatefs_stat()
    delegatefs_remove()
    # 下面两个OssFileSystem中为空实现，但不会抛出异常
    delegatefs_mkdir()
    delegatefs_rmdir()
    delegatefs_truncate()