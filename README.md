# Alluxio Python Library

## Installation

Install from source
```
cd alluxio-python-library
python setup.py sdist bdist_wheel
pip install dist/alluxio_python_library-0.1-py3-none-any.whl
```

## Usage

Import Alluxio
```
from alluxio import AlluxioFileSystem
```

Init Alluxio filesystem
```
"""
Inits Alluxio file system.

Args:
    etcd_host (str, optional):
        The hostname of ETCD to get worker addresses from
        Either etcd_host or worker_hosts should be provided, not both.
    worker_hosts (str, optional):
        The worker hostnames in host1,host2,host3 format. Either etcd_host or worker_hosts should be provided, not both.
    options (dict, optional):
        A dictionary of Alluxio property key and values.
        Note that Alluxio Python API only support a limited set of Alluxio properties.
    logger (Logger, optional):
        A logger instance for logging messages.
    concurrency (int, optional):
        The maximum number of concurrent operations. Default to 64.
"""

# Minimum setup for Alluxio with ETCD membership service
alluxio = AlluxioFileSystem(etcd_host="localhost")

# Minimum setup for Alluxio with user-defined worker list
alluxio = AlluxioFileSystem(worker_hosts="worker_host1,worker_host2")

# Minimum setup for Alluxio with self-defined page size
alluxio = AlluxioFileSystem(
            etcd_host="localhost",
            options={"alluxio.worker.page.store.page.size": "20MB"}
            )
# Minimum setup for Alluxio with ETCD membership service with username/password
options = {
    "alluxio.etcd.username": "my_user",
    "alluxio.etcd.password": "my_password",
    "alluxio.worker.page.store.page.size": "20MB"  # Any other options should be included here
}
alluxio = AlluxioFileSystem(
    etcd_host="localhost",
    options=options
)
```

List directory

```
"""
Lists the directory.

Args:
    path (str): The full ufs path to list from

Returns:
    list of dict: A list containing dictionaries, where each dictionary has:
        - mType (string): directory or file
        - mName (string): name of the directory/file
        - mLength (integer): length of the file or 0 for directory

Example:
    [
        {
            "mType": "file",
            "mName": "my_file_name",
            "mLength": 77542
        },
        {
            "mType": "directory",
            "mName": "my_dir_name",
            "mLength": 0
        },

    ]
"""
print(alluxio.listdir(full_ufs_dataset_path))
```


Read a file
```
"""
Reads a file.

Args:
    file_path (str): The full ufs file path to read data from

Returns:
    file content (str): The full file content
"""
print(alluxio.read(full_ufs_file_path))
```
See datasets/alluxio.py AlluxioDataset for more example usage

## Development

See [Contributions](CONTRIBUTING.md) for guidelines around making new contributions and reviewing them.
