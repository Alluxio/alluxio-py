# Alluxio Python Library

This repo contains the Alluxio Python API to interact with Alluxio servers, bridging the gap between computation frameworks and undelying storage systems. This module provides a convenient interface for performing file system operations such as reading, writing, and listing files in an Alluxio cluster.

## Features

- Directory listing and file status fetching
- Put data to Alluxio system cache and read from Alluxio system cache (include range read)
- Alluxio system Load operations with progress tracking
- Support dynamic Alluxio worker membership services (ETCD periodically refreshing and manually specified worker hosts)

## Limitations

Alluxio Python library supports reading from Alluxio cached data.
The data needs to either
- Loaded into Alluxio servers via `load` operations
- Put into Alluxio servers via `write_page` operation.

If you need to read from storage systems directly with Alluxio on demand caching capabilities,
please use [alluxiofs](https://github.com/fsspec/alluxiofs) instead.

## Installation

Install from source
```
cd alluxio-python-library
python setup.py sdist bdist_wheel
pip install dist/alluxio_python_library-0.1-py3-none-any.whl
```

## Usage

### Initialization
Import and initialize the `AlluxioFileSystem` class:
```
# Minimum setup for Alluxio with ETCD membership service
alluxio = AlluxioFileSystem(etcd_hosts="localhost")

# Minimum setup for Alluxio with user-defined worker list
alluxio = AlluxioFileSystem(worker_hosts="worker_host1,worker_host2")

# Minimum setup for Alluxio with self-defined page size
alluxio = AlluxioFileSystem(
            etcd_hosts="localhost",
            options={"alluxio.worker.page.store.page.size": "20MB"}
            )
# Minimum setup for Alluxio with ETCD membership service with username/password
options = {
    "alluxio.etcd.username": "my_user",
    "alluxio.etcd.password": "my_password",
    "alluxio.worker.page.store.page.size": "20MB"  # Any other options should be included here
}
alluxio = AlluxioFileSystem(
    etcd_hosts="localhost",
    options=options
)
```

### Load Operations
Dataset metadata and data in the Alluxio under storage need to be loaded into Alluxio system cache
to read by end-users. Run the load operations before executing the read commands.
```
# Start a load operation
load_success = alluxio_fs.load('s3://mybucket/mypath/file')
print('Load successful:', load_success)

# Check load progress
progress = alluxio_fs.load_progress('s3://mybucket/mypath/file')
print('Load progress:', progress)

# Stop a load operation
stop_success = alluxio_fs.stop_load('s3://mybucket/mypath/file')
print('Stop successful:', stop_success)
```

### (Advanced) Page Write
Alluxio system cache can be used as a key value cache system.
Data can be written to Alluxio system cache via `write_page` command
after which the data can be read from Alluxio system cache (Alternative to load operations).

```
success = alluxio_fs.write_page('s3://mybucket/mypath/file', page_index, page_bytes)
print('Write successful:', success)
```

### Directory Listing
List the contents of a directory:
```
"""
contents = alluxio_fs.listdir('s3://mybucket/mypath/dir')
print(contents)
```

### Get File Status
Retrieve the status of a file or directory:
```
status = alluxio_fs.get_file_status('s3://mybucket/mypath/file')
print(status)
```

### File Reading
Read the entire content of a file:
```
"""
Reads a file.

Args:
    file_path (str): The full ufs file path to read data from

Returns:
    file content (str): The full file content
"""
content = alluxio_fs.read('s3://mybucket/mypath/file')
print(content)
```
Read a specific range of a file:
```
content = alluxio_fs.read_range('s3://mybucket/mypath/file', offset, length)
print(content)
```

## Development

See [Contributions](CONTRIBUTING.md) for guidelines around making new contributions and reviewing them.
