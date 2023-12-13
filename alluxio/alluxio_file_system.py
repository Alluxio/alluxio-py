import hashlib
import io
import json
import logging
import os
import re
import time

import humanfriendly
import requests
from requests.adapters import HTTPAdapter
from enum import Enum

from .worker_ring import ConsistentHashProvider
from .worker_ring import EtcdClient
from .worker_ring import WorkerNetAddress

logging.basicConfig(
    level=logging.WARN,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

class FileStatus:
    def __init__(self, type, name, path, ufs_path, last_modification_time_ms, human_readable_file_size, length):
        self.type = type
        self.name = name
        self.path = path
        self.ufs_path = ufs_path
        self.last_modification_time_ms = last_modification_time_ms
        self.human_readable_file_size = human_readable_file_size
        self.length = length

class Progress(Enum):
    ONGOING = 1
    SUCCESS = 2
    FAIL = 3

class AlluxioFileSystem:
    """
    Access Alluxio file system

    Examples
    --------
    >>> # Launch Alluxio with ETCD as service discovery
    >>> alluxio = AlluxioFileSystem(etcd_host=localhost)
    >>> # Or launch Alluxio with user provided worker list
    >>> alluxio = AlluxioFileSystem(worker_hosts="host1,host2,host3")

    >>> print(alluxio.listdir("s3://mybucket/mypath/dir"))
    [
        {
            "mType": "file",
            "mName": "myfile",
            "mLength": 77542
        }

    ]
    >>> print(alluxio.read("s3://mybucket/mypath/dir/myfile"))
    my_file_content
    """

    ALLUXIO_PAGE_SIZE_KEY = "alluxio.worker.page.store.page.size"
    ALLUXIO_PAGE_SIZE_DEFAULT_VALUE = "1MB"
    LIST_URL_FORMAT = "http://{worker_host}:{http_port}/v1/files"
    PAGE_URL_FORMAT = (
        "http://{worker_host}:{http_port}/v1/file/{path_id}/page/{page_index}"
    )
    GET_FILE_STATUS_URL_FORMAT = (
        "http://{worker_host}:{http_port}/v1/info"
    )
    LOAD_SUBMIT_URL_FORMAT = (
        "http://{worker_host}:{http_port}/v1/load?path={path}&opType=submit"
    )
    LOAD_PROGRESS_URL_FORMAT = (
        "http://{worker_host}:{http_port}/v1/load?path={path}&opType=progress"
    )
    LOAD_STOP_URL_FORMAT = (
        "http://{worker_host}:{http_port}/v1/load?path={path}&opType=stop"
    )

    def __init__(
        self,
        etcd_host=None,
        worker_hosts=None,
        options=None,
        logger=None,
        concurrency=64,
        http_port="28080",
    ):
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
            http_port (string, optional):
                The port of the HTTP server on each Alluxio worker node.
        """
        if etcd_host is None and worker_hosts is None:
            raise ValueError(
                "Must supply either 'etcd_host' or 'worker_hosts'"
            )
        if etcd_host and worker_hosts:
            raise ValueError(
                "Supply either 'etcd_host' or 'worker_hosts', not both"
            )
        self.logger = logger or logging.getLogger("AlluxioFileSystem")
        self.session = self._create_session(concurrency)

        # parse options
        page_size = self.ALLUXIO_PAGE_SIZE_DEFAULT_VALUE
        if options:
            if self.ALLUXIO_PAGE_SIZE_KEY in options:
                page_size = options[self.ALLUXIO_PAGE_SIZE_KEY]
                self.logger.debug(f"Page size is set to {page_size}")
        self.page_size = humanfriendly.parse_size(page_size, binary=True)

        # parse worker info to form hash ring
        worker_addresses = None
        if etcd_host:
            worker_addresses = EtcdClient(
                host=etcd_host, options=options
            ).get_worker_addresses()
        else:
            worker_addresses = WorkerNetAddress.from_worker_hosts(worker_hosts)
        self.hash_provider = ConsistentHashProvider(
            worker_addresses, self.logger
        )
        self.http_port = http_port

    def listdir(self, path):
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
                    type: "file",
                    name: "my_file_name",
                    path: '/my_file_name',
                    ufs_path: 's3://example-bucket/my_file_name',
                    last_modification_time_ms: 0,
                    length: 77542,
                    human_readable_file_size: '75.72KB'
                },
                {
                    type: "directory",
                    name: "my_dir_name",
                    path: '/my_dir_name',
                    ufs_path: 's3://example-bucket/my_dir_name',
                    last_modification_time_ms: 0,
                    length: 0,
                    human_readable_file_size: '0B'
                },

            ]
        """
        self._validate_path(path)
        worker_host = self._get_preferred_worker_host(path)
        params = {"path": path}
        try:
            response = self.session.get(
                self.LIST_URL_FORMAT.format(
                    worker_host=worker_host, http_port=self.http_port
                ),
                params=params,
            )
            response.raise_for_status()
            result = []
            for data in json.loads(response.content):
                result.append(FileStatus(
                    data['mType'],
                    data['mName'],
                    data['mPath'],
                    data['mUfsPath'],
                    data['mLastModificationTimeMs'],
                    data['mHumanReadableFileSize'],
                    data['mLength']
                ))
            return result
        except Exception as e:
            raise Exception(
                f"Error when listing path {path}: error {e}"
            ) from e

    def get_file_status(self, path):
        """
        Gets the file status of the path.

        Args:
            path (str): The full ufs path to get the file status of

        Returns:
            File Status: The struct has:
                - type (string): directory or file
                - name (string): name of the directory/file
                - path (string): the path of the file
                - ufs_path (string): the ufs path of the file
                - last_modification_time_ms (long): the last modification time
                - length (integer): length of the file or 0 for directory
                - human_readable_file_size (string): the size of the human readable files

        Example:
            {
                type: 'directory',
                name: 'a',
                path: '/a',
                ufs_path: 's3://example-bucket/a',
                last_modification_time_ms: 0,
                length: 0,
                human_readable_file_size: '0B'
            }
        """
        self._validate_path(path)
        worker_host = self._get_preferred_worker_host(path)
        params = {"path": path}
        try:
            response = self.session.get(
                self.GET_FILE_STATUS_URL_FORMAT.format(
                    worker_host=worker_host,
                    http_port=self.http_port,
                ),
                params=params,
            )
            response.raise_for_status()
            data = json.loads(response.content)[0]
            return FileStatus(
                data['mType'],
                data['mName'],
                data['mPath'],
                data['mUfsPath'],
                data['mLastModificationTimeMs'],
                data['mHumanReadableFileSize'],
                data['mLength']
            )
        except Exception as e:
            raise Exception(
                f"Error when getting file status path {path}: error {e}"
            ) from e

    def load(
        self,
        file_path,
        timeout=None,
    ):
        """
        Loads a file.

        Args:
            file_path (str): The full ufs file path to load data from
            timout (integer): The number of seconds for timeout

        Returns:
            result (boolean): Whether the file has been loaded successfully
        """
        worker_host = self._get_preferred_worker_host(file_path)
        return self._load_file(worker_host, file_path, timeout)

    def submit_load(
        self,
        file_path,
    ):
        """
        Submits a load job for a file.

        Args:
            file_path (str): The full ufs file path to load data from

        Returns:
            result (boolean): Whether the job has been submitted successfully
        """
        worker_host = self._get_preferred_worker_host(file_path)
        try:
            response = self.session.get(
                self.LOAD_SUBMIT_URL_FORMAT.format(
                    worker_host=worker_host,
                    http_port=self.http_port,
                    path=file_path,
                ),
            )
            response.raise_for_status()
            return b"successfully" in response.content
        except Exception as e:
            raise Exception(
                f"Error when submitting load job for path {file_path} from {worker_host}: error {e}"
            ) from e

    def stop_load(
        self,
        file_path,
    ):
        """
        Stops a load job for a file.

        Args:
            file_path (str): The full ufs file path to load data from

        Returns:
            result (boolean): Whether the job has been stopped successfully
        """
        worker_host = self._get_preferred_worker_host(file_path)
        try:
            response = self.session.get(
                self.LOAD_STOP_URL_FORMAT.format(
                    worker_host=worker_host,
                    http_port=self.http_port,
                    path=file_path,
                ),
            )
            response.raise_for_status()
            return b"successfully" in response.content
        except Exception as e:
            raise Exception(
                f"Error when stopping load job for path {file_path} from {worker_host}: error {e}"
            ) from e

    def load_progress(
        self,
        file_path,
    ):
        """
        Gets the progress of the load job for a file.

        Args:
            file_path (str): The full ufs file path to load data from

        Returns:
            progress (Progress): The progress of the load job
        """
        worker_host = self._get_preferred_worker_host(file_path)
        try:
            response = self.session.get(
                self.LOAD_PROGRESS_URL_FORMAT.format(
                    worker_host=worker_host,
                    http_port=self.http_port,
                    path=file_path,
                ),
            )
            response.raise_for_status()
            progress_str = response.content.decode("utf-8")
            if 'RUNNING' in progress_str:
                return Progress.ONGOING
            if 'SUCCEEDED' in progress_str:
                return Progress.SUCCESS
            return Progress.FAIL
        except Exception as e:
            raise Exception(
                f"Error when getting load job progress for path {file_path} from {worker_host}: error {e}"
            ) from e

    def read(self, file_path):
        """
        Reads a file.

        Args:
            file_path (str): The full ufs file path to read data from

        Returns:
            file content (str): The full file content
        """
        self._validate_path(file_path)
        worker_host = self._get_preferred_worker_host(file_path)
        path_id = self._get_path_hash(file_path)
        try:
            return b"".join(self._all_page_generator(worker_host, path_id))
        except Exception as e:
            raise Exception(
                f"Error when reading file {file_path}: error {e}"
            ) from e

    def read_range(self, file_path, offset, length):
        """
        Reads parts of a file.

        Args:
            file_path (str): The full ufs file path to read data from
            offset (integer): The offset to start reading data from
            length (integer): The file length to read

        Returns:
            file content (str): The file content with length from offset
        """
        self._validate_path(file_path)
        if not isinstance(offset, int) or offset < 0:
            raise ValueError("Offset must be a non-negative integer")

        if not isinstance(length, int) or (length <= 0 and length != -1):
            raise ValueError("Length must be a positive integer or -1")

        worker_host = self._get_preferred_worker_host(file_path)
        path_id = self._get_path_hash(file_path)
        try:
            return b"".join(
                self._range_page_generator(
                    worker_host, path_id, offset, length
                )
            )
        except Exception as e:
            raise Exception(
                f"Error when reading file {file_path}: error {e}"
            ) from e

    def _all_page_generator(self, worker_host, path_id):
        page_index = 0
        while True:
            try:
                page_content = self._read_page(
                    worker_host, path_id, page_index
                )
            except Exception as e:
                if page_index == 0:
                    raise Exception(
                        f"Error when reading page 0 of {path_id}: error {e}"
                    ) from e
                else:
                    # TODO(lu) distinguish end of file exception and real exception
                    break
            if not page_content:
                break
            yield page_content
            if len(page_content) < self.page_size:  # last page
                break
            page_index += 1

    def _range_page_generator(self, worker_host, path_id, offset, length):
        start_page_index = offset // self.page_size
        start_page_offset = offset % self.page_size

        if length == -1:
            end_page_index = None
        else:
            end_page_index = (offset + length - 1) // self.page_size
            end_page_read_to = ((offset + length - 1) % self.page_size) + 1

        page_index = start_page_index
        while True:
            try:
                page_content = self._read_page(
                    worker_host, path_id, page_index
                )
            except Exception as e:
                if page_index == start_page_index:
                    raise Exception(
                        f"Error when reading page {page_index} of {path_id}: error {e}"
                    ) from e
                else:
                    # TODO(lu) distinguish end of file exception and real exception
                    break
            if page_index == start_page_index:
                if start_page_index == end_page_index:
                    yield page_content[start_page_offset:end_page_read_to]
                    break
                else:
                    page_content = page_content[start_page_offset:]
            elif page_index == end_page_index:
                yield page_content[:end_page_read_to]
                break
            elif len(page_content) < self.page_size:
                yield page_content
                break

            yield page_content
            page_index += 1

    def _create_session(self, concurrency):
        session = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=concurrency, pool_maxsize=concurrency
        )
        session.mount("http://", adapter)
        return session

    def _load_file(self, worker_host, path, timeout):
        try:
            response = self.session.get(
                self.LOAD_SUBMIT_URL_FORMAT.format(
                    worker_host=worker_host,
                    http_port=self.http_port,
                    path=path,
                ),
            )
            response.raise_for_status()
            currentTime = time.time()
            stopTime = 0
            if timeout is not None:
                stopTime = currentTime + timeout
            while timeout is None or time.time() <= stopTime:
                response = self.session.get(
                    self.LOAD_PROGRESS_URL_FORMAT.format(
                        worker_host=worker_host,
                        http_port=self.http_port,
                        path=path,
                    ),
                )
                response.raise_for_status()
                content = response.content
                if b"SUCCEEDED" in content:
                    logging.info(content.decode("utf-8"))
                    return True
                if b"FAILED" in content:
                    raise Exception(f"{content}")
                if timeout is None or stopTime - time.time() >= 10:
                    time.sleep(10)
                else:
                    if timeout is not None:
                        time.sleep(stopTime - time.time())
            raise Exception(f"Load timeout!")
        except Exception as e:
            raise Exception(
                f"Error when loading file {path} from {worker_host}: error {e}"
            ) from e

    def _read_page(self, worker_host, path_id, page_index):
        try:
            response = self.session.get(
                self.PAGE_URL_FORMAT.format(
                    worker_host=worker_host,
                    http_port=self.http_port,
                    path_id=path_id,
                    page_index=page_index,
                ),
            )
            response.raise_for_status()
            return response.content
        except Exception as e:
            raise Exception(
                f"Error when requesting file {path_id} page {page_index} from {worker_host}: error {e}"
            ) from e

    def _get_path_hash(self, uri):
        hash_functions = [
            hashlib.sha256,
            hashlib.md5,
            lambda x: hex(hash(x))[2:].lower(),  # Fallback to simple hashCode
        ]
        for hash_function in hash_functions:
            try:
                hash_obj = hash_function()
                hash_obj.update(uri.encode("utf-8"))
                return hash_obj.hexdigest().lower()
            except AttributeError:
                continue

    def _get_preferred_worker_host(self, full_ufs_path):
        workers = self.hash_provider.get_multiple_workers(full_ufs_path, 1)
        if len(workers) != 1:
            raise ValueError(
                "Expected exactly one worker from hash ring, but found {} workers {}.".format(
                    len(workers), workers
                )
            )
        return workers[0].host

    def _validate_path(self, path):
        if not isinstance(path, str):
            raise TypeError("path must be a string")

        if not re.search(r"^[a-zA-Z0-9]+://", path):
            raise ValueError(
                "path must be a full path with a protocol (e.g., 'protocol://path')"
            )
