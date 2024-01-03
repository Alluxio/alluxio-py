import hashlib
import io
import json
import logging
import os
import re
import time
from enum import Enum

import humanfriendly
import requests
from requests.adapters import HTTPAdapter

from .worker_ring import ConsistentHashProvider
from .worker_ring import EtcdClient
from .worker_ring import WorkerNetAddress

logging.basicConfig(
    level=logging.WARN,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


class AlluxioPathStatus:
    def __init__(
        self,
        type,
        name,
        path,
        ufs_path,
        last_modification_time_ms,
        human_readable_file_size,
        length,
    ):
        self.type = type
        self.name = name
        self.path = path
        self.ufs_path = ufs_path
        self.last_modification_time_ms = last_modification_time_ms
        self.human_readable_file_size = human_readable_file_size
        self.length = length


class LoadState(Enum):
    RUNNING = 1
    VERIFYING = 2
    STOPPED = 3
    SUCCEEDED = 4
    FAILED = 5

    @staticmethod
    def from_string(status):
        try:
            return LoadState[status]
        except KeyError:
            raise ValueError(f"'{status}' is not a valid LoadState")


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
    ALLUXIO_SUCCESS_IDENTIFIER = "success"
    LIST_URL_FORMAT = "http://{worker_host}:{http_port}/v1/files"
    FULL_PAGE_URL_FORMAT = (
        "http://{worker_host}:{http_port}/v1/file/{path_id}/page/{page_index}"
    )
    PAGE_URL_FORMAT = "http://{worker_host}:{http_port}/v1/file/{path_id}/page/{page_index}?offset={page_offset}&length={page_length}"
    WRITE_PAGE_URL_FORMAT = (
        "http://{worker_host}:{http_port}/v1/file/{path_id}/page/{page_index}"
    )
    GET_FILE_STATUS_URL_FORMAT = "http://{worker_host}:{http_port}/v1/info"
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
        etcd_hosts=None,
        worker_hosts=None,
        options=None,
        logger=None,
        concurrency=64,
        http_port="28080",
    ):
        """
        Inits Alluxio file system.

        Args:
            etcd_hosts (str, optional):
                The hostnames of ETCD to get worker addresses from
                The hostnames in host1,host2,host3 format. Either etcd_hosts or worker_hosts should be provided, not both.
            worker_hosts (str, optional):
                The worker hostnames in host1,host2,host3 format. Either etcd_hosts or worker_hosts should be provided, not both.
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
        if etcd_hosts is None and worker_hosts is None:
            raise ValueError(
                "Must supply either 'etcd_hosts' or 'worker_hosts'"
            )
        if etcd_hosts and worker_hosts:
            raise ValueError(
                "Supply either 'etcd_hosts' or 'worker_hosts', not both"
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
        self.hash_provider = ConsistentHashProvider(
            etcd_hosts, worker_hosts, self.logger
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
                result.append(
                    AlluxioPathStatus(
                        data["mType"],
                        data["mName"],
                        data["mPath"],
                        data["mUfsPath"],
                        data["mLastModificationTimeMs"],
                        data["mHumanReadableFileSize"],
                        data["mLength"],
                    )
                )
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
            return AlluxioPathStatus(
                data["mType"],
                data["mName"],
                data["mPath"],
                data["mUfsPath"],
                data["mLastModificationTimeMs"],
                data["mHumanReadableFileSize"],
                data["mLength"],
            )
        except Exception as e:
            raise Exception(
                f"Error when getting file status path {path}: error {e}"
            ) from e

    def load(
        self,
        path,
        timeout=None,
    ):
        """
        Loads a file.

        Args:
            path (str): The full path with storage protocol to load data from
            timeout (integer): The number of seconds for timeout, optional

        Returns:
            result (boolean): Whether the file has been loaded successfully
        """
        self._validate_path(path)
        worker_host = self._get_preferred_worker_host(path)
        return self._load_file(worker_host, path, timeout)

    def submit_load(
        self,
        path,
    ):
        """
        Submits a load job for a file.

        Args:
            path (str): The full ufs file path to load data from

        Returns:
            result (boolean): Whether the job has been submitted successfully
        """
        self._validate_path(path)
        worker_host = self._get_preferred_worker_host(path)
        try:
            response = self.session.get(
                self.LOAD_SUBMIT_URL_FORMAT.format(
                    worker_host=worker_host,
                    http_port=self.http_port,
                    path=path,
                ),
            )
            response.raise_for_status()
            content = json.loads(response.content.decode("utf-8"))
            return content[self.ALLUXIO_SUCCESS_IDENTIFIER]
        except Exception as e:
            raise Exception(
                f"Error when submitting load job for path {path} from {worker_host}: error {e}"
            ) from e

    def stop_load(
        self,
        path,
    ):
        """
        Stops a load job for a file.

        Args:
            path (str): The full ufs file path to load data from

        Returns:
            result (boolean): Whether the job has been stopped successfully
        """
        self._validate_path(path)
        worker_host = self._get_preferred_worker_host(path)
        try:
            response = self.session.get(
                self.LOAD_STOP_URL_FORMAT.format(
                    worker_host=worker_host,
                    http_port=self.http_port,
                    path=path,
                ),
            )
            response.raise_for_status()
            content = json.loads(response.content.decode("utf-8"))
            return content[self.ALLUXIO_SUCCESS_IDENTIFIER]
        except Exception as e:
            raise Exception(
                f"Error when stopping load job for path {path} from {worker_host}: error {e}"
            ) from e

    def load_progress(
        self,
        path,
    ):
        """
        Gets the progress of the load job for a file.

        Args:
            path (str): The full ufs file path to load data from

        Returns:
            progress (Progress): The progress of the load job
        """
        self._validate_path(path)
        load_progress_url = self.LOAD_PROGRESS_URL_FORMAT.format(
            worker_host=self._get_preferred_worker_host(path),
            http_port=self.http_port,
            path=path,
        )
        return self._load_progress_internal(load_progress_url)

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

    def write_page(self, file_path, page_index, page_bytes):
        """
        Writes a page.

        Args:
            file_path: The path of the file where data is to be written.
            page_index: The page index in the file to write the data.
            page_bytes: The byte data to write to the specified page, MUST BE FULL PAGE.

        Returns:
            True if the write was successful, False otherwise.
        """
        self._validate_path(file_path)
        worker_host = self._get_preferred_worker_host(file_path)
        path_id = self._get_path_hash(file_path)
        try:
            response = requests.post(
                self.WRITE_PAGE_URL_FORMAT.format(
                    worker_host=worker_host,
                    http_port=self.http_port,
                    path_id=path_id,
                    page_index=page_index,
                ),
                headers={"Content-Type": "application/octet-stream"},
                data=page_bytes,
            )
            response.raise_for_status()
            return 200 <= response.status_code < 300
        except requests.RequestException as e:
            raise Exception(
                f"Error writing to file {file_path} at page {page_index}: {e}"
            )

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

        # Determine the end page index and the read-to position
        if length == -1:
            end_page_index = None
        else:
            end_page_index = (offset + length - 1) // self.page_size
            end_page_read_to = ((offset + length - 1) % self.page_size) + 1

        page_index = start_page_index
        while True:
            try:
                if page_index == start_page_index:
                    if start_page_index == end_page_index:
                        read_length = end_page_read_to - start_page_offset
                    else:
                        read_length = self.page_size - start_page_offset
                    page_content = self._read_page(
                        worker_host,
                        path_id,
                        page_index,
                        start_page_offset,
                        read_length,
                    )
                elif page_index == end_page_index:
                    page_content = self._read_page(
                        worker_host, path_id, page_index, 0, end_page_read_to
                    )
                else:
                    page_content = self._read_page(
                        worker_host, path_id, page_index
                    )

                yield page_content

                # Check if it's the last page or the end of the file
                if (
                    page_index == end_page_index
                    or len(page_content) < self.page_size
                ):
                    break

                page_index += 1

            except Exception as e:
                if page_index == start_page_index:
                    raise Exception(
                        f"Error when reading page {page_index} of {path_id}: error {e}"
                    ) from e
                else:
                    # read some data successfully, return those data
                    break

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
            content = json.loads(response.content.decode("utf-8"))
            if not content[self.ALLUXIO_SUCCESS_IDENTIFIER]:
                return False

            load_progress_url = self.LOAD_PROGRESS_URL_FORMAT.format(
                worker_host=worker_host,
                http_port=self.http_port,
                path=path,
            )
            stop_time = 0
            if timeout is not None:
                stop_time = time.time() + timeout
            while True:
                job_state = self._load_progress_internal(load_progress_url)
                if job_state == LoadState.SUCCEEDED:
                    return True
                if job_state == LoadState.FAILED:
                    self.logger.debug(
                        f"Failed to load path {path} with return message {content}"
                    )
                    return False
                if job_state == LoadState.STOPPED:
                    self.logger.debug(
                        f"Failed to load path {path} with return message {content}, load stopped"
                    )
                    return False
                if timeout is None or stop_time - time.time() >= 10:
                    time.sleep(10)
                else:
                    self.logger.debug(
                        f"Failed to load path {path} within timeout"
                    )
                    return False

        except Exception as e:
            self.logger.debug(
                f"Error when loading file {path} from {worker_host} with timeout {timeout}: error {e}"
            )
            return False

    def _load_progress_internal(self, load_url):
        try:
            response = self.session.get(load_url)
            response.raise_for_status()
            content = json.loads(response.content.decode("utf-8"))
            if "jobState" not in content:
                raise KeyError(
                    "The field 'jobState' is missing from the load progress response content"
                )
            return LoadState.from_string(content["jobState"])
        except Exception as e:
            raise Exception(
                f"Error when getting load job progress for {load_url}: error {e}"
            ) from e

    def _read_page(
        self, worker_host, path_id, page_index, offset=None, length=None
    ):
        if (offset is None) != (length is None):
            raise ValueError(
                "Both offset and length should be either None or both not None"
            )

        try:
            if offset is None:
                page_url = self.FULL_PAGE_URL_FORMAT.format(
                    worker_host=worker_host,
                    http_port=self.http_port,
                    path_id=path_id,
                    page_index=page_index,
                )
            else:
                page_url = self.PAGE_URL_FORMAT.format(
                    worker_host=worker_host,
                    http_port=self.http_port,
                    path_id=path_id,
                    page_index=page_index,
                    page_offset=offset,
                    page_length=length,
                )

            response = self.session.get(page_url)
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
