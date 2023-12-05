import hashlib
import io
import json
import logging
import os
import re

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
        self._validate_path(path)
        path_id = self._get_path_hash(path)
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
            return json.loads(response.content)
        except Exception as e:
            raise Exception(
                f"Error when listing path {path}: error {e}"
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
                        f"Error when reading page 0 of {file_path}: error {e}"
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
                        f"Error when reading page {page_index} of {file_path}: error {e}"
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
