# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Tuple, Generator

import requests
from requests import Session
from requests.adapters import HTTPAdapter

from alluxio.config import AlluxioClientConfig
from alluxio.const import LIST_URL_FORMAT, GET_FILE_STATUS_URL_FORMAT, \
    WRITE_PAGE_URL_FORMAT, FULL_PAGE_URL_FORMAT, PAGE_URL_FORMAT, LOAD_URL_FORMAT, ALLUXIO_SUCCESS_IDENTIFIER, \
    MKDIR_URL_FORMAT, TOUCH_URL_FORMAT, MV_URL_FORMAT, RM_URL_FORMAT, CP_URL_FORMAT, TAIL_URL_FORMAT, HEAD_URL_FORMAT
from alluxio.exception import PageReadException, PageWriteException, GetFileStatusException, LoadException, \
    FileOperationException
from alluxio.load import load_file, load_progress_internal, OpType
from alluxio.logger import set_log_level
from alluxio.segment import SegmentRanges
from alluxio.worker_ring import ConsistentHashProvider, logger


@dataclass
class AlluxioPathStatus:
    type: str
    name: str
    path: str
    ufs_path: str
    last_modification_time_ms: int
    human_readable_file_size: str
    length: int


@dataclass(frozen=True)
class RangeParameters:
    start_page_index: int
    start_page_offset: int
    end_page_index: int
    end_page_read_to: int


@dataclass
class RMOption:
    # delete files and subdirectories recursively
    recursive: bool = False
    # Marks a directory to either trigger a metadata sync or skip the metadata sync on next access.
    sync_parent_next_time: bool = False
    # remove directories without checking UFS contents are in sync
    remove_unchecked_option: bool = False
    # remove data and metadata from Alluxio space only
    remove_alluxio_only: bool = True
    # remove mount points in the directory
    delete_mount_point: bool = False


@dataclass(frozen=True)
class CPOption:
    # delete files and subdirectories recursively
    recursive: bool = True
    # forces to overwrite the destination file if it exists
    forced: bool = False
    # Number of threads used to copy files in parallel, default value is CPU cores * 2
    thread: int = None
    # Read buffer size in bytes, default is 8MB when copying from local, and 64MB when copying to local
    buffer_size: str = None
    # Preserve file permission attributes when copying files. All ownership, permissions and ACLs will be preserved
    preserve: bool = True


class AlluxioClient:
    def __init__(
            self,
            **kwargs,
    ):
        self.config = AlluxioClientConfig(**kwargs)
        self.session = create_session(self.config.concurrency)
        self.hash_provider = ConsistentHashProvider(self.config)

        test_options = kwargs.get("test_options", {})
        set_log_level(logger, test_options)

    def listdir(self, path: str) -> list[AlluxioPathStatus]:
        validate_path(path)
        worker_host, worker_http_port = self._get_preferred_worker_address(
            path
        )
        params = {"path": path}
        try:
            response = self.session.get(
                LIST_URL_FORMAT.format(
                    worker_host=worker_host, http_port=worker_http_port
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
            raise GetFileStatusException(
                f"Error happens when listing path {path}"
            ) from e

    def get_file_status(self, path: str) -> AlluxioPathStatus:
        validate_path(path)
        worker_host, worker_http_port = self._get_preferred_worker_address(
            path
        )
        params = {"path": path}
        try:
            response = self.session.get(
                GET_FILE_STATUS_URL_FORMAT.format(
                    worker_host=worker_host,
                    http_port=worker_http_port,
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
            raise GetFileStatusException(
                f"Error happens when getting file status of {path}"
            ) from e

    def read(self, file_path: str, use_segment: bool = False, segment_size: int = 2 * 1024) -> bytes:
        validate_path(file_path)
        # todo not a completed function
        if use_segment:
            file_status = self.get_file_status(file_path)
            if file_status is None:
                raise FileNotFoundError(f"File {file_path} not found")
            self._page_generator_on_segmented_file(file_path, 0, file_status.length, segment_size)

        worker_host, worker_http_port = self._get_preferred_worker_address(
            file_path
        )
        path_id = get_path_hash(file_path)

        return b"".join(
            self._page_generator(
                worker_host, worker_http_port, path_id, file_path
            )
        )

    def read_range(self, file_path: str, offset: int, length: int) -> bytes:
        validate_path(file_path)
        if not isinstance(offset, int) or offset < 0:
            raise ValueError("Offset must be a non-negative integer")

        if length is None or length == -1:
            file_status = self.get_file_status(file_path)
            if file_status is None:
                raise FileNotFoundError(f"File {file_path} not found")
            length = file_status.length - offset

        if length == 0:
            return b""

        if not isinstance(length, int) or length < 0:
            raise ValueError(
                f"Invalid length: {length}. Length must be a non-negative integer, -1, or None. Requested offset: {offset}"
            )

        worker_host, worker_http_port = self._get_preferred_worker_address(
            file_path
        )
        path_id = get_path_hash(file_path)

        return b"".join(
            self._page_generator(
                worker_host,
                worker_http_port,
                path_id,
                file_path,
                offset,
                length,
            )
        )

    def _write_all_pages_in_file(self, worker_host: str, worker_http_port: int, path_id: str, file_path: str,
                                 file_bytes: bytes):
        page_index = 0
        page_size = self.config.page_size
        offset = 0
        try:
            while True:
                end = min(offset + page_size, len(file_bytes))
                page_bytes = file_bytes[offset:end]
                self.write_page(
                    worker_host,
                    worker_http_port,
                    path_id,
                    file_path,
                    page_index,
                    page_bytes,
                )
                page_index += 1
                offset += page_size
                if end >= len(file_bytes):
                    break
            return True
        except Exception as e:
            raise PageWriteException(
                f"Error when writing all pages of {path_id}"
            ) from e

    def write(self, file_path: str, file_bytes: bytes) -> bool:
        validate_path(file_path)
        worker_host, worker_http_port = self._get_preferred_worker_address(
            file_path
        )
        path_id = get_path_hash(file_path)
        try:
            return self._write_all_pages_in_file(
                worker_host,
                worker_http_port,
                path_id,
                file_path,
                file_bytes,
            )
        except Exception as e:
            raise PageWriteException(
                f"Error when reading file {file_path}"
            ) from e

    @staticmethod
    def write_page(worker_host: str, worker_http_port: int, path_id: str, file_path: str, page_index: int,
                   page_bytes: bytes) -> bool:
        try:
            response = requests.post(
                WRITE_PAGE_URL_FORMAT.format(
                    worker_host=worker_host,
                    http_port=worker_http_port,
                    path_id=path_id,
                    file_path=file_path,
                    page_index=page_index,
                ),
                headers={"Content-Type": "application/octet-stream"},
                data=page_bytes,
            )
            response.raise_for_status()
            return 200 <= response.status_code < 300
        except requests.RequestException as e:
            raise PageWriteException(
                f"Error writing to file {file_path} at page {page_index}"
            ) from e

    def _create(self, file_path: str, url_format: str) -> bool:
        validate_path(file_path)
        worker_host, worker_http_port = self._get_preferred_worker_address(file_path)
        path_id = get_path_hash(file_path)
        try:
            response = requests.post(
                url_format.format(
                    worker_host=worker_host,
                    http_port=worker_http_port,
                    path_id=path_id,
                    file_path=file_path,
                )
            )
            response.raise_for_status()
            return 200 <= response.status_code < 300
        except requests.RequestException as e:
            raise FileOperationException(f"Error happens when creating {file_path}") from e

    def create_directory(self, file_path: str) -> bool:
        return self._create(file_path, MKDIR_URL_FORMAT)

    def create_file(self, file_path: str) -> bool:
        return self._create(file_path, TOUCH_URL_FORMAT)

    def move(self, source_path: str, target_path: str) -> bool:
        validate_path(source_path)
        validate_path(target_path)
        worker_host, worker_http_port = self._get_preferred_worker_address(source_path)
        path_id = get_path_hash(source_path)
        try:
            response = requests.post(
                MV_URL_FORMAT.format(
                    worker_host=worker_host,
                    http_port=worker_http_port,
                    path_id=path_id,
                    srcPath=source_path,
                    dstPath=target_path,
                )
            )
            response.raise_for_status()
            return 200 <= response.status_code < 300
        except requests.RequestException as e:
            raise FileOperationException(f"Error move a file from {source_path} to {target_path}") from e

    def remove(self, path: str, option: RMOption = RMOption()) -> bool:
        validate_path(path)
        worker_host, worker_http_port = self._get_preferred_worker_address(
            path
        )
        path_id = get_path_hash(path)
        params = option.__dict__
        try:
            response = requests.post(
                RM_URL_FORMAT.format(
                    worker_host=worker_host,
                    http_port=worker_http_port,
                    path_id=path_id,
                    file_path=path,
                ),
                params=params,
            )
            response.raise_for_status()
            return 200 <= response.status_code < 300
        except requests.RequestException as e:
            raise FileOperationException(f"Error remove a file {path}") from e

    def copy(self, source_path: str, target_path: str, option: CPOption = CPOption()) -> bool:
        validate_path(source_path)
        worker_host, worker_http_port = self._get_preferred_worker_address(
            source_path
        )
        path_id = get_path_hash(source_path)
        params = option.__dict__
        try:
            response = requests.post(
                CP_URL_FORMAT.format(
                    worker_host=worker_host,
                    http_port=worker_http_port,
                    path_id=path_id,
                    srcPath=source_path,
                    dstPath=target_path,
                ),
                params=params,
            )
            response.raise_for_status()
            return 200 <= response.status_code < 300
        except requests.RequestException as e:
            raise FileOperationException(f"Error copy a file from {source_path} to {target_path}") from e

    def _ends(self, file_path: str, url_format: str, num_of_bytes: int = None) -> bytes:
        validate_path(file_path)
        worker_host, worker_http_port = self._get_preferred_worker_address(
            file_path
        )
        path_id = get_path_hash(file_path)
        try:
            response = requests.get(
                url_format.format(
                    worker_host=worker_host,
                    http_port=worker_http_port,
                    path_id=path_id,
                    file_path=file_path,
                ),
                params={"numOfBytes": num_of_bytes},
            )
            return b"".join(response.iter_content())
        except requests.RequestException as e:
            raise FileOperationException(f"Error show the ends of {file_path}") from e

    def tail(self, file_path: str, num_of_bytes: int = None) -> bytes:
        return self._ends(file_path, TAIL_URL_FORMAT, num_of_bytes)

    def head(self, file_path: str, num_of_bytes: int = None) -> bytes:
        return self._ends(file_path, HEAD_URL_FORMAT, num_of_bytes)

    def _adjust_offset_and_length(self, read_range_parameters: RangeParameters, page_index: int) -> Tuple[int, int]:
        read_offset = 0
        read_length = self.config.page_size
        if page_index == read_range_parameters.start_page_index:
            read_offset = read_range_parameters.start_page_offset
            if read_range_parameters.start_page_index == read_range_parameters.end_page_index:
                read_length = read_range_parameters.end_page_read_to - read_range_parameters.start_page_offset
            else:
                read_length = self.config.page_size - read_range_parameters.start_page_offset
        elif page_index == read_range_parameters.end_page_index:
            read_length = read_range_parameters.end_page_read_to

        return read_offset, read_length

    def _page_generator(
            self, worker_host: str, worker_http_port: int, path_id: str, file_path: str, offset: int = None,
            length: int = None
    ) -> Generator[bytes, None, None]:
        if (offset is None) != (length is None):
            raise ValueError(
                "Both offset and length should be either None or both not None"
            )
        page_index = 0
        range_read_parameters = None
        finished = False
        if offset:
            range_read_parameters = RangeParameters(offset // self.config.page_size, offset % self.config.page_size,
                                                    (offset + length - 1) // self.config.page_size,
                                                    ((offset + length - 1) % self.config.page_size) + 1)
        while True:
            try:
                if range_read_parameters:
                    read_offset, read_length = self._adjust_offset_and_length(range_read_parameters, page_index)
                    page_content = self._read_page(worker_host, worker_http_port, path_id, file_path, page_index,
                                                   read_offset, read_length)
                    if page_index == range_read_parameters.end_page_index or len(page_content) < read_length:
                        finished = True
                else:
                    page_content = self._read_page(
                        worker_host,
                        worker_http_port,
                        path_id,
                        file_path,
                        page_index,
                    )
                    if len(page_content) < self.config.page_size:  # last page
                        finished = True
                yield page_content
                if finished:
                    break
                page_index += 1
            except Exception as e:
                raise PageReadException(
                    f"Error when reading page {page_index} of {path_id}"
                ) from e

    def _page_generator_on_segmented_file(self, file_path, start, end, segment_size):
        segment_range_list = SegmentRanges(file_path, segment_size, start, end)
        for i in range(segment_range_list.size()):
            segment_range = segment_range_list.get(i)
            segment_path = segment_range.get_path()
            validate_path(segment_path)
            worker_host, worker_http_port = self._get_preferred_worker_address(
                segment_path
            )
            segment_path_id = get_path_hash(segment_path)
            yield from self._page_generator(worker_host, worker_http_port, segment_path_id, segment_path)

    def _read_page(
            self,
            worker_host: str,
            worker_http_port: int,
            path_id: str,
            file_path: str,
            page_index: int,
            offset: int = None,
            length: int = None,
    ) -> bytes:
        try:
            if offset is None:
                page_url = FULL_PAGE_URL_FORMAT.format(
                    worker_host=worker_host,
                    http_port=worker_http_port,
                    path_id=path_id,
                    file_path=file_path,
                    page_index=page_index,
                )
                logger.debug(f"Reading full page request {page_url}")
            else:
                page_url = PAGE_URL_FORMAT.format(
                    worker_host=worker_host,
                    http_port=worker_http_port,
                    path_id=path_id,
                    file_path=file_path,
                    page_index=page_index,
                    page_offset=offset,
                    page_length=length,
                )
                logger.debug(f"Reading page request {page_url}")
            response = self.session.get(page_url)
            response.raise_for_status()
            return response.content

        except Exception as e:
            raise PageReadException(
                f"Error when requesting file {path_id} page {page_index} from {worker_host}: error {e}"
            ) from e

    def _get_preferred_worker_address(self, full_ufs_path: str) -> Tuple[str, int]:
        workers = self.hash_provider.get_multiple_workers(full_ufs_path, 1)
        if len(workers) != 1:
            raise ValueError(
                "Expected exactly one worker from hash ring, but found {} workers {}.".format(
                    len(workers), workers
                )
            )
        return workers[0].host, workers[0].http_server_port

    def load(
            self,
            path: str,
            timeout: int = None,
            verbose: bool = True,
    ) -> bool:
        validate_path(path)
        worker_host, worker_http_port = self._get_preferred_worker_address(
            path
        )
        return load_file(
            self.session, worker_host, worker_http_port, path, timeout, verbose
        )

    def load_progress(
            self,
            path,
            verbose=True,
    ):
        validate_path(path)
        worker_host, worker_http_port = self._get_preferred_worker_address(
            path
        )
        params = {
            "path": path,
            "opType": OpType.PROGRESS.value,
            "verbose": json.dumps(verbose),
        }
        load_progress_url = LOAD_URL_FORMAT.format(
            worker_host=worker_host,
            http_port=worker_http_port,
        )
        return load_progress_internal(self.session, load_progress_url, params)

    def stop_load(
            self,
            path,
    ):
        validate_path(path)
        worker_host, worker_http_port = self._get_preferred_worker_address(
            path
        )
        try:
            params = {"path": path, "opType": OpType.STOP.value}
            response = self.session.get(
                LOAD_URL_FORMAT.format(
                    worker_host=worker_host,
                    http_port=worker_http_port,
                ),
                params=params,
            )
            response.raise_for_status()
            content = json.loads(response.content.decode("utf-8"))
            return content[ALLUXIO_SUCCESS_IDENTIFIER]
        except Exception as e:
            raise LoadException(
                f"Error when stopping load job for path {path} from {worker_host}: error {e}"
            ) from e

    def submit_load(
            self,
            path,
            verbose=True,
    ):
        validate_path(path)
        worker_host, worker_http_port = self._get_preferred_worker_address(
            path
        )
        try:
            params = {
                "path": path,
                "opType": OpType.SUBMIT.value,
                "verbose": json.dumps(verbose),
            }
            response = self.session.get(
                LOAD_URL_FORMAT.format(
                    worker_host=worker_host,
                    http_port=worker_http_port,
                ),
                params=params,
            )
            response.raise_for_status()
            content = json.loads(response.content.decode("utf-8"))
            return content[ALLUXIO_SUCCESS_IDENTIFIER]
        except Exception as e:
            raise LoadException(
                f"Error when submitting load job for path {path} from {worker_host}: error {e}"
            ) from e


def validate_path(path: str) -> None:
    if not isinstance(path, str):
        raise TypeError("path must be a string")

    if not re.search(r"^[a-zA-Z0-9]+://", path):
        raise ValueError(
            "path must be a full path with a protocol (e.g., 'protocol://path')"
        )


def create_session(concurrency: int) -> Session:
    session = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=concurrency, pool_maxsize=concurrency
    )
    session.mount("http://", adapter)
    return session


def get_path_hash(uri: str) -> str:
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
