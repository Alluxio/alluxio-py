import os
import random

from alluxio import AlluxioFileSystem
from tests.conftest import ALLUXIO_FILE_PATH
from tests.conftest import LOCAL_FILE_PATH

NUM_TESTS = 10

import logging

LOGGER = logging.getLogger(__name__)


def validate_read_range(
    alluxio_fs: AlluxioFileSystem,
    alluxio_file_path,
    local_file_path,
    offset,
    length,
):
    alluxio_data = alluxio_fs.read_range(alluxio_file_path, offset, length)

    with open(local_file_path, "rb") as local_file:
        local_file.seek(offset)
        local_data = local_file.read(length)

    try:
        assert alluxio_data == local_data
    except AssertionError:
        error_message = (
            f"Data mismatch between Alluxio and local file\n"
            f"Alluxio file path: {alluxio_file_path}\n"
            f"Local file path: {local_file_path}\n"
            f"Offset: {offset}\n"
            f"Length: {length}\n"
            f"Alluxio data: {alluxio_data}\n"
            f"Local data: {local_data}"
        )
        raise AssertionError(error_message)


def validate_invalid_read_range(
    alluxio_fs, alluxio_file_path, local_file_path, offset, length
):
    try:
        alluxio_fs.read_range(alluxio_file_path, offset, length)
    except Exception:
        pass
    else:
        raise AssertionError(
            "Expected an exception from Alluxio but none occurred."
        )

    try:
        with open(local_file_path, "rb") as local_file:
            local_file.seek(offset)
            local_file.read(length)
    except Exception:
        pass
    else:
        raise AssertionError(
            "Expected an exception from local file read but none occurred."
        )


def test_alluxio_filesystem(fs: AlluxioFileSystem):
    file_size = os.path.getsize(LOCAL_FILE_PATH)
    assert fs.load(ALLUXIO_FILE_PATH, 200)
    invalid_test_cases = [(-1, 100), (file_size - 1, -2)]
    for offset, length in invalid_test_cases:
        validate_invalid_read_range(
            fs,
            ALLUXIO_FILE_PATH,
            LOCAL_FILE_PATH,
            offset,
            length,
        )
    LOGGER.debug("Passed invalid test cases")

    # Validate normal case
    max_length = 13 * 1024
    for _ in range(NUM_TESTS):
        offset = random.randint(0, file_size - 1)
        length = min(random.randint(1, file_size - offset), max_length)
        validate_read_range(
            fs,
            ALLUXIO_FILE_PATH,
            LOCAL_FILE_PATH,
            offset,
            length,
        )

    LOGGER.debug(
        f"Data matches between Alluxio file and local source file for {NUM_TESTS} times"
    )

    special_test_cases = [
        (file_size - 1, -1),
        (file_size - 1, file_size + 1),
        (file_size, 100),
    ]

    for offset, length in special_test_cases:
        validate_read_range(
            fs,
            ALLUXIO_FILE_PATH,
            LOCAL_FILE_PATH,
            offset,
            length,
        )
    LOGGER.debug("Passed corner test cases")
