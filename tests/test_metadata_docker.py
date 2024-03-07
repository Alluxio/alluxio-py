import os

from alluxio import AlluxioClient
from tests.conftest import TEST_DIR
from tests.conftest import TEST_ROOT


def validate_list_get_status(
    alluxio_client: AlluxioClient, alluxio_path, local_path
):
    validate_get_status(alluxio_client, alluxio_path, local_path)
    if not os.path.isdir(local_path):
        return
    alluxio_res = alluxio_client.listdir(alluxio_path)
    alluxio_listing = {item.name: item for item in alluxio_res}
    local_listing = list(os.scandir(local_path))
    assert len(local_listing) == len(
        alluxio_listing
    ), "Local listing result has different length compared to alluxio_listing"
    for entry in local_listing:
        assert (
            entry.name in alluxio_listing
        ), f"{entry.name} is missing in Alluxio listing"
        alluxio_entry = alluxio_listing[entry.name]
        expected_type = "directory" if entry.is_dir() else "file"
        assert (
            expected_type == alluxio_entry.type
        ), f"Type mismatch for {entry.name}: expected {expected_type}, got {alluxio_entry.type}"
        local_mod_time_ms = int(entry.stat().st_mtime * 1000)
        alluxio_mod_time_ms = alluxio_entry.last_modification_time_ms
        assert (
            abs(local_mod_time_ms - alluxio_mod_time_ms) <= 1000
        ), f"Last modification time mismatch for {entry.name}: expected {local_mod_time_ms}, got {alluxio_mod_time_ms}"
        if expected_type == "file":
            assert (
                entry.stat().st_size == alluxio_entry.length
            ), f"Size mismatch for {entry.name}: expected {entry.stat().st_size}, got {alluxio_entry.length}"
        validate_list_get_status(
            alluxio_client,
            os.path.join(alluxio_path, alluxio_entry.name),
            os.path.join(local_path, entry.name),
        )


def validate_get_status(
    alluxio_client: AlluxioClient, alluxio_path: str, local_path: str
):
    alluxio_status = alluxio_client.get_file_status(alluxio_path)
    local_status = os.stat(local_path)
    expected_type = "directory" if os.path.isdir(local_path) else "file"
    assert (
        expected_type == alluxio_status.type
    ), f"Type mismatch for {alluxio_path}: expected {expected_type}, got {alluxio_status.type}"
    local_mod_time_ms = int(local_status.st_mtime * 1000)
    assert (
        abs(local_mod_time_ms - alluxio_status.last_modification_time_ms)
        <= 1000
    ), f"Last modification time mismatch for {alluxio_path}"
    if expected_type == "file":
        assert (
            local_status.st_size == alluxio_status.length
        ), f"Size mismatch for {alluxio_path}: expected {local_status.st_size}, got {alluxio_status.length}"


def test_list_get_status(alluxio_client: AlluxioClient):
    validate_list_get_status(alluxio_client, TEST_ROOT, TEST_DIR)
