import os

from alluxio import AlluxioClient
from tests.conftest import TEST_DIR
from tests.conftest import TEST_ROOT


def validate_list(alluxio_client: AlluxioClient, alluxio_path, local_path):
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
        else:
            validate_list(
                alluxio_client,
                os.path.join(alluxio_path, alluxio_entry.name),
                os.path.join(local_path, entry.name),
            )


def test_list(alluxio_client: AlluxioClient):
    validate_list(alluxio_client, TEST_ROOT, TEST_DIR)
