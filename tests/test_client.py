import pytest

from alluxio import wire
from alluxio.option import ListStatus, GetStatus
from alluxio.wire import LOAD_METADATA_TYPE_ALWAYS, FileInfo, LOAD_METADATA_TYPE_NEVER, LOAD_METADATA_TYPE_ONCE


def _expected_test_dir_info(name, path):
    # type: (str, str) -> FileInfo
    return FileInfo(
        block_ids=[], block_size_bytes=0, cacheable=False, completed=True, file_block_infos=[],
        folder=True, owner="alluxio", group="alluxio", in_memory_percentage=0,
        name=name, path=path, ufs_path="/opt/alluxio-2.8.0/underFSStorage" + path,
        pinned=False, persisted=False, persistence_state=wire.PERSISTENCE_STATE_NOT_PERSISTED,
        mode=493, mount_point=False, ttl=-1, ttl_action=wire.TTL_ACTION_DELETE,
        # volatile, not to be compared in assertions:
        creation_time_ms=123, last_modification_time_ms=123, file_id=123, length=1
    )


def _drop_volatile(file_info_json):
    # type: (dict) -> dict
    del file_info_json["creationTimeMs"]
    del file_info_json["lastModificationTimeMs"]
    del file_info_json["fileId"]
    del file_info_json["length"]
    return file_info_json


def _assert_file_info_equals(expected, actual):
    # type: (FileInfo, FileInfo) -> None
    assert _drop_volatile(expected.json()) == _drop_volatile(actual.json())


@pytest.mark.it
class Test_list_status:
    @pytest.mark.parametrize('load_meta_type', (
        LOAD_METADATA_TYPE_NEVER,
        LOAD_METADATA_TYPE_ONCE,
        LOAD_METADATA_TYPE_ALWAYS,
    ))
    def test_list_status(self, conf, client, fs_tree, load_meta_type):
        opt = ListStatus(load_metadata_type=load_meta_type)
        _assert_file_info_equals(
            _expected_test_dir_info("path", "/test/path"),
            client.list_status(conf.test_dir, opt)[0]
        )

    @pytest.mark.parametrize('load_meta_type', (
        LOAD_METADATA_TYPE_NEVER,
        LOAD_METADATA_TYPE_ONCE,
        LOAD_METADATA_TYPE_ALWAYS,
    ))
    def test_list_status_recursive(self, conf, client, fs_tree, load_meta_type):
        opt = ListStatus(load_metadata_type=load_meta_type, recursive=True)
        _assert_file_info_equals(
            _expected_test_dir_info("bar", "/test/path/bar"),
            client.list_status(conf.test_dir, opt)[0]
        )
        _assert_file_info_equals(
            _expected_test_dir_info("path", "/test/path"),
            client.list_status(conf.test_dir, opt)[1]
        )


@pytest.mark.it
class Test_get_status:
    @pytest.mark.parametrize('load_meta_type', (
        LOAD_METADATA_TYPE_NEVER,
        LOAD_METADATA_TYPE_ONCE,
        LOAD_METADATA_TYPE_ALWAYS,
    ))
    def test_get_status(self, conf, client, fs_tree, load_meta_type):
        opt = GetStatus(load_metadata_type=load_meta_type)
        _assert_file_info_equals(
            _expected_test_dir_info("test", "/test"),
            client.get_status(conf.test_dir, opt)
        )
