from alluxio import wire

from util import assert_json_decode, assert_json_encode, assert_string_subclass
from random_wire import *


def test_bits():
    assert_string_subclass(wire.BITS_NONE, 'NONE')
    assert_string_subclass(wire.BITS_EXECUTE, 'EXECUTE')
    assert_string_subclass(wire.BITS_WRITE, 'WRITE')
    assert_string_subclass(wire.BITS_WRITE_EXECUTE, 'WRITE_EXECUTE')
    assert_string_subclass(wire.BITS_READ, 'READ')
    assert_string_subclass(wire.BITS_READ_EXECUTE, 'READ_EXECUTE')
    assert_string_subclass(wire.BITS_READ_WRITE, 'READ_WRITE')
    assert_string_subclass(wire.BITS_ALL, 'ALL')


def test_load_metadata_type():
    assert_string_subclass(wire.LOAD_METADATA_TYPE_NEVER, 'Never')
    assert_string_subclass(wire.LOAD_METADATA_TYPE_ONCE, 'Once')
    assert_string_subclass(wire.LOAD_METADATA_TYPE_ALWAYS, 'Always')


def test_read_type():
    assert_string_subclass(wire.READ_TYPE_NO_CACHE, 'NO_CACHE')
    assert_string_subclass(wire.READ_TYPE_CACHE, 'CACHE')
    assert_string_subclass(wire.READ_TYPE_CACHE_PROMOTE, 'CACHE_PROMOTE')


def test_ttl_action():
    assert_string_subclass(wire.TTL_ACTION_DELETE, 'DELETE')
    assert_string_subclass(wire.TTL_ACTION_FREE, 'FREE')


def test_write_type():
    assert_string_subclass(wire.WRITE_TYPE_MUST_CACHE, 'MUST_CACHE')
    assert_string_subclass(wire.WRITE_TYPE_CACHE_THROUGH, 'CACHE_THROUGH')
    assert_string_subclass(wire.WRITE_TYPE_THROUGH, 'THROUGH')
    assert_string_subclass(wire.WRITE_TYPE_ASYNC_THROUGH, 'ASYNC_THROUGH')


def test_worker_net_address():
    addr = random_worker_net_address()
    json = dict(host=addr.host, rpcPort=addr.rpc_port,
                dataPort=addr.data_port, webPort=addr.web_port)
    assert_json_encode(addr, json)
    assert_json_decode(addr, json)


def test_block_location():
    location = random_block_location()
    json = dict(workerId=location.worker_id,
                workerAddress=location.worker_address.json(),
                tierAlias=location.tier_alias)
    assert_json_encode(location, json)
    assert_json_decode(location, json)


def test_block_info():
    block = random_block_info()
    json = dict(blockId=block.block_id, length=block.length,
                locations=[location.json() for location in block.locations])
    assert_json_encode(block, json)
    assert_json_decode(block, json)


def test_file_block_info():
    file_block_info = random_file_block_info()
    json = dict(blockInfo=file_block_info.block_info.json(),
                offset=file_block_info.offset,
                ufsLocations=file_block_info.ufs_locations)
    assert_json_encode(file_block_info, json)
    assert_json_decode(file_block_info, json)


def test_mode():
    mode = random_mode()
    json = dict(ownerBits=mode.owner_bits.json(),
                groupBits=mode.group_bits.json(), otherBits=mode.other_bits.json())
    assert_json_encode(mode, json)
    assert_json_decode(mode, json)


def test_persistence_state():
    assert_string_subclass(
        wire.PERSISTENCE_STATE_NOT_PERSISTED, 'NOT_PERSISTED')
    assert_string_subclass(
        wire.PERSISTENCE_STATE_TO_BE_PERSISTED, 'TO_BE_PERSISTED')
    assert_string_subclass(wire.PERSISTENCE_STATE_PERSISTED, 'PERSISTED')
    assert_string_subclass(wire.PERSISTENCE_STATE_LOST, 'LOST')


def test_file_info():
    info = random_file_info()
    json = dict(
        blockIds=info.block_ids,
        blockSizeBytes=info.block_size_bytes,
        cacheable=info.cacheable,
        completed=info.completed,
        creationTimeMs=info.creation_time_ms,
        lastModificationTimeMs=info.last_modification_time_ms,
        fileBlockInfos=[block.json() for block in info.file_block_infos],
        fileId=info.file_id,
        folder=info.folder,
        owner=info.owner,
        group=info.group,
        inMemoryPercentage=info.in_memory_percentage,
        length=info.length,
        name=info.name,
        path=info.path,
        ufsPath=info.ufs_path,
        pinned=info.pinned,
        persisted=info.persisted,
        persistenceState=info.persistence_state.json(),
        mode=info.mode,
        mountPoint=info.mount_point,
        ttl=info.ttl,
        ttlAction=info.ttl_action.json())
    assert_json_encode(info, json)
    assert_json_decode(info, json)
