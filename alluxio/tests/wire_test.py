from alluxio import wire

from util import assert_json_decode, assert_json_encode, assert_string_subclass


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
    addr = wire.WorkerNetAddress(
        host='localhost', rpc_port=1000, data_port=1001, web_port=1002)
    json = dict(host=addr.host, rpcPort=addr.rpc_port,
                dataPort=addr.data_port, webPort=addr.web_port)
    assert_json_encode(addr, json)
    assert_json_decode(addr, json)


def test_block_location():
    addr = wire.WorkerNetAddress(
        host='localhost', rpc_port=1000, data_port=1001, web_port=1002)
    location = wire.BlockLocation(
        worker_id=1001, worker_address=addr, tier_alias='MEM')
    json = dict(workerId=location.worker_id, workerAddress=location.worker_address.json(
    ), tierAlias=location.tier_alias)
    assert_json_encode(location, json)
    assert_json_decode(location, json)


def create_block_info():
    locations = []
    for i in range(10):
        addr = wire.WorkerNetAddress(host='worker{}'.format(
            i), rpc_port=1000, data_port=1001, web_port=1002)
        mod = i % 3
        if mod == 0:
            tier = 'MEM'
        elif mod == 1:
            tier = 'SSD'
        else:
            tier = 'HDD'
        location = wire.BlockLocation(
            worker_id=i, worker_address=addr, tier_alias=tier)
        locations.append(location)
    return wire.BlockInfo(block_id=12345, length=678, locations=locations)


def test_block_info():
    block = create_block_info()
    json = dict(blockId=block.block_id, length=block.length,
                locations=[location.json() for location in block.locations])
    assert_json_encode(block, json)
    assert_json_decode(block, json)


def create_file_block_info():
    block = create_block_info()
    return wire.FileBlockInfo(block_info=block, offset=100, ufs_locations=['localhost', '192.168.1.1'])


def test_file_block_info():
    file_block_info = create_file_block_info()
    json = dict(blockInfo=file_block_info.block_info.json(
    ), offset=file_block_info.offset, ufsLocations=file_block_info.ufs_locations)
    assert_json_encode(file_block_info, json)
    assert_json_decode(file_block_info, json)


def test_mode():
    mode = wire.Mode(owner_bits=wire.BITS_ALL,
                     group_bits=wire.BITS_READ, other_bits=wire.BITS_NONE)
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
    mode = wire.Mode(owner_bits=wire.BITS_ALL,
                     group_bits=wire.BITS_READ, other_bits=wire.BITS_NONE)
    info = wire.FileInfo(
        block_ids=[1, 2, 3],
        block_size_bytes=1,
        cacheable=True,
        completed=True,
        creation_time_ms=3,
        last_modification_time_ms=1,
        file_block_infos=[create_file_block_info()],
        file_id=1000,
        folder=False,
        owner='alluxion',
        group='alluxio',
        in_memory_percentage=100,
        length=100,
        name='alluxiofile',
        path='/alluxio/alluxiofile',
        ufs_path='s3a://alluxio/alluxiofile',
        pinned=True,
        persisted=True,
        persistence_state=wire.PERSISTENCE_STATE_PERSISTED,
        mode=mode,
        mount_point=True,
        ttl=100,
        ttl_action=wire.TTL_ACTION_DELETE)
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
