import random

from alluxio import wire

from util import random_int, random_str, random_bool


def random_list_element(l):
    if len(l) == 0:
        return None
    idx = random.randint(0, len(l) - 1)
    return l[idx]


def random_bits():
    return random_list_element([
        wire.BITS_NONE,
        wire.BITS_EXECUTE,
        wire.BITS_WRITE,
        wire.BITS_WRITE_EXECUTE,
        wire.BITS_READ,
        wire.BITS_READ_EXECUTE,
        wire.BITS_READ_WRITE,
        wire.BITS_ALL
    ])


def random_mode():
    return wire.Mode(owner_bits=random_bits(), group_bits=random_bits(),
                     other_bits=random_bits())


def random_worker_net_address():
    return wire.WorkerNetAddress(host=random_str(), rpc_port=random_int(),
                                 data_port=random_int(), web_port=random_int())


def random_block_location():
    return wire.BlockLocation(worker_id=random_int(),
                              worker_address=random_worker_net_address(),
                              tier_alias=random_str())


def random_block_info():
    locations = []
    for _ in range(random.randint(1, 10)):
        locations.append(random_block_location())
    return wire.BlockInfo(block_id=random_int(), length=random_int(),
                          locations=locations)


def random_file_block_info():
    ufs_locations = []
    for _ in range(random.randint(1, 10)):
        ufs_locations.append(random_str())
    return wire.FileBlockInfo(block_info=random_block_info(),
                              offset=random_int(), ufs_locations=ufs_locations)


def random_persistence_state():
    return random_list_element([
        wire.PERSISTENCE_STATE_NOT_PERSISTED,
        wire.PERSISTENCE_STATE_TO_BE_PERSISTED,
        wire.PERSISTENCE_STATE_PERSISTED,
        wire.PERSISTENCE_STATE_LOST
    ])


def random_ttl_action():
    return random_list_element([
        wire.TTL_ACTION_DELETE,
        wire.TTL_ACTION_FREE
    ])


def random_write_type():
    return random_list_element([
        wire.WRITE_TYPE_THROUGH,
        wire.WRITE_TYPE_CACHE_THROUGH,
        wire.WRITE_TYPE_MUST_CACHE,
        wire.WRITE_TYPE_ASYNC_THROUGH
    ])


def random_read_type():
    return random_list_element([
        wire.READ_TYPE_NO_CACHE,
        wire.READ_TYPE_CACHE,
        wire.READ_TYPE_CACHE_PROMOTE
    ])


def random_load_metadata_type():
    return random_list_element([
        wire.LOAD_METADATA_TYPE_NEVER,
        wire.LOAD_METADATA_TYPE_ONCE,
        wire.LOAD_METADATA_TYPE_ALWAYS
    ])


def random_file_info():
    block_ids = []
    for _ in range(random.randint(1, 10)):
        block_ids.append(random_int())
    file_block_infos = []
    for _ in range(random.randint(1, 10)):
        file_block_infos.append(random_file_block_info())
    return wire.FileInfo(block_ids=block_ids, block_size_bytes=random_int(),
                         cacheable=random_bool(), completed=random_bool(),
                         creation_time_ms=random_int(),
                         file_block_infos=file_block_infos, file_id=random_int(),
                         folder=random_bool(), group=random_str(),
                         in_memory_percentage=random_int(),
                         last_modification_time_ms=random_int(),
                         length=random_int(), mode=random_int(),
                         mount_point=random_bool(), name=random_str(),
                         owner=random_str(), path=random_str(),
                         persisted=random_bool(),
                         persistence_state=random_persistence_state(),
                         pinned=random_bool(), ttl=random_int(),
                         ttl_action=random_ttl_action(), ufs_path=random_str())
