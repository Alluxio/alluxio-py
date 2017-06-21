from alluxio import option
from alluxio import wire

from util import assert_json_encode


def test_create_directory():
    mode = wire.Mode(owner_bits=wire.BITS_ALL,
                     group_bits=wire.BITS_READ, other_bits=wire.BITS_NONE)
    opt = option.CreateDirectory(
        allow_exists=True,
        mode=mode,
        recursive=True,
        write_type=wire.WRITE_TYPE_CACHE_THROUGH)
    json = dict(allowExists=opt.allow_exists,
                mode=opt.mode.json(),
                recursive=opt.recursive,
                writeType=opt.write_type.json())
    assert_json_encode(opt, json)


def test_create_file():
    mode = wire.Mode(owner_bits=wire.BITS_ALL,
                     group_bits=wire.BITS_READ, other_bits=wire.BITS_NONE)
    opt = option.CreateFile(
        block_size_bytes=100,
        location_policy_class="location.policy.class",
        mode=mode,
        recursive=True,
        ttl=100,
        ttl_action=wire.TTL_ACTION_FREE,
        write_type=wire.WRITE_TYPE_THROUGH,
        replication_durable=100,
        replication_max=1000,
        replication_min=10)
    json = dict(blockSizeBytes=opt.block_size_bytes,
                locationPolicyClass=opt.location_policy_class,
                mode=opt.mode.json(),
                recursive=opt.recursive,
                ttl=opt.ttl,
                ttlAction=opt.ttl_action.json(),
                writeType=opt.write_type.json(),
                replicationDurable=opt.replication_durable,
                replicationMax=opt.replication_max,
                replicationMin=opt.replication_min)
    assert_json_encode(opt, json)


def test_delete():
    opt = option.Delete(recursive=True)
    json = dict(recursive=opt.recursive)
    assert_json_encode(opt, json)


def test_free():
    opt = option.Free(recursive=True)
    json = dict(recursive=opt.recursive)
    assert_json_encode(opt, json)


def test_list_status():
    opt = option.ListStatus(load_metadata_type=wire.LOAD_METADATA_TYPE_ALWAYS)
    json = dict(loadMetadataType=opt.load_metadata_type.json())
    assert_json_encode(opt, json)


def test_mount():
    opt = option.Mount(properties=dict(key='value'),
                       read_only=True, shared=True)
    json = dict(properties=opt.properties,
                readOnly=opt.read_only, shared=opt.shared)
    assert_json_encode(opt, json)


def test_open_file():
    opt = option.OpenFile(cache_location_policy_class='cache.location.policy.class',
                          max_ufs_read_concurrency=100, read_type=wire.READ_TYPE_CACHE,
                          ufs_read_location_policy_class='ufs.read.location.policy.class')
    json = dict(cacheLocationPolicyClass=opt.cache_location_policy_class,
                maxUfsReadConcurrency=opt.max_ufs_read_concurrency,
                readType=opt.read_type.json(),
                ufsReadLocationPolicyClass=opt.ufs_read_location_policy_class)
    assert_json_encode(opt, json)


def test_set_attribute():
    mode = wire.Mode(owner_bits=wire.BITS_ALL,
                     group_bits=wire.BITS_READ, other_bits=wire.BITS_NONE)
    opt = option.SetAttribute(owner='alluxio', group='alluxio', mode=mode,
                              pinned=True, recursive=True, ttl=100, ttl_action=wire.TTL_ACTION_FREE)
    json = dict(owner=opt.owner, group=opt.group, mode=opt.mode.json(),
                pinned=opt.pinned, recursive=opt.recursive, ttl=opt.ttl,
                ttlAction=opt.ttl_action.json())
    assert_json_encode(opt, json)
