from util import assert_json_encode
from random_option import *


def test_create_directory():
    opt = random_create_directory()
    print(opt.json())
    json = dict(allowExists=opt.allow_exists,
                mode=opt.mode.json(),
                recursive=opt.recursive,
                writeType=opt.write_type.json())
    assert_json_encode(opt, json)


def test_create_file():
    opt = random_create_file()
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
    opt = random_delete()
    json = dict(recursive=opt.recursive)
    assert_json_encode(opt, json)


def test_free():
    opt = random_free()
    json = dict(recursive=opt.recursive)
    assert_json_encode(opt, json)


def test_list_status():
    opt = random_list_status()
    json = dict(loadMetadataType=opt.load_metadata_type.json())
    assert_json_encode(opt, json)


def test_mount():
    opt = random_mount()
    json = dict(properties=opt.properties,
                readOnly=opt.read_only, shared=opt.shared)
    assert_json_encode(opt, json)


def test_open_file():
    opt = random_open_file()
    json = dict(cacheLocationPolicyClass=opt.cache_location_policy_class,
                maxUfsReadConcurrency=opt.max_ufs_read_concurrency,
                readType=opt.read_type.json(),
                ufsReadLocationPolicyClass=opt.ufs_read_location_policy_class)
    assert_json_encode(opt, json)


def test_set_attribute():
    opt = random_set_attribute()
    json = dict(owner=opt.owner, group=opt.group, mode=opt.mode.json(),
                pinned=opt.pinned, recursive=opt.recursive, ttl=opt.ttl,
                ttlAction=opt.ttl_action.json())
    assert_json_encode(opt, json)
