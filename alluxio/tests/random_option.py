import random

from alluxio import option

from random_wire import *
from util import random_bool, random_str, random_int


def random_create_directory():
    return option.CreateDirectory(
        allow_exists=random_bool(),
        mode=random_mode(),
        recursive=random_bool(),
        write_type=random_write_type())


def random_create_file():
    return option.CreateFile(
        block_size_bytes=random_int(),
        location_policy_class=random_str(),
        mode=random_mode(),
        recursive=random_bool(),
        ttl=random_int(),
        ttl_action=random_ttl_action(),
        write_type=random_write_type(),
        replication_durable=random_int(),
        replication_max=random_int(),
        replication_min=random_int())


def random_delete():
    return option.Delete(recursive=random_bool())


def random_free():
    return option.Free(recursive=random_bool())


def random_list_status():
    return option.ListStatus(load_metadata_type=random_load_metadata_type())


def random_mount():
    properties = {}
    for _ in range(random.randint(1, 10)):
        properties[random_str()] = random_str()
    return option.Mount(properties=properties, read_only=random_bool(),
                        shared=random_bool())


def random_open_file():
    return option.OpenFile(
        cache_location_policy_class=random_str(),
        ufs_read_location_policy_class=random_str(),
        max_ufs_read_concurrency=random_int(),
        read_type=random_read_type())


def random_set_attribute():
    return option.SetAttribute(
        group=random_str(),
        mode=random_mode(),
        owner=random_str(),
        persisted=random_bool(),
        pinned=random_bool(),
        recursive=random_bool(),
        ttl=random_int(),
        ttl_action=random_ttl_action())
