# -*- coding: utf-8 -*-
"""Options for Alluxio Client methods.

Notes:
    All classes in this module have a **json** method, which converts the class
    into a python dict that can be encoded into a json string.
"""
import attr
from .common import Jsonnable
from .wire import mk_ttl_action, mk_read_type, mk_load_metadata_type, mk_write_type, mk_mode


@attr.s
class CreateDirectory(Jsonnable):
    """
    Options to be used in :meth:`alluxio.Client.create_directory`.
    """
    allow_exists = attr.ib(default=None)  # (bool): Whether the directory can already exist.
    mode = attr.ib(default=None)  # (:obj:`alluxio.wire.Mode`): The directory's access mode.
    recursive = attr.ib(default=None)  # (bool): Whether to create intermediate directories along the path as necessary.
    write_type = attr.ib(default=None)  # (:obj:`alluxio.wire.WriteType`): Where to create the directory, e.g. in
    # Alluxio only, in under storage only, or in both.

    member_json_map = {
        'allow_exists': 'allowExists',
        'write_type': 'writeType',
    }


@attr.s  # pylint: disable=too-many-instance-attributes
class CreateFile(Jsonnable):  # pylint: disable=too-many-instance-attributes
    """
    Options to be used in :meth:`alluxio.Client.create_file`.
    """
    block_size_bytes = attr.ib(default=None)  # (int): Block size of the file in bytes.
    location_policy_class = attr.ib(default=None)  # (str): The Java class name for the location policy. If this is not
    # specified, Alluxio will use the default value of the property
    # key **alluxio.user.file.write.location.policy.class**.
    mode = attr.ib(default=None, converter=mk_mode)  # (:obj:`alluxio.wire.Mode`): The file's access mode.
    recursive = attr.ib(default=None)  # (bool): If True, creates intermediate directories along the path as necessary.
    ttl = attr.ib(default=None)  # (int): The TTL (time to live) value. It identifies duration (in milliseconds) the
    # created file should be kept around before it is automatically deleted. -1 means no TTL value is set.
    ttl_action = attr.ib(default=None, converter=mk_ttl_action)  # (:obj:`alluxio.wire.TTLAction`): The file action to
    # take when its TTL expires.
    write_type = attr.ib(default=None, converter=mk_write_type)  # (:obj:`alluxio.wire.WriteType`): It can be used to
    # decide where the file will be created, like in Alluxio only, or in both Alluxio and under storage.
    replication_durable = attr.ib(default=None)  # (int): The number of block replication for durable write.
    replication_max = attr.ib(default=None)  # (int): The maximum number of block replication.
    replication_min = attr.ib(default=None)  # (int): The minimum number of block replication.

    member_json_map = {
        'block_size_bytes': 'blockSizeBytes',
        'location_policy_class': 'locationPolicyClass',
        'ttl_action': 'ttlAction',
        'write_type': 'writeType',
        'replication_durable': 'replicationDurable',
        'replication_max': 'replicationMax',
        'replication_min': 'replicationMin',
    }


@attr.s
class Delete(Jsonnable):
    """
    Options to be used in :meth:`alluxio.Client.delete`.
    """

    recursive = attr.ib(default=None)  # (bool): If set to true for a path to a directory, the directory and its
    # contents will be deleted.


@attr.s
class Exists(Jsonnable):
    """Options to be used in :meth:`alluxio.Client.exists`.

    Currently, it is an empty class, options may be added in future releases.
    """


@attr.s
class Free(Jsonnable):
    """
    Options to be used in :meth:`alluxio.Client.free`.
    """

    recursive = attr.ib(default=None)  # If set to true for a path to a directory, the directory and its contents will
    # be freed.


@attr.s
class GetStatus(Jsonnable):
    """Options to be used in :meth:`alluxio.Client.get_status`.

    Currently, it is an empty class, options may be added in future releases.
    """


@attr.s
class ListStatus(Jsonnable):
    """
    Options to be used in :meth:`alluxio.Client.list_status`.
    """

    load_metadata_type = attr.ib(default=None,
                                 converter=mk_load_metadata_type)  # (:class:`alluxio.wire.LoadMetadataType`):
    # The type of loading metadata, can be one of
    #  :data:`alluxio.wire.LOAD_METADATA_TYPE_NEVER`,
    #  :data:`alluxio.wire.LOAD_METADATA_TYPE_ONCE`,
    #  :data:`alluxio.wire.LOAD_METADATA_TYPE_ALWAYS`,
    #  see documentation on :class:`alluxio.wire.LoadMetadataType` for more details
    recursive = attr.ib(default=None)

    member_json_map = {
        'load_metadata_type': 'loadMetadataType'
    }


@attr.s
class Mount(Jsonnable):
    """
    Options to be used in :meth:`alluxio.Client.mount`.
    """

    properties = attr.ib(default=None)  # (dict): A dictionary mapping property key strings to value strings.
    read_only = attr.ib(default=None)  # (bool): Whether the mount point is read-only.
    shared = attr.ib(default=None)  # (bool): Whether the mount point is shared with all Alluxio users.

    member_json_map = {
        'read_only': 'readOnly'
    }


@attr.s
class OpenFile(Jsonnable):
    """
    Options to be used in :meth:`alluxio.Client.open_file`.
    """

    cache_location_policy_class = attr.ib(default=None)  # (str): The Java class name for the location policy to be
    # used when caching the opened file. If this is not specified, Alluxio will use the default value of the property
    # key **alluxio.user.file.write.location.policy.class**.
    max_ufs_read_concurrency = attr.ib(default=None)  # (int): The maximum UFS read concurrency for one block on one
    # Alluxio worker.
    read_type = attr.ib(default=None, converter=mk_read_type)  # (:obj:`alluxio.wire.ReadType`): The read type, like
    # whether the file readshould be cached, if this is not specified, Alluxio will use the default value of the
    # property key **alluxio.user.file.readtype.default**.
    ufs_read_location_policy_class = attr.ib(default=None)  # (str): The Java class name for the location policy to be
    # used when reading from under storage. If this is not specified, Alluxio will use the default value of the
    # property key **alluxio.user.ufs.block.read.location.policy**.

    member_json_map = {
        'cache_location_policy_class': 'cacheLocationPolicyClass',
        'read_type': 'readType',
        'max_ufs_read_concurrency': 'maxUfsReadConcurrency',
        'ufs_read_location_policy_class': 'ufsReadLocationPolicyClass',
    }


@attr.s
class Rename(Jsonnable):
    """Options to be used in :meth:`alluxio.Client.rename`.

    Currently, it is an empty class, options may be added in future releases.
    """


@attr.s
class SetAttribute(Jsonnable):
    """
    Options to be used in :meth:`alluxio.Client.set_attribute`.
    """

    owner = attr.ib(default=None)  # (str): The owner of the path.
    group = attr.ib(default=None)  # (str): The group of the path.
    mode = attr.ib(default='', converter=mk_mode)  # (:obj:`alluxio.wire.Mode`): The access mode of the path.
    pinned = attr.ib(default=None)  # (bool): Whether the path is pinned in Alluxio, which means it should be kept in
    # memory.
    recursive = attr.ib(default=None)  # (bool): Whether to set ACL (access control list) recursively under a directory.
    ttl = attr.ib(default=None)  # (int): The TTL (time to live) value. It identifies duration (in milliseconds) the
    # file should be kept around before it is automatically deleted. -1 means no TTL value is set.
    ttl_action = attr.ib(default='', converter=mk_ttl_action)  # (:obj:`alluxio.wire.TTLAction`): file action to take
    # when its TTL expires.

    member_json_map = {
        'ttl_action': 'ttlAction',
    }


@attr.s
class Unmount(Jsonnable):
    """Options to be used in :meth:`alluxio.Client.unmount`.

    Currently, it is an empty class, options may be added in future releases.
    """
