# -*- coding: utf-8 -*-
"""Options for Alluxio Client methods.

Notes:
    All classes in this module have a **json** method, which converts the class
    into a python dict that can be encoded into a json string.
"""

from .common import _JsonEncodable


class CreateDirectory(_JsonEncodable):  # pylint: disable=too-few-public-methods
    """Options to be used in :meth:`alluxio.Client.create_directory`.

    Args:
        allow_exists (bool): Whether the directory can already exist.
        mode (:obj:`alluxio.wire.Mode`): The directory's access mode.
        recursive (bool): Whether to create intermediate directories along the path as necessary.
        write_type (:obj:`alluxio.wire.WriteType`): Where to create the directory,
            e.g. in Alluxio only, in under storage only, or in both.
    """
    def __init__(self, **kwargs):
        self.allow_exists = kwargs.get('allow_exists')
        self.mode = kwargs.get('mode')
        self.recursive = kwargs.get('recursive')
        self.write_type = kwargs.get('write_type')

    def json(self):
        obj = {}
        if self.allow_exists is not None:
            obj['allowExists'] = self.allow_exists
        if self.mode is not None:
            obj['mode'] = self.mode.json()
        if self.recursive is not None:
            obj['recursive'] = self.recursive
        if self.write_type is not None:
            obj['writeType'] = self.write_type.json()
        return obj


class CreateFile(_JsonEncodable):  # pylint: disable=too-many-instance-attributes,too-few-public-methods
    """Options to be used in :meth:`alluxio.Client.create_file`.

    Args:
        block_size_bytes (int): Block size of the file in bytes.
        location_policy_class (str): The Java class name for the location policy.
            If this is not specified, Alluxio will use the default value of the
            property key **alluxio.user.file.write.location.policy.class**.
        mode (:obj:`alluxio.wire.Mode`): The file's access mode.
        recursive (bool): If True, creates intermediate directories along the
            path as necessary.
        ttl (int): The TTL (time to live) value. It identifies duration
            (in milliseconds) the created file should be kept around before it
            is automatically deleted. -1 means no TTL value is set.
        ttl_action (:obj:`alluxio.wire.TTLAction`): The file action to take when
            its TTL expires.
        write_type (:obj:`alluxio.wire.WriteType`): It can be used to decide
            where the file will be created, like in Alluxio only, or in
            both Alluxio and under storage.
        replication_durable (int): The number of block replication for durable write.
        replication_max (int): The maximum number of block replication.
        replication_min (int): The minimum number of block replication.
    """

    def __init__(self, **kwargs):
        self.block_size_bytes = kwargs.get('block_size_bytes')
        self.location_policy_class = kwargs.get('location_policy_class')
        self.mode = kwargs.get('mode')
        self.recursive = kwargs.get('recursive')
        self.ttl = kwargs.get('ttl')
        self.ttl_action = kwargs.get('ttl_action')
        self.write_type = kwargs.get('write_type')
        self.replication_durable = kwargs.get('replication_durable')
        self.replication_max = kwargs.get('replication_max')
        self.replication_min = kwargs.get('replication_min')

    def json(self):
        obj = {}
        if self.block_size_bytes is not None:
            obj['blockSizeBytes'] = self.block_size_bytes
        if self.location_policy_class is not None:
            obj['locationPolicyClass'] = self.location_policy_class
        if self.mode is not None:
            obj['mode'] = self.mode.json()
        if self.recursive is not None:
            obj['recursive'] = self.recursive
        if self.ttl is not None:
            obj['ttl'] = self.ttl
        if self.ttl_action is not None:
            obj['ttlAction'] = self.ttl_action.json()
        if self.write_type is not None:
            obj['writeType'] = self.write_type.json()
        if self.replication_durable is not None:
            obj['replicationDurable'] = self.replication_durable
        if self.replication_max is not None:
            obj['replicationMax'] = self.replication_max
        if self.replication_min is not None:
            obj['replicationMin'] = self.replication_min
        return obj


class Delete(_JsonEncodable):  # pylint: disable=too-few-public-methods
    """Options to be used in :meth:`alluxio.Client.delete`.

    Args:
        recursive (bool): If set to true for a path to a directory, the
            directory and its contents will be deleted.
    """

    def __init__(self, **kwargs):
        self.recursive = kwargs.get('recursive')

    def json(self):
        obj = {}
        if self.recursive is not None:
            obj['recursive'] = self.recursive
        return obj


class Exists(_JsonEncodable):  # pylint: disable=too-few-public-methods
    """Options to be used in :meth:`alluxio.Client.exists`.

    Currently, it is an empty class, options may be added in future releases.
    """


class Free(_JsonEncodable):  # pylint: disable=too-few-public-methods
    """Options to be used in :meth:`alluxio.Client.free`.

    Args:
        recursive (bool): If set to true for a path to a directory, the
            directory and its contents will be freed.
    """

    def __init__(self, **kwargs):
        self.recursive = kwargs.get('recursive')

    def json(self):
        obj = {}
        if self.recursive is not None:
            obj['recursive'] = self.recursive
        return obj


class GetStatus(_JsonEncodable):  # pylint: disable=too-few-public-methods
    """Options to be used in :meth:`alluxio.Client.get_status`.

    Args:
        load_metadata_type (:class:`alluxio.wire.LoadMetadataType`): The type of
            loading metadata, can be one of
            :data:`alluxio.wire.LOAD_METADATA_TYPE_NEVER`,
            :data:`alluxio.wire.LOAD_METADATA_TYPE_ONCE`,
            :data:`alluxio.wire.LOAD_METADATA_TYPE_ALWAYS`,
            see documentation on :class:`alluxio.wire.LoadMetadataType` for more
            details
    """

    def __init__(self, **kwargs):
        self.load_metadata_type = kwargs.get('load_metadata_type')

    def json(self):
        obj = {}
        if self.load_metadata_type is not None:
            obj['loadMetadataType'] = self.load_metadata_type.json()
        return obj


class ListStatus(_JsonEncodable):  # pylint: disable=too-few-public-methods
    """Options to be used in :meth:`alluxio.Client.list_status`.

    Args:
        load_metadata_type (:class:`alluxio.wire.LoadMetadataType`): The type of
            loading metadata, can be one of
            :data:`alluxio.wire.LOAD_METADATA_TYPE_NEVER`,
            :data:`alluxio.wire.LOAD_METADATA_TYPE_ONCE`,
            :data:`alluxio.wire.LOAD_METADATA_TYPE_ALWAYS`,
            see documentation on :class:`alluxio.wire.LoadMetadataType` for more
            details
    """

    def __init__(self, **kwargs):
        self.load_metadata_type = kwargs.get('load_metadata_type')
        self.recursive = kwargs.get('recursive')

    def json(self):
        obj = {}
        if self.load_metadata_type is not None:
            obj['loadMetadataType'] = self.load_metadata_type.json()
        if self.recursive is not None:
            obj['recursive'] = self.recursive
        return obj


class Mount(_JsonEncodable):  # pylint: disable=too-few-public-methods
    """Options to be used in :meth:`alluxio.Client.mount`.

    Args:
        properties (dict): A dictionary mapping property key strings to value strings.
        read_only (bool): Whether the mount point is read-only.
        shared (bool): Whether the mount point is shared with all Alluxio users.
    """

    def __init__(self, **kwargs):
        self.properties = kwargs.get('properties')
        self.read_only = kwargs.get('read_only')
        self.shared = kwargs.get('shared')

    def json(self):
        obj = {}
        if self.properties is not None:
            obj['properties'] = self.properties
        if self.read_only is not None:
            obj['readOnly'] = self.read_only
        if self.shared is not None:
            obj['shared'] = self.shared
        return obj


class OpenFile(_JsonEncodable):  # pylint: disable=too-few-public-methods
    """Options to be used in :meth:`alluxio.Client.open_file`.

    Args:
        cache_location_policy_class (str): The Java class name for the location
            policy to be used when caching the opened file. If this is not
            specified, Alluxio will use the default value of the property
            key **alluxio.user.file.write.location.policy.class**.
        max_ufs_read_concurrency (int): The maximum UFS read concurrency for
            one block on one Alluxio worker.
        read_type (:obj:`alluxio.wire.ReadType`): The read type, like whether
            the file read should be cached, if this is not specified, Alluxio
            will use the default value of the property key
            **alluxio.user.file.readtype.default**.
        ufs_read_location_policy_class (str): The Java class name for the
            location policy to be used when reading from under storage. If this
            is not specified, Alluxio will use the default value of the property
            key **alluxio.user.ufs.block.read.location.policy**.
    """

    def __init__(self, **kwargs):
        self.cache_location_policy_class = kwargs.get(
            'cache_location_policy_class')
        self.max_ufs_read_concurrency = kwargs.get('max_ufs_read_concurrency')
        self.read_type = kwargs.get('read_type')
        self.ufs_read_location_policy_class = kwargs.get(
            'ufs_read_location_policy_class')

    def json(self):
        obj = {}
        if self.cache_location_policy_class is not None:
            obj['cacheLocationPolicyClass'] = self.cache_location_policy_class
        if self.max_ufs_read_concurrency is not None:
            obj['maxUfsReadConcurrency'] = self.max_ufs_read_concurrency
        if self.read_type is not None:
            obj['readType'] = self.read_type.json()
        if self.ufs_read_location_policy_class is not None:
            obj['ufsReadLocationPolicyClass'] = self.ufs_read_location_policy_class
        return obj


class Rename(_JsonEncodable):  # pylint: disable=too-few-public-methods
    """Options to be used in :meth:`alluxio.Client.rename`.

    Currently, it is an empty class, options may be added in future releases.
    """


class SetAttribute(_JsonEncodable):  # pylint: disable=too-few-public-methods
    """Options to be used in :meth:`alluxio.Client.set_attribute`.

    Args:
        owner (str): The owner of the path.
        group (str): The group of the path.
        mode (:obj:`alluxio.wire.Mode`): The access mode of the path.
        pinned (bool): Whether the path is pinned in Alluxio, which means it
            should be kept in memory.
        recursive (bool): Whether to set ACL (access control list) recursively
            under a directory.
        ttl (int): The TTL (time to live) value. It identifies duration
            (in milliseconds) the file should be kept around before it is
            automatically deleted. -1 means no TTL value is set.
        ttl_action (:obj:`alluxio.wire.TTLAction`): The file action to take when
            its TTL expires.
    """

    def __init__(self, **kwargs):
        self.owner = kwargs.get('owner')
        self.group = kwargs.get('group')
        self.mode = kwargs.get('mode')
        self.pinned = kwargs.get('pinned')
        self.recursive = kwargs.get('recursive')
        self.ttl = kwargs.get('ttl')
        self.ttl_action = kwargs.get('ttl_action')

    def json(self):
        obj = {}
        if self.owner is not None:
            obj['owner'] = self.owner
        if self.group is not None:
            obj['group'] = self.group
        if self.mode is not None:
            obj['mode'] = self.mode.json()
        if self.pinned is not None:
            obj['pinned'] = self.pinned
        if self.recursive is not None:
            obj['recursive'] = self.recursive
        if self.ttl is not None:
            obj['ttl'] = self.ttl
        if self.ttl_action is not None:
            obj['ttlAction'] = self.ttl_action.json()
        return obj


class Unmount(_JsonEncodable):  # pylint: disable=too-few-public-methods
    """Options to be used in :meth:`alluxio.Client.unmount`.

    Currently, it is an empty class, options may be added in future releases.
    """
