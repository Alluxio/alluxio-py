# -*- coding: utf-8 -*-
"""Classes in this module define the wire format of the data sent from the REST API server.

All the classes in this module have a **json** method and a **from_json**
static method. The **json** method converts the class instance to a python
dictionary that can be encoded into a json string. The **from_json** method
decodes a json string into a class instance.
"""

import attr
from .common import String, Jsonnable


class Bits(String):
    """String representation of the access mode's bits.

    Args:
        name (str): The string representation of the access mode.

    Examples:
        The unix mode bits *wrx* can be represented as :data:`BITS_ALL`.

    Existing instances are:

    * :data:`BITS_NONE`
    * :data:`BITS_EXECUTE`
    * :data:`BITS_WRITE`
    * :data:`BITS_WRITE_EXECUTE`
    * :data:`BITS_READ`
    * :data:`BITS_READ_EXECUTE`
    * :data:`BITS_READ_WRITE`
    * :data:`BITS_ALL`
    """


#: No access.
BITS_NONE = Bits('NONE')
#: Execute access.
BITS_EXECUTE = Bits('EXECUTE')
#: Write access.
BITS_WRITE = Bits('WRITE')
#: Write and execute access.
BITS_WRITE_EXECUTE = Bits('WRITE_EXECUTE')
#: Read access.
BITS_READ = Bits('READ')
#: Read and execute access.
BITS_READ_EXECUTE = Bits('READ_EXECUTE')
#: Read and write access.
BITS_READ_WRITE = Bits('READ_WRITE')
#: Read, write, and execute access
BITS_ALL = Bits('ALL')


class ReadType(String):
    """Convenience modes for commonly used read types.

    This can be one of the following, see their documentation for details:

    * :data:`READ_TYPE_NO_CACHE`
    * :data:`READ_TYPE_CACHE`
    * :data:`READ_TYPE_CACHE_PROMOTE`

    Args:
        name (str): The string representation of the read type.
    """


#: Read the file and skip Alluxio storage. This read type will not cause any
#: data migration or eviction in Alluxio storage.
READ_TYPE_NO_CACHE = ReadType('NO_CACHE')
#: Read the file and cache it in the highest tier of a local worker.
#: This read type will not move data between tiers of Alluxio Storage.
#: Users should use :data:`READ_TYPE_CACHE_PROMOTE` for more optimized
#: performance with tiered storage.
READ_TYPE_CACHE = ReadType('CACHE')
#: Read the file and cache it in a local worker. Additionally, if the file was
#: in Alluxio storage, it will be promoted to the top storage layer.
READ_TYPE_CACHE_PROMOTE = ReadType('CACHE_PROMOTE')


def mk_read_type(name):
    """
    Used for `attrs` convertes, during decoding `from_json`. This allows to have the `from_json` avoid doing nested
    types, such as a `Mode` that contains a `Mode` for the owner_bits attribute.
    """
    if isinstance(name, ReadType):
        return name
    return ReadType(name)


class TTLAction(String):
    """Represent the file action to take when its TTL expires.

    This can be one of the following, see their documentation for details:

    * :data:`TTL_ACTION_DELETE`
    * :data:`TTL_ACTION_FREE`

    Args:
        name (str): The string representation of the read type.
    """


#: Represents the action of deleting a path.
TTL_ACTION_DELETE = TTLAction("DELETE")
#: Represents the action of freeing a path.
TTL_ACTION_FREE = TTLAction("FREE")


def mk_ttl_action(name):
    """
    Used for `attrs` convertes, during decoding `from_json`. This allows to have the `from_json` avoid doing nested
    types, such as a `Mode` that contains a `Mode` for the owner_bits attribute.
    """
    if isinstance(name, TTLAction):
        return name
    return TTLAction(name)


class WriteType(String):
    """Write types for creating a file.

    This can be one of the following, see their documentation for details:

    * :data:`WRITE_TYPE_MUST_CACHE`
    * :data:`WRITE_TYPE_CACHE_THROUGH`
    * :data:`WRITE_TYPE_THROUGH`
    * :data:`WRITE_TYPE_ASYNC_THROUGH`

    Args:
        name (str): The string representation of the write type.
    """


#: Write the file, guaranteeing the data is written to Alluxio storage or
#: failing the operation. The data will be written to the highest tier in a
#: worker's storage. Data will not be persisted to the under storage.
WRITE_TYPE_MUST_CACHE = WriteType('MUST_CACHE')
#: Write the file synchronously to the under storage, and also try to write to
#: the highest tier in a worker's Alluxio storage.
WRITE_TYPE_CACHE_THROUGH = WriteType('CACHE_THROUGH')
#: Write the file synchronously to the under storage, skipping Alluxio storage.
WRITE_TYPE_THROUGH = WriteType('THROUGH')
#: Write the file asynchronously to the under storage.
WRITE_TYPE_ASYNC_THROUGH = WriteType('ASYNC_THROUGH')


def mk_write_type(name):
    """
    Used for `attrs` convertes, during decoding `from_json`. This allows to have the `from_json` avoid doing nested
    types, such as a `Mode` that contains a `Mode` for the owner_bits attribute.
    """
    if isinstance(name, WriteType):
        return name
    return WriteType(name)


class PersistenceState(String):
    """Persistence state of a file.

    This can be one of the following, see their documentation for details:

    * :data:`PERSISTENCE_STATE_NOT_PERSISTED`
    * :data:`PERSISTENCE_STATE_TO_BE_PERSISTED`
    * :data:`PERSISTENCE_STATE_PERSISTED`
    * :data:`PERSISTENCE_STATE_LOST`

    Args:
        name (str): The string representation of the persistence state.
    """


#: File not persisted in the under FS.
PERSISTENCE_STATE_NOT_PERSISTED = PersistenceState('NOT_PERSISTED')
#: File is to be persisted in the under FS.
PERSISTENCE_STATE_TO_BE_PERSISTED = PersistenceState('TO_BE_PERSISTED')
#: File is persisted in the under FS.
PERSISTENCE_STATE_PERSISTED = PersistenceState('PERSISTED')
#: File is lost but not persisted in the under FS.
PERSISTENCE_STATE_LOST = PersistenceState('LOST')


def mk_persistence_state(name):
    """
    Used for `attrs` convertes, during decoding `from_json`. This allows to have the `from_json` avoid doing nested
    types, such as a `Mode` that contains a `Mode` for the owner_bits attribute.
    """
    if isinstance(name, PersistenceState):
        return name
    return PersistenceState(name)


@attr.s
class WorkerNetAddress(Jsonnable):
    """
    Worker network address.
    """

    member_json_map = {
        'rpc_port': 'rpcPort',
        'data_port': 'dataPort',
        'web_port': 'webPort',
    }

    host = attr.ib(default='')  # (str): Worker's hostname.
    rpc_port = attr.ib(default=0)  # (int): Port of the worker's RPC server.
    data_port = attr.ib(default=0)  # (int): Port of the worker's data server.
    web_port = attr.ib(default=0)  # (int): Port of the worker's web server.


@attr.s
class BlockLocation(Jsonnable):
    """
    A block's location.
    """

    member_json_map = {
        'worker_id': 'workerId',
        'worker_address': 'workerAddress',
        'tier_alias': 'tierAlias',
    }

    worker_id = attr.ib(default=0)  # (int): ID of the worker that contains the block.
    worker_address = attr.ib(factory=WorkerNetAddress, converter=WorkerNetAddress)
    # (:obj:`alluxio.wire.WorkerNetAddress`): Address of the worker that contains the block.
    tier_alias = attr.ib(default='')  # (str): Alias of the Alluxio storage tier that contains the block,
    # for example, MEM, SSD, or HDD.


@attr.s
class BlockInfo(Jsonnable):
    """
    A block's information.
    """

    member_json_map = {
        'block_id': 'blockId',
    }

    member_list_map = {
        'locations': BlockLocation
    }

    block_id = attr.ib(default=0)  # (int): Block ID.
    length = attr.ib(default=0)  # (int): Block size in bytes
    locations = attr.ib(factory=list)  # (list of :obj:`alluxio.wire.BlockLocation`): List of file block locations.


def mk_block_info(block_id=0, length=0, locations=None):
    """
    Used for `attrs` convertes, during decoding `from_json`. This allows to have the `from_json` avoid doing nested
    types, such as a `Mode` that contains a `Mode` for the owner_bits attribute.
    """
    if isinstance(block_id, BlockInfo):
        return block_id
    return BlockInfo(block_id=block_id, length=length, locations=locations)


@attr.s
class FileBlockInfo(Jsonnable):
    """
    A file block's information.
    """

    member_json_map = {
        'block_info': 'blockInfo',
        'ufs_locations': 'ufsLocations',
    }

    block_info = attr.ib(factory=BlockInfo, converter=mk_block_info)  # (:obj:`alluxio.wire.BlockInfo`): The block's
    # information.
    offset = attr.ib(default=0)  # (int): The block's offset in the file.
    ufs_locations = attr.ib(factory=list)  # (list of str): The under storage locations that contain this block.


@attr.s(eq=False, order=False, hash=False)  # pylint: disable=too-many-instance-attributes
class FileInfo(Jsonnable):  # pylint: disable=too-many-instance-attributes
    """A file or directory's information.

    Two :obj:`FileInfo` are comparable based on the attribute **name**. So a
    list of :obj:`FileInfo` can be sorted by python's built-in **sort** function.
    """
    member_json_map = {
        'persistence_state': 'persistenceState',
        'ttl_action': 'ttlAction',
        'file_id': 'fileId',
        'block_ids': 'blockIds',
        'block_size_bytes': 'blockSizeBytes',
        'creation_time_ms': 'creationTimeMs',
        'last_modification_time_ms': 'lastModificationTimeMs',
        'file_block_infos': 'fileBlockInfos',
        'in_memory_percentage': 'inMemoryPercentage',
        'ufs_path': 'ufsPath',
        'mount_point': 'mountPoint',
    }

    member_list_map = {
        'file_block_infos': FileBlockInfo,
    }

    block_ids = attr.ib(factory=list)  # (list of int): List of block IDs.
    block_size_bytes = attr.ib(default=0)  # (int): Block size in bytes.
    cacheable = attr.ib(default=False)  # (bool): Whether the file can be cached in Alluxio.
    completed = attr.ib(default=False)  # (bool): Whether the file has been marked as completed.
    creation_time_ms = attr.ib(default=0)  # (int): The epoch time the file was created.
    last_modification_time_ms = attr.ib(default=0)  # (int):  The epoch time the file was last modified.
    file_block_infos = attr.ib(factory=list)  # (list of :obj:`alluxio.wire.FileBlockInfo`): List of file block
    # information.
    file_id = attr.ib(default=0)  # (int): File ID.
    folder = attr.ib(default=False)  # (bool): Whether this is a directory.
    owner = attr.ib(default='')  # (str): Owner of this file or directory.
    group = attr.ib(default='')  # (str): Group of this file or directory.
    in_memory_percentage = attr.ib(default=0)  # (int): Percentage of the in memory data.
    length = attr.ib(default=0)  # (int): File size in bytes.
    name = attr.ib(default='')  # (str): File name.
    path = attr.ib(default='')  # (str): Absolute file path.
    ufs_path = attr.ib(default='')  # (str): Under storage path of this file.
    pinned = attr.ib(default=False)  # (bool): Whether the file is pinned.
    persisted = attr.ib(default=False)  # (bool): Whether the file is persisted.
    persistence_state = attr.ib(factory=PersistenceState, converter=mk_persistence_state)
    # (:obj:`alluxio.wire.PersistenceState`): Persistence state.
    mode = attr.ib(default=0)  # (int): Access mode of the file or directory.
    mount_point = attr.ib(default=False)  # (bool): Whether this is a mount point.
    ttl = attr.ib(default=0)  # (int): The TTL (time to live) value. It identifies duration (in milliseconds) the
    # created file should be kept around before it is automatically deleted. -1 means no TTL value is set.
    ttl_action = attr.ib(factory=TTLAction, converter=mk_ttl_action)  # (:obj:`alluxio.wire.TTLAction`): The file action
    # to take when its TTL expires.

    def __lt__(self, other):
        return self.name < other.name

    def __eq__(self, other):
        return self.name == other.name

    def __hash__(self):
        return hash(self.name)


class LoadMetadataType(String):
    """The way to load metadata.

    This can be one of the following, see their documentation for details:

    * :data:`LOAD_METADATA_TYPE_NEVER`
    * :data:`LOAD_METADATA_TYPE_ONCE`
    * :data:`LOAD_METADATA_TYPE_ALWAYS`

    Args:
        name (str): The string representation of the way to load metadata.
    """


#: Never load metadata.
LOAD_METADATA_TYPE_NEVER = LoadMetadataType('Never')
#: Load metadata only at the first time of listing status on a directory.
LOAD_METADATA_TYPE_ONCE = LoadMetadataType('Once')
#: Always load metadata when listing status on a directory.
LOAD_METADATA_TYPE_ALWAYS = LoadMetadataType('Always')


def mk_load_metadata_type(name):
    """
    Used for `attrs` convertes, during decoding `from_json`. This allows to have the `from_json` avoid doing nested
    types, such as a `Mode` that contains a `Mode` for the owner_bits attribute.
    """
    if isinstance(name, LoadMetadataType):
        return name
    return LoadMetadataType(name)


@attr.s
class Mode(Jsonnable):
    """
    A file's access mode.
    """

    owner_bits = attr.ib(default='', converter=Bits)  # (:obj:`alluxio.wire.Bits`): Access mode of the file's owner.
    group_bits = attr.ib(default='', converter=Bits)  # (:obj:`alluxio.wire.Bits`): Access mode of the users in the
    # file's group.
    other_bits = attr.ib(default='', converter=Bits)  # (:obj:`alluxio.wire.Bits`): Access mode of others who are
    # neither the owner nor in the group.

    member_json_map = {
        'owner_bits': 'ownerBits',
        'group_bits': 'groupBits',
        'other_bits': 'otherBits'
    }


def mk_mode(owner_bits='', group_bits='', other_bits=''):
    """
    Used for `attrs` convertes, during decoding `from_json`. This allows to have the `from_json` avoid doing nested
    types, such as a `Mode` that contains a `Mode` for the owner_bits attribute.
    """
    if isinstance(owner_bits, Mode):
        return owner_bits
    return Mode(owner_bits=owner_bits, group_bits=group_bits, other_bits=other_bits)
