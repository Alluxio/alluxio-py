# -*- coding: utf-8 -*-
"""Classes in this module define the wire format of the data sent from the REST API server.

All the classes in this module have a **json** method and a **from_json**
static method. The **json** method converts the class instance to a python
dictionary that can be encoded into a json string. The **from_json** method
decodes a json string into a class instance.
"""

from .common import _JsonEncodable, _JsonDecodable, String


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


class BlockInfo(_JsonEncodable, _JsonDecodable):
    """A block's information.

    Args:
        block_id (int): Block ID.
        length (int): Block size in bytes.
        locations (list of :obj:`alluxio.wire.BlockLocation`): List of file
            block locations.
    """

    def __init__(self, block_id=0, length=0, locations=None):
        self.block_id = block_id
        self.length = length
        self.locations = locations or []

    def json(self):
        return {
            'blockId': self.block_id,
            'length': self.length,
            'locations': [location.json() for location in self.locations],
        }

    @classmethod
    def from_json(cls, obj):
        block_id = obj['blockId']
        length = obj['length']
        locations = obj['locations']
        locations = [BlockLocation.from_json(
            location) for location in locations]
        return cls(block_id, length, locations)


class WorkerNetAddress(_JsonEncodable, _JsonDecodable):
    """Worker network address.

    Args:
        host (str): Worker's hostname.
        rpc_port (int): Port of the worker's RPC server.
        data_port (int): Port of the worker's data server.
        web_port (int): Port of the worker's web server.
    """

    def __init__(self, host='', rpc_port=0, data_port=0, web_port=0):
        self.host = host
        self.rpc_port = rpc_port
        self.data_port = data_port
        self.web_port = web_port

    def json(self):
        return {
            'host': self.host,
            'rpcPort': self.rpc_port,
            'dataPort': self.data_port,
            'webPort': self.web_port,
        }

    @classmethod
    def from_json(cls, obj):
        addr = cls()
        addr.host = obj['host']
        addr.rpc_port = obj['rpcPort']
        addr.data_port = obj['dataPort']
        addr.web_port = obj['webPort']
        return addr


class BlockLocation(_JsonEncodable, _JsonDecodable):
    """A block's location.

    Args:
        worker_id (int): ID of the worker that contains the block.
        worker_address (:obj:`alluxio.wire.WorkerNetAddress`): Address of the
            worker that contains the block.
        tier_alias (str): Alias of the Alluxio storage tier that contains the
            block, for example, MEM, SSD, or HDD.
    """

    def __init__(self, worker_id=0, worker_address=WorkerNetAddress(), tier_alias=''):
        self.worker_id = worker_id
        self.worker_address = worker_address
        self.tier_alias = tier_alias

    def json(self):
        return {
            'workerId': self.worker_id,
            'workerAddress': self.worker_address.json(),
            'tierAlias': self.tier_alias,
        }

    @classmethod
    def from_json(cls, obj):
        worker_id = obj['workerId']
        worker_address = WorkerNetAddress.from_json(obj['workerAddress'])
        tier_alias = obj['tierAlias']
        return cls(worker_id, worker_address, tier_alias)


class FileBlockInfo(_JsonEncodable, _JsonDecodable):
    """A file block's information.

    Args:
        block_info (:obj:`alluxio.wire.BlockInfo`): The block's information.
        offset (int): The block's offset in the file.
        ufs_locations (list of str): The under storage locations that contain this block.
    """

    def __init__(self, block_info=BlockInfo(), offset=0, ufs_locations=None):
        self.block_info = block_info
        self.offset = offset
        self.ufs_locations = ufs_locations or []

    def json(self):
        return {
            'blockInfo': self.block_info.json(),
            'offset': self.offset,
            'ufsLocations': self.ufs_locations,
        }

    @classmethod
    def from_json(cls, obj):
        block_info = BlockInfo.from_json(obj['blockInfo'])
        offset = obj['offset']
        ufs_locations = obj['ufsLocations']
        return cls(block_info, offset, ufs_locations)


class FileInfo(_JsonEncodable, _JsonDecodable):  # pylint: disable=too-many-instance-attributes
    """A file or directory's information.

    Two :obj:`FileInfo` are comparable based on the attribute **name**. So a
    list of :obj:`FileInfo` can be sorted by python's built-in **sort** function.

    Args:
        block_ids (list of int): List of block IDs.
        block_size_bytes (int): Block size in bytes.
        cacheable (bool): Whether the file can be cached in Alluxio.
        completed (bool): Whether the file has been marked as completed.
        creation_time_ms (int): The epoch time the file was created.
        last_modification_time_ms (int):  The epoch time the file was last modified.
        file_block_infos (list of :obj:`alluxio.wire.FileBlockInfo`): List of file block information.
        file_id (int): File ID.
        folder (bool): Whether this is a directory.
        owner (str): Owner of this file or directory.
        group (str): Group of this file or directory.
        in_memory_percentage (int): Percentage of the in memory data.
        length (int): File size in bytes.
        name (str): File name.
        path (str): Absolute file path.
        ufs_path (str): Under storage path of this file.
        pinned (bool): Whether the file is pinned.
        persisted (bool): Whether the file is persisted.
        persistence_state (:obj:`alluxio.wire.PersistenceState`): Persistence state.
        mode (int): Access mode of the file or directory.
        mount_point (bool): Whether this is a mount point.
        ttl (int): The TTL (time to live) value. It identifies duration
            (in milliseconds) the created file should be kept around before it
            is automatically deleted. -1 means no TTL value is set.
        ttl_action (:obj:`alluxio.wire.TTLAction`): The file action to take when
            its TTL expires.
    """

    def __init__(self,   # pylint: disable=too-many-arguments,too-many-locals
                 block_ids=None,
                 block_size_bytes=0,
                 cacheable=False,
                 completed=False,
                 creation_time_ms=0,
                 last_modification_time_ms=0,
                 file_block_infos=None,
                 file_id=0,
                 folder=False,
                 owner='',
                 group='',
                 in_memory_percentage=0,
                 length=0,
                 name='',
                 path='',
                 ufs_path='',
                 pinned=False,
                 persisted=False,
                 persistence_state='',
                 mode=0,
                 mount_point=False,
                 ttl=0,
                 ttl_action=''):
        self.block_ids = block_ids or []
        self.block_size_bytes = block_size_bytes
        self.cacheable = cacheable
        self.completed = completed
        self.creation_time_ms = creation_time_ms
        self.last_modification_time_ms = last_modification_time_ms
        self.file_block_infos = file_block_infos or []
        self.file_id = file_id
        self.folder = folder
        self.owner = owner
        self.group = group
        self.in_memory_percentage = in_memory_percentage
        self.length = length
        self.name = name
        self.path = path
        self.ufs_path = ufs_path
        self.pinned = pinned
        self.persisted = persisted
        self.persistence_state = persistence_state
        self.mode = mode
        self.mount_point = mount_point
        self.ttl = ttl
        self.ttl_action = ttl_action

    def __lt__(self, other):
        return self.name < other.name

    def __eq__(self, other):
        return self.name == other.name

    def __hash__(self):
        return hash(self.name)

    def json(self):
        return {
            'blockIds': self.block_ids,
            'blockSizeBytes': self.block_size_bytes,
            'cacheable': self.cacheable,
            'completed': self.completed,
            'creationTimeMs': self.creation_time_ms,
            'lastModificationTimeMs': self.last_modification_time_ms,
            'fileBlockInfos': [info.json() for info in self.file_block_infos],
            'fileId': self.file_id,
            'folder': self.folder,
            'owner': self.owner,
            'group': self.group,
            'inMemoryPercentage': self.in_memory_percentage,
            'length': self.length,
            'name': self.name,
            'path': self.path,
            'ufsPath': self.ufs_path,
            'pinned': self.pinned,
            'persisted': self.persisted,
            'persistenceState': self.persistence_state.json(),
            'mode': self.mode,
            'mountPoint': self.mount_point,
            'ttl': self.ttl,
            'ttlAction': self.ttl_action.json(),
        }

    @classmethod
    def from_json(cls, obj):
        info = cls()
        info.block_ids = obj['blockIds']
        info.block_size_bytes = obj['blockSizeBytes']
        info.cacheable = obj['cacheable']
        info.completed = obj['completed']
        info.creation_time_ms = obj['creationTimeMs']
        info.last_modification_time_ms = obj['lastModificationTimeMs']
        info.file_block_infos = [FileBlockInfo.from_json(
            block) for block in obj['fileBlockInfos']]
        info.file_id = obj['fileId']
        info.folder = obj['folder']
        info.owner = obj['owner']
        info.group = obj['group']
        info.in_memory_percentage = obj['inMemoryPercentage']
        info.length = obj['length']
        info.name = obj['name']
        info.path = obj['path']
        info.ufs_path = obj['ufsPath']
        info.pinned = obj['pinned']
        info.persisted = obj['persisted']
        info.persistence_state = PersistenceState.from_json(obj['persistenceState'])
        info.mode = obj['mode']
        info.mount_point = obj['mountPoint']
        info.ttl = obj['ttl']
        info.ttl_action = TTLAction.from_json(obj['ttlAction'])
        return info


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
LOAD_METADATA_TYPE_NEVER = LoadMetadataType('NEVER')
#: Load metadata only at the first time of listing status on a directory.
LOAD_METADATA_TYPE_ONCE = LoadMetadataType('ONCE')
#: Always load metadata when listing status on a directory.
LOAD_METADATA_TYPE_ALWAYS = LoadMetadataType('ALWAYS')


class Mode(_JsonEncodable, _JsonDecodable):
    """A file's access mode.

    Args:
        owner_bits (:obj:`alluxio.wire.Bits`): Access mode of the file's owner.
        group_bits (:obj:`alluxio.wire.Bits`): Access mode of the users in the file's group.
        other_bits (:obj:`alluxio.wire.Bits`): Access mode of others who are neither the owner nor in the group.
    """

    def __init__(self, owner_bits=Bits(), group_bits=Bits(), other_bits=Bits()):
        # owner_bits represents the owner access mode
        self.owner_bits = owner_bits
        # group_bits represents the group access mode
        self.group_bits = group_bits
        # other_bits represents the other access mode
        self.other_bits = other_bits

    def json(self):
        return {
            'ownerBits': self.owner_bits.json(),
            'groupBits': self.group_bits.json(),
            'otherBits': self.other_bits.json(),
        }

    @classmethod
    def from_json(cls, obj):
        owner = Bits.from_json(obj['ownerBits'])
        group = Bits.from_json(obj['groupBits'])
        other = Bits.from_json(obj['otherBits'])
        return cls(owner_bits=owner, group_bits=group, other_bits=other)


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
