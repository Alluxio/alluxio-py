# -*- coding: utf-8 -*-

import json


class Bits(object):
    """String representation of the access mode's bits.

    Args:
        name (str): The string representation of the access mode.

    Existing instances are:
    
    * BITS_NONE
    * BITS_EXECUTE
    * BITS_WRITE
    * BITS_WRITE_EXECUTE
    * BITS_READ
    * BITS_READ_EXECUTE
    * BITS_READ_WRITE
    * BITS_ALL
    """

    def __init__(self, name):
        self.name = name

    def json(self):
        """Return the string representation which can be marshaled into json."""

        return self.name

    @staticmethod
    def from_json(obj):
        """Unmarshal the json encoded obj string into a Bits object."""

        return Bits(str(obj))


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


class BlockInfo(object):
    """Alluxio file block's information.

    Args:
        block_id (int): block ID.
        length (int): block 
    
    """

    def __init__(self, block_id, length, locations):
        self.block_id = block_id
        self.length = length
        self.locations = locations

    def json(self):
        return {
            'blockId': self.block_id,
            'length': self.length,
            'locations': [location.json() for location in self.locations],
        }

    @staticmethod
    def from_json(obj):
        block_id = obj['blockId']
        length = obj['length']
        locations = obj['locations']
        locations = [BlockLocation.from_json(location) for location in locations]
        return BlockInfo(block_id, length, locations)


class BlockLocation(object):
    """BlockLocation represents a block's location."""

    def __init__(self, worker_id, worker_address, tier_alias):
        self.worker_id = worker_id
        self.worker_address = worker_address
        self.tier_alias = tier_alias

    def json(self):
        return {
            'workerId': self.worker_id,
            'workerAddress': self.worker_address.json(),
            'tierAlias': self.tier_alias,
        }

    @staticmethod
    def from_json(obj):
        worker_id = obj['workerId']
        worker_address = WorkerNetAddress.from_json(obj['workerAddress'])
        tier_alias = obj['tierAlias']
        return BlockLocation(worker_id, worker_address, tier_alias)


class FileBlockInfo(object):
    """FileBlockInfo represents a file block's information."""

    def __init__(self, block_info, offset, ufs_locations):
        self.block_info = block_info
        self.offset = offset
        self.ufs_locations = ufs_locations

    def json(self):
        return {
            'blockInfo': self.block_info.json(),
            'offset': self.offset,
            'ufsLocations': self.ufs_locations,
        }

    @staticmethod
    def from_json(obj):
        block_info = BlockInfo.from_json(obj['blockInfo'])
        offset = obj['offset']
        ufs_locations = obj['ufsLocations']
        return FileBlockInfo(block_info, offset, ufs_locations)


class FileInfo(object):
    """FileInfo represents a file's information."""

    def __init__(self):
        self.block_ids = []
        self.block_size_bytes = 0
        self.cacheable = False
        self.completed = False
        self.creation_time_ms = 0
        self.file_block_infos = []
        self.file_id = 0
        self.folder = False
        self.group = ""
        self.in_memory_percentage = 0
        self.last_modification_time_ms = 0
        self.length = 0
        self.name = ""
        self.path = ""
        self.persisted = False
        self.persistence_state = ""
        self.pinned = False
        self.mode = 0
        self.mount_point = False
        self.owner = ""
        self.ttl = 0
        self.ttl_action = ""
        self.ufs_path = ""

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
            'fileBlockInfos': [info.json() for info in self.file_block_infos],
            'fileId': self.file_id,
            'folder': self.folder,
            'group': self.group,
            'inMemoryPercentage': self.in_memory_percentage,
            'lastModificationTimeMs': self.last_modification_time_ms,
            'length': self.length,
            'name': self.name,
            'path': self.path,
            'persisted': self.persisted,
            'persistenceState': self.persistence_state,
            'pinned': self.pinned,
            'mode': self.mode,
            'mountPoint': self.mount_point,
            'owner': self.owner,
            'ttl': self.ttl,
            'ttlAction': self.ttl_action,
            'ufsPath': self.ufs_path,
        }

    @staticmethod
    def from_json(obj):
        info = FileInfo()
        info.block_ids = obj['blockIds']
        info.block_size_bytes = obj['blockSizeBytes']
        info.cacheable = obj['cacheable']
        info.completed = obj['completed']
        info.creation_time_ms = obj['creationTimeMs']
        info.file_block_infos = [FileBlockInfo.from_json(block) for block in obj['fileBlockInfos']]
        info.file_id = obj['fileId']
        info.folder = obj['folder']
        info.group = obj['group']
        info.in_memory_percentage = obj['inMemoryPercentage']
        info.last_modification_time_ms = obj['lastModificationTimeMs']
        info.length = obj['length']
        info.name = obj['name']
        info.path = obj['path']
        info.persisted = obj['persisted']
        info.persistence_state = obj['persistenceState']
        info.pinned = obj['pinned']
        info.mode = obj['mode']
        info.mount_point = obj['mountPoint']
        info.owner = obj['owner']
        info.ttl = obj['ttl']
        info.ttl_action = obj['ttlAction']
        info.ufs_path = obj['ufsPath']
        return info


class LoadMetadataType(object):
    """LoadMetadataType represents the load metadata type."""

    def __init__(self, name):
        self.name = name

    def json(self):
        return self.name

    @staticmethod
    def from_json(obj):
        return LoadMetadataType(str(obj))

load_metadata_type_never = LoadMetadataType('Never')
load_metadata_type_once = LoadMetadataType('Once')
load_metadata_type_always = LoadMetadataType('Always')


class Mode(object):
    """Mode represents the file's access mode."""

    def __init__(self, owner, group, other):
        if not (isinstance(owner, Bits) and isinstance(group, Bits) and isinstance(other, Bits)):
            raise TypeError('owner, group, and other should be of type Bits')
        # owner_bits represents the owner access mode
        owner_bits = owner
        # group_bits represents the group access mode
        group_bits = group
        # other_bits represents the other access mode
        other_bits = other

    def json(self):
        return {
            'ownerBits': owner_bits,
            'groupBits': group_bits,
            'otherBits': other_bits,
        }

    @staticmethod
    def from_json(obj):
        owner = obj['ownerBits']
        group = obj['groupBits']
        other = obj['otherBits']
        return Mode(owner, group, other)


class ReadType(object):
    """ReadType represents a read type."""

    def __init__(self, name):
        self.name = name

    def json(self):
        return self.name

    @staticmethod
    def from_json(obj):
        return ReadType(str(obj))

read_type_no_cache = ReadType('NO_CACHE')
read_type_cache = ReadType('CACHE')
read_type_cache_promote = ReadType('CACHE_PROMOTE')


class TTLAction(object):
    """TTLAction represents a TTL action."""

    def __init__(self, name):
        self.name = name

    def json(self):
        return self.name

    @staticmethod
    def from_json(obj):
        return TTLAction(str(obj))

# ttl_action_delete represents the action of deleting a path.
ttl_action_delete = TTLAction("DELETE")
# ttl_action_free represents the action of freeing a path.
ttl_action_free = TTLAction("FREE")


class WorkerNetAddress(object):
    def __init__(self):
        self.host = ""
        self.rpc_port = 0
        self.data_port = 0
        self.web_port = 0

    def json(self):
        return {
            'host': self.host,
            'rpcPort': self.rpc_port,
            'dataPort': self.data_port,
            'webPort': self.web_port,
        }

    @staticmethod
    def from_json(obj):
        addr = WorkerNetAddress()
        addr.host = obj['host']
        addr.rpc_port = obj['rpcPort']
        addr.data_port = obj['dataPort']
        addr.web_port = obj['webPort']
        return addr


class WriteType(object):
    """WriteType represents a write type"""

    def __init__(self, name):
        self.name = name

    def json(self):
        return self.name

    @staticmethod
    def from_json(obj):
        return WriteType(str(obj))

#: Data will be stored in Alluxio.
WRITE_TYPE_MUST_CACHE = WriteType("MUST_CACHE")
#: Data will be stored in Alluxio and synchronously written to UFS.
WRITE_TYPE_CACHE_THROUGH = WriteType("CACHE_THROUGH")
# Data will be sychrounously written to UFS.
WRITE_TYPE_THROUGH = WriteType("THROUGH")
# Data will be stored in Alluxio and asynchrounously written to UFS.
WRITE_TYPE_ASYNC_THROUGH = WriteType("ASYNC_THROUGH")
# Data will no be stored.
WRITE_TYPE_NONE = WriteType("NONE")
