# -*- coding: utf-8 -*-

import json


class StrAlias(str):
    def json(self):
        return self


class Bits(StrAlias):
    """Bits represents access mode's bits."""

    @staticmethod
    def from_json(obj):
        return Bits(str(obj))


# bits_none represents no access.
bits_none = Bits('NONE')
# bits_execute represent execute access.
bits_execute = Bits('EXECUTE')
# bits_write represents write access.
bits_write = Bits('WRITE')
# bits_write_execute represents write and execute access.
bits_write_execute = Bits('WRITE_EXECUTE')
# bits_read represents read access.
bits_read = Bits('READ')
# bits_read_execute represents read and execute access.
bits_read_execute = Bits('READ_EXECUTE')
# bits_read_write represents read and write access.
bits_read_write = Bits('READ_WRITE')
# bits_all represents read, write, and execute access
bits_all = Bits('ALL')


class BlockInfo(object):
    """BlockInfo represents a block's information."""

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


class LoadMetadataType(StrAlias):
    """LoadMetadataType represents the load metadata type."""

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


class ReadType(StrAlias):
    """ReadType represents a read type."""

    @staticmethod
    def from_json(obj):
        return ReadType(str(obj))

read_type_no_cache = ReadType('NO_CACHE')
read_type_cache = ReadType('CACHE')
read_type_cache_promote = ReadType('CACHE_PROMOTE')


class TTLAction(StrAlias):
    """TTLAction represents a TTL action."""

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


class WriteType(StrAlias):
    """WriteType represents a write type"""

    @staticmethod
    def from_json(obj):
        return WriteType(str(obj))

# write_type_must_cache means the data will be stored in Alluxio.
write_type_must_cache = WriteType("MUST_CACHE")
# write_type_cache_through means the data will be stored in Alluxio and
# synchronously written to UFS.
write_type_cache_through = WriteType("CACHE_THROUGH")
# write_type_through means the data will be sychrounously written to UFS.
write_type_through = WriteType("THROUGH")
# write_type_async_through means the data will be stored in Alluxio and
# asynchrounously written to UFS.
write_type_async_through = WriteType("ASYNC_THROUGH")
# write_type_none means the data will no be stored.
write_type_none = WriteType("NONE")
