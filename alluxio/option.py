# -*- coding: utf-8 -*-

import json


class CreateDirectory(object):
    def __init__(self):
        self.allow_exists = None
        self.mode = None
        self.recursive = None
        self.write_type = None

    def json(self):
        obj = {}
        if self.allow_exists:
            obj['allowExists'] = self.allow_exists
        if self.mode:
            obj['mode'] = self.mode.json()
        if self.recursive:
            obj['recursive'] = self.recursive
        if self.write_type:
            obj['writeType'] = self.write_type.json()
        return obj


class CreateFile(object):
    def __init__(self):
        self.block_size_bytes = None
        self.location_policy_class = None
        self.mode = None
        self.recursive = None
        self.ttl = None
        self.ttl_action = None
        self.write_type = None

    def json(self):
        obj = {}
        if self.block_size_bytes:
            obj['blockSizeBytes'] = self.block_size_bytes
        if self.location_policy_class:
            obj['locationPolicyClass'] = self.location_policy_class
        if self.mode:
            obj['mode'] = self.mode.json()
        if self.recursive:
            obj['recursive'] = self.recursive
        if self.ttl:
            obj['ttl'] = self.ttl
        if self.ttl_action:
            obj['ttlAction'] = self.ttl_action.json()
        if self.write_type:
            obj['writeType'] = self.write_type.json()
        return obj


class Delete(object):
    def __init__(self):
        self.recursive = None

    def json(self):
        obj = {}
        if self.recursive:
            obj['recursive'] = self.recursive
        return obj


class Exists(object):
    pass


class Free(object):
    def __init__(self):
        self.recursive = None

    def json(self):
        obj = {}
        if self.recursive:
            obj['recursive'] = self.recursive
        return obj


class GetStatus(object):
    pass


class ListStatus(object):
    def __init__(self):
        self.load_metadata_type = None

    def json(self):
        obj = {}
        if self.load_metadata_type:
            obj['loadMetadataType'] = self.load_metadata_type.json()
        return obj


class Mount(object):
    def __init__(self):
        self.properties = None
        self.read_only = None
        self.shared = None

    def json(self):
        obj = {}
        if self.properties:
            obj['properties'] = self.properties
        if self.read_only:
            obj['readOnly'] = self.read_only
        if self.shared:
            obj['shared'] = self.shared
        return obj


class OpenFile(object):
    def __init__(self):
        self.location_policy_class = None
        self.read_type = None

    def json(self):
        obj = {}
        if self.location_policy_class:
            obj['locationPolicyClass'] = self.location_policy_class
        if self.read_type:
            obj['readType'] = self.read_type.json()
        return obj


class Rename(object):
    pass


class SetAttribute(object):
    def __init__(self):
        self.group = None
        self.mode = None
        self.owner = None
        self.persisted = None
        self.pinned = None
        self.recursive = None
        self.ttl = None
        self.ttl_action = None

    def json(self):
        obj = {}
        if self.group:
            obj['group'] = self.group
        if self.mode:
            obj['mode'] = self.mode.json()
        if self.owner:
            obj['owner'] = self.owner
        if self.persisted:
            obj['persisted'] = self.persisted
        if self.pinned:
            obj['pinned'] = self.pinned
        if self.recursive:
            obj['recursive'] = self.recursive
        if self.ttl:
            obj['ttl'] = self.ttl
        if self.ttl_action:
            obj['ttlAction'] = self.ttl_action.json()
        return obj


class Unmount(object):
    pass
