#! -*- coding: utf-8 -*-
import json


class _Jsonable(object):
    """Base class for all classes that can be marshaled into json.

    It defines the shared methods: **json** and **__repr__**.
    """

    def json(self):
        pass

    def __repr__(self):
        return json.dumps(self.json())
