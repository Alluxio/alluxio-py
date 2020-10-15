#! -*- coding: utf-8 -*-
"""
Commonly used classes throughout the entire Alluxio Python interface.
"""
import json
import sys

import attr
from six import reraise


def dict_by_values(items):
    """Takes the output of `dictionary.items()` and transponses so that the value becomes the key"""
    by_values = {}
    for key, value in items:
        by_values[value] = key
    return by_values


@attr.s
class Jsonnable(object):
    """Base class for all classes that can be encoded into json."""

    member_json_map = {}
    member_list_map = {}

    @property
    def __dict__(self):
        obj = {}
        for field in attr.fields(self.__class__):
            name = field.name
            value = getattr(self, name, None)
            if value is not None:
                if isinstance(value, list):
                    value = [vars(x) if hasattr(x, 'json') else x for x in value]
                else:
                    to_json = getattr(value, 'json', None)
                    if callable(to_json):
                        # This looks wrong. I'm getting the dict, and appending that, instead of getting the json
                        # string. However, the json string for the object is the wrong choice. We need to append a json
                        # serializeable object (the dict) instead of the string, since Alluxio won't unpack a json
                        # string version of the dict. It expects the dict, so that's what we need to send.
                        value = vars(value)
                mapped = self.member_json_map.get(name, name)
                obj[mapped] = value
        return obj

    def __str__(self):
        """Return the json string representation of this object."""
        return self.json()

    def json(self):
        """Convert the object into a python dict which can be encoded into a json string."""
        return json.dumps(vars(self))

    @classmethod
    def from_json(cls, jsondata):
        """Return an instance of cls decoded from the json string obj.

        The implementation of in this base class just returns cls(str(obj)).

        Args:
            cls: This class.
            obj: The json string to be decoded.

        Returns:
                An instance of cls decoded from obj.
        """
        by_values = dict_by_values(cls.member_json_map.items())

        obj = cls()
        decodedinput = json.loads(jsondata)
        for key, value in decodedinput.items():
            name = by_values.get(key, key)
            converter = attr.fields_dict(cls)[name].converter
            if callable(converter):
                if isinstance(value, dict):
                    # I need to get the next level down's list of `by_values` here.
                    v_by_values = dict_by_values(converter().member_json_map.items())
                    mapped = {}
                    for v_name in value:
                        mapped[v_by_values.get(v_name, v_name)] = value[v_name]
                    value = converter(**(mapped))
                else:
                    value = converter(value)
            if name in obj.member_list_map:
                decoder = obj.member_list_map[name]
                value = [decoder.from_json(json.dumps(x)) for x in value]
            setattr(obj, name, value)
        return obj


@attr.s
class String(Jsonnable):
    """Base class for all classes that are type aliases of a str.

    Args:
        name (str): The string alias of the object.

    Examples:
        see :class:`alluxio.wire.Bits`.
    """

    name = attr.ib(default='')

    @property
    def __dict__(self):
        return self.name

    def json(self):
        return self.name

    @classmethod
    def from_json(cls, jsondata):
        return cls(str(jsondata).strip('"'))


def raise_with_traceback(error_type, message):
    """Raise a new error with the most recent traceback."""

    traceback = sys.exc_info()[2]
    reraise(error_type, error_type(message), traceback)
